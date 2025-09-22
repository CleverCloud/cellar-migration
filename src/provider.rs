use std::{
    collections::VecDeque,
    fmt::Debug,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use aws_smithy_types_convert::date_time::DateTimeExt;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use dyn_clone::DynClone;
use futures::{Stream, StreamExt};
use http_body::Frame;
use tracing::{error, event, instrument, Level};

use crate::radosgw::RadosGW;

pub struct ProviderConf {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: Option<String>,
}

impl ProviderConf {
    pub fn new(
        endpoint: Option<String>,
        region: Option<String>,
        access_key: String,
        secret_key: String,
        bucket: Option<String>,
    ) -> ProviderConf {
        ProviderConf {
            endpoint,
            region,
            access_key,
            secret_key,
            bucket,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProviderObject {
    key: String,
    last_modified: DateTime<Utc>,
    etag: String,
    size: u64,
}

impl ProviderObject {
    pub fn get_key(&self) -> String {
        self.key.clone()
    }

    pub fn get_last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }

    pub fn get_etag(&self) -> &str {
        &self.etag
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }
}

impl From<&aws_sdk_s3::types::Object> for ProviderObject {
    fn from(value: &aws_sdk_s3::types::Object) -> Self {
        ProviderObject {
            key: value.key.clone().expect("Object key shouldn't be null"),
            last_modified: value
                .last_modified
                .map(|e| {
                    e.to_chrono_utc().unwrap_or_else(|_| {
                        panic!(
                            "Should be able to transform AWS datetime {:?} to chrono datetime",
                            e
                        )
                    })
                })
                .expect("Object last_modified shouldn't be null"),
            etag: value.e_tag.clone().expect("Object ETag shouldn't be null"),
            size: value.size.expect("Object size shouldn't be null") as u64,
        }
    }
}

impl PartialEq<ProviderObject> for ProviderObject {
    #[instrument(skip_all, level = "trace")]
    fn eq(&self, other: &ProviderObject) -> bool {
        event!(Level::TRACE, "Self: {:#?}\nOther: {:#?}", self, other);

        if other.key == self.key && other.size == self.get_size() {
            if other.etag == self.etag {
                true
            } else if self.get_etag().contains('-') {
                event!(Level::WARN, "Object {} has been uploaded using multipart upload. Falling back to last modification date to compare objects.", self.get_key());
                self.last_modified < other.last_modified
            } else if other.etag.contains('-') {
                event!(Level::WARN, "Object {} has been uploaded without multipart on source bucket but with multipart on destination bucket. Falling back to last modification date to compare objects.", self.get_key());
                self.last_modified < other.last_modified
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub struct ProviderObjectMetadata {
    pub acl_public: bool,
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_md5: Option<String>,
    pub expires: Option<DateTime<Utc>>,
}

impl From<aws_sdk_s3::operation::head_object::HeadObjectOutput> for ProviderObjectMetadata {
    fn from(value: aws_sdk_s3::operation::head_object::HeadObjectOutput) -> Self {
        let expires = value.expires_string().and_then(|s| {
            DateTime::parse_from_rfc2822(s)
                .map(|dt| dt.with_timezone(&Utc))
                .map_err(|e| {
                    panic!(
                        "Should be able to parse expires string '{}' to datetime: {:?}",
                        s, e
                    )
                })
                .ok()
        });

        // Extract content_md5 from etag before moving value
        let content_md5 = value.e_tag().and_then(|etag| {
            // ETag is often the MD5 hash in hex quotes, convert to base64 for Content-MD5
            if etag.starts_with('"') && etag.ends_with('"') && !etag.contains('-') {
                let hex_md5 = etag.trim_matches('"');
                // Convert hex to bytes, then to base64
                hex::decode(hex_md5)
                    .ok()
                    .map(|bytes| base64::engine::general_purpose::STANDARD.encode(&bytes))
            } else {
                None
            }
        });

        ProviderObjectMetadata {
            acl_public: false,
            last_modified: value.last_modified.map(|date| {
                date.to_chrono_utc().unwrap_or_else(|_| {
                    panic!(
                        "Should be able to transfom AWS datetime {:?} to chrono datetime",
                        date
                    )
                })
            }),
            etag: value.e_tag,
            content_type: value.content_type,
            content_length: value
                .content_length
                .expect("Object should have a content length") as usize,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_md5,
            expires,
        }
    }
}

pub(crate) type ProviderResponseStream =
    Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Sync>>;
pub(crate) type ProviderResponseStreamInner = Arc<Mutex<ProviderResponseStream>>;

/// This struct exists so we can share a single ProviderResponseStreamChunk
/// that will be fed to multiple ByteStream instances, without losing the
/// ownership on the inner Stream.
pub struct ProviderResponseStreamChunkWrapper {
    inner: ProviderResponseStreamInner,
}

impl ProviderResponseStreamChunkWrapper {
    pub fn new(inner: ProviderResponseStreamInner) -> ProviderResponseStreamChunkWrapper {
        ProviderResponseStreamChunkWrapper { inner }
    }
}

impl Stream for ProviderResponseStreamChunkWrapper {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.clone().lock().unwrap().poll_next_unpin(cx)
    }
}

pub(crate) struct ProviderResponseHttp04X {
    stream: ProviderResponseStream,
    size: u64,
    bytes_sent: u64,
}

impl ProviderResponseHttp04X {
    pub fn with_exact_size(stream: ProviderResponseStream, size: usize) -> Self {
        Self {
            stream,
            size: size as u64,
            bytes_sent: 0,
        }
    }
}

impl http_body::Body for ProviderResponseHttp04X {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let projected = this.bytes_sent + chunk.len() as u64;
                if projected > this.size {
                    error!(
                        expected = this.size,
                        received = projected,
                        "ProviderResponseHttp04X produced more bytes than expected"
                    );
                }
                this.bytes_sent = this.bytes_sent.saturating_add(chunk.len() as u64);
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                if this.bytes_sent != this.size {
                    error!(
                        expected = this.size,
                        sent = this.bytes_sent,
                        "ProviderResponseHttp04X ended before sending expected bytes"
                    );
                }
                this.bytes_sent = this.size;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let remaining = self.size.saturating_sub(self.bytes_sent);
        http_body::SizeHint::with_exact(remaining)
    }

    fn is_end_stream(&self) -> bool {
        self.bytes_sent >= self.size
    }
}

pub trait ProviderResponse: Debug + Send + Sync {
    fn status(&self) -> u16;
    fn body(&mut self) -> ProviderResponseStream;
    fn body_chunked(&mut self, chunk_size: usize) -> ProviderResponseStream;
}

impl dyn ProviderResponse {
    pub fn success(&self) -> bool {
        self.status() >= 200 || self.status() < 300
    }

    pub async fn consume_body(&mut self) -> Option<Result<bytes::Bytes, std::io::Error>> {
        let mut ret = BytesMut::new();
        while let Some(res) = self.body().next().await {
            match res {
                Ok(part) => ret.extend(part),
                Err(err) => return Some(Err(err)),
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(Ok(ret.freeze()))
        }
    }
}

#[async_trait]
pub trait Provider: Debug + DynClone + Send + Sync {
    async fn get_buckets(&self) -> anyhow::Result<Vec<String>>;
    fn list_objects(
        &self,
        max_keys: Option<usize>,
        start_after: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<Vec<ProviderObject>>> + '_>>;
    async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<ProviderObjectMetadata>;
    async fn get_object(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<Box<dyn ProviderResponse>>;
}

dyn_clone::clone_trait_object!(Provider);

#[derive(Debug, Clone)]
pub enum Providers {
    Cellar,
    AwsS3,
}

impl TryFrom<&str> for Providers {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "cellar" => Ok(Providers::Cellar),
            "aws-s3" => Ok(Providers::AwsS3),
            _ => Err(format!("Failed to parse provider name: {}", value)),
        }
    }
}

pub fn get_provider(provider: &Providers, conf: ProviderConf) -> Box<dyn Provider> {
    match provider {
        Providers::Cellar => Box::new(RadosGW::new(
            conf.endpoint,
            None,
            conf.access_key,
            conf.secret_key,
            conf.bucket,
        )),
        Providers::AwsS3 => Box::new(RadosGW::new(
            None,
            conf.region,
            conf.access_key,
            conf.secret_key,
            conf.bucket,
        )),
    }
}

#[derive(Debug)]
pub enum ProviderResponseStreamChunkState {
    Active,
    Ended,
    Error(std::io::Error),
}

/// This structure exists because when we give a part to upload to rusoto
/// it will read until the end of the stream to end the part instead of just reading what the part's size
/// This structure simulates that, encapsulates the ProviderResponseStream and keep an internal state on when to end the stream
/// because a part has been fully read.
pub struct ProviderResponseStreamChunk {
    response: ProviderResponseStream,
    chunk_size: usize,
    chunks: VecDeque<Bytes>,
    returned_bytes: usize,
    state: ProviderResponseStreamChunkState,
}

impl ProviderResponseStreamChunk {
    pub fn new(response: ProviderResponseStream, chunk_size: usize) -> ProviderResponseStreamChunk {
        ProviderResponseStreamChunk {
            response,
            chunk_size,
            chunks: VecDeque::new(),
            returned_bytes: 0,
            state: ProviderResponseStreamChunkState::Active,
        }
    }
}

impl Stream for ProviderResponseStreamChunk {
    type Item = Result<Bytes, std::io::Error>;

    #[allow(clippy::branches_sharing_code)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        event!(Level::TRACE, "ProviderResponseStreamChunk: poll_next, chunk_size={}, chunks_len={}, returned_bytes={}, state={:?}", self.chunk_size, self.chunks.len(), self.returned_bytes, self.state);
        if let ProviderResponseStreamChunkState::Error(err) = &self.state {
            return Poll::Ready(Some(Err(std::io::Error::new(err.kind(), err.to_string()))));
        }

        match Pin::new(&mut self.response).poll_next(cx) {
            Poll::Ready(None) => {
                event!(
                    Level::TRACE,
                    "ProviderResponseStreamChunk poll: got EOF from provider"
                );
                self.state = ProviderResponseStreamChunkState::Ended;
            }
            Poll::Ready(Some(Ok(bytes))) => {
                event!(
                    Level::TRACE,
                    "ProviderResponseStreamChunk: Got new chunk of length: {}",
                    bytes.len()
                );
                self.chunks.push_back(bytes);
            }
            Poll::Ready(Some(Err(error))) => {
                event!(
                    Level::TRACE,
                    "ProviderResponseStreamChunk: Got error from stream, {:?}",
                    error
                );
                self.state = ProviderResponseStreamChunkState::Error(error);
            }
            Poll::Pending => {}
        };

        if self.returned_bytes == self.chunk_size {
            event!(Level::TRACE, "ProviderResponseStreamChunk: our stream has returned all needed bytes for now. Reset returned_bytes to 0.");
            self.returned_bytes = 0;
        }

        if !self.chunks.is_empty() {
            event!(
                Level::TRACE,
                "ProviderResponseStreamChunk: we have some chunks ({}) to return. returned_bytes={}",
                self.chunks.len(),
                self.returned_bytes
            );
            let mut chunk = self.chunks.pop_front().unwrap();
            let diff = self.chunk_size - self.returned_bytes;
            event!(
                Level::TRACE,
                "ProviderResponseStreamChunk: current chunk len={}, diff={}",
                chunk.len(),
                diff
            );
            if diff > chunk.len() {
                self.returned_bytes += chunk.len();
                event!(Level::TRACE, "ProviderResponseStreamChunk: chunk is smaller than needed diff, returning it. returned_bytes={}", self.returned_bytes);
                Poll::Ready(Some(Ok(chunk)))
            } else {
                let new_chunk = chunk.split_off(diff);
                self.chunks.push_front(new_chunk);
                self.returned_bytes += chunk.len();
                event!(Level::TRACE, "ProviderResponseStreamChunk: chunk is bigger than needed diff, only returning a portion. returned_bytes={}", self.returned_bytes);
                Poll::Ready(Some(Ok(chunk)))
            }
        } else if let ProviderResponseStreamChunkState::Ended = self.state {
            self.returned_bytes = 0;
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
