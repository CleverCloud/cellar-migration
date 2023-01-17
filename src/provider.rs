use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, FixedOffset, Utc};
use dyn_clone::DynClone;
use futures::{Stream, StreamExt};
use rusoto_core::Region;
use tracing::{event, instrument, Level};

use crate::{
    radosgw::RadosGW,
    riakcs::{
        dto::{ObjectContents, ObjectMetadataResponse},
        RiakCS,
    },
};

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

impl From<&ObjectContents> for ProviderObject {
    fn from(value: &ObjectContents) -> Self {
        ProviderObject {
            key: value.get_key(),
            etag: value.get_etag(),
            last_modified: value.get_last_modified(),
            size: value.get_size(),
        }
    }
}

impl From<&rusoto_s3::Object> for ProviderObject {
    fn from(value: &rusoto_s3::Object) -> Self {
        ProviderObject {
            key: value.key.clone().expect("Object key shouldn't be null"),
            last_modified: value
                .last_modified
                .clone()
                .map(|e| DateTime::from_str(&e).expect("Object last_modified should be a date"))
                .expect("Object last_modified shouldn't be null"),
            etag: value.e_tag.clone().expect("Object ETag shouldn't be null"),
            size: value.size.expect("Object size shouldn't be null") as u64,
        }
    }
}

impl PartialEq<rusoto_s3::Object> for ProviderObject {
    #[instrument(skip_all, level = "trace")]
    fn eq(&self, other: &rusoto_s3::Object) -> bool {
        event!(Level::TRACE, "Self: {:#?}\nOther: {:#?}", self, other);

        if other.key.as_ref() == Some(&self.key) && other.size == Some(self.get_size() as i64) {
            if other.e_tag.as_ref() == Some(&self.etag) {
                true
            } else if self.get_etag().contains('-') {
                event!(Level::WARN, "Object {} has been uploaded using multipart upload. Falling back to last modification date to compare objects.", self.get_key());
                let other_date: Option<DateTime<Utc>> = other
                    .last_modified
                    .as_ref()
                    .and_then(|date| DateTime::from_str(date).ok());
                if let Some(other_date) = other_date {
                    self.last_modified < other_date
                } else {
                    false
                }
            } else if other.e_tag.as_ref().unwrap_or(&String::new()).contains('-') {
                event!(Level::WARN, "Object {} has been uploaded without multipart on source bucket but with multipart on destination bucket. Falling back to last modification date to compare objects.", self.get_key());
                let other_date: Option<DateTime<Utc>> = other
                    .last_modified
                    .as_ref()
                    .and_then(|date| DateTime::from_str(date).ok());
                if let Some(other_date) = other_date {
                    self.last_modified < other_date
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }
}

pub type ProviderObjects = HashMap<String, ProviderObject>;

#[derive(Debug)]
pub struct ProviderObjectMetadata {
    pub acl_public: bool,
    pub last_modified: Option<DateTime<FixedOffset>>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_md5: Option<String>,
    pub expires: Option<String>,
}

impl From<ObjectMetadataResponse> for ProviderObjectMetadata {
    fn from(value: ObjectMetadataResponse) -> Self {
        let m = value.metadata;
        ProviderObjectMetadata {
            acl_public: value.acl_public,
            last_modified: m.last_modified.clone(),
            etag: m.etag.clone(),
            content_type: m.content_type.clone(),
            content_length: m.content_length,
            cache_control: m.cache_control.clone(),
            content_disposition: m.content_disposition.clone(),
            content_encoding: m.content_encoding.clone(),
            content_language: m.content_language.clone(),
            content_md5: m.content_md5.clone(),
            expires: m.expires.clone(),
        }
    }
}

impl From<rusoto_s3::HeadObjectOutput> for ProviderObjectMetadata {
    fn from(value: rusoto_s3::HeadObjectOutput) -> Self {
        ProviderObjectMetadata {
            acl_public: false,
            last_modified: value.last_modified.map(|d| {
                DateTime::parse_from_rfc2822(&d).expect(&format!(
                    "Object should have a valid last modified date: {}",
                    d
                ))
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
            content_md5: None,
            expires: value.expires,
        }
    }
}

/// This struct exists so we can share a single RiakResponseStreamChunk
/// that will be fed to multiple ByteStream instances, without losing the
/// ownership on the inner Stream.
pub struct ProviderResponseStreamChunkWrapper {
    inner: Arc<Mutex<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>>>,
}

impl ProviderResponseStreamChunkWrapper {
    pub fn new(
        inner: Arc<Mutex<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>>>,
    ) -> ProviderResponseStreamChunkWrapper {
        ProviderResponseStreamChunkWrapper { inner }
    }
}

impl Stream for ProviderResponseStreamChunkWrapper {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.clone().lock().unwrap().poll_next_unpin(cx)
    }
}

pub trait ProviderResponse: Debug + Send + Sync {
    fn status(&self) -> u16;
    fn body(&mut self) -> Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>;
    fn body_chunked(
        &mut self,
        chunk_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>;
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
    async fn list_objects(&self, max_keys: Option<usize>) -> anyhow::Result<ProviderObjects>;
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

pub fn get_provider(provider: &str, conf: ProviderConf) -> Box<dyn Provider> {
    match provider {
        "riak-cs" => Box::new(RiakCS::new(
            conf.endpoint
                .expect("RiakCS requires an endpoint and not a region"),
            conf.access_key,
            conf.secret_key,
            conf.bucket,
        )),
        "cellar" => Box::new(RadosGW::new(
            conf.endpoint,
            None,
            conf.access_key,
            conf.secret_key,
            conf.bucket,
        )),
        "aws-s3" => Box::new(RadosGW::new(
            None,
            conf.region,
            conf.access_key,
            conf.secret_key,
            conf.bucket,
        )),
        p => {
            event!(Level::ERROR, "Unknown provider {}", p);
            unreachable!();
        }
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
/// This structure simulates that, encapsulates the RiakResponseStream and keep an internal state on when to end the stream
/// because a part has been fully read.
pub struct ProviderResponseStreamChunk {
    response: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    chunk_size: usize,
    chunks: VecDeque<Bytes>,
    returned_bytes: usize,
    state: ProviderResponseStreamChunkState,
}

impl ProviderResponseStreamChunk {
    pub fn new(
        response: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        chunk_size: usize,
    ) -> ProviderResponseStreamChunk {
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
