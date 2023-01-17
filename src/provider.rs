use std::{
    collections::HashMap,
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
use tracing::{event, instrument, Level};

use crate::riakcs::{
    dto::{ObjectContents, ObjectMetadataResponse},
    RiakCS,
};

pub struct ProviderConf {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket: Option<String>,
}

impl ProviderConf {
    pub fn new(
        endpoint: String,
        access_key: String,
        secret_key: String,
        bucket: Option<String>,
    ) -> ProviderConf {
        ProviderConf {
            endpoint,
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
    async fn list_objects(&self, max_keys: usize) -> anyhow::Result<ProviderObjects>;
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

pub fn get_provider(provider: &str, conf: ProviderConf) -> impl Provider {
    match provider {
        "riak-cs" => RiakCS::new(conf.endpoint, conf.access_key, conf.secret_key, conf.bucket),
        p => {
            event!(Level::ERROR, "Unknown provider {}", p);
            unreachable!();
        }
    }
}
