pub mod awscredentials;
pub mod uploader;

use std::{
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex}, fmt::Debug,
};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use rusoto_core::{ByteStream, RusotoError, HttpConfig};
use rusoto_s3::{
    AbortMultipartUploadError, AbortMultipartUploadOutput, AbortMultipartUploadRequest, Bucket,
    CompleteMultipartUploadError, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateBucketError, CreateBucketRequest,
    CreateMultipartUploadError, CreateMultipartUploadOutput, CreateMultipartUploadRequest,
    DeleteObjectError, DeleteObjectRequest, GetObjectError, GetObjectOutput, GetObjectRequest,
    HeadObjectOutput, HeadObjectRequest, ListObjectsV2Request, PutObjectError, PutObjectOutput,
    PutObjectRequest, S3Client, UploadPartError, UploadPartOutput, UploadPartRequest, S3,
};
use tracing::{event, instrument, Level};

use crate::provider::{
    Provider, ProviderObject, ProviderObjectMetadata, ProviderResponse, ProviderResponseStreamChunk,
};

const MAX_FETCH_KEYS: usize = 1000;
const REQUESTS_MAX_RETRIES: usize = 5;

#[derive(Clone)]
pub struct RadosGW {
    bucket: Option<String>,
    client: S3Client,
}

impl Debug for RadosGW {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RadosGW")
            .field("bucket", &self.bucket)
            .field("client", &"S3Client")
            .finish()
    }
}

impl RadosGW {
    pub fn new(
        endpoint: Option<String>,
        region: Option<String>,
        access_key: String,
        secret_key: String,
        bucket: Option<String>,
    ) -> RadosGW {
        RadosGW {
            bucket,
            client: RadosGW::get_client(access_key, secret_key, endpoint, region)
        }
    }

    #[instrument(level = "trace")]
    fn get_client(access_key: String, secret_key: String, endpoint: Option<String>, region: Option<String>) -> S3Client {
        let radosgw_credential_provider = awscredentials::AWSCredentialsProvider::new(
            access_key,
            secret_key
        );

        let mut http_config = HttpConfig::new();
        http_config.pool_idle_timeout(std::time::Duration::from_secs(10));
        let http_client = rusoto_core::HttpClient::new_with_config(http_config).unwrap();

        let region = match (&endpoint, &region) {
            // Can happen for other S3 like services
            (Some(endpoint), Some(region)) => rusoto_core::Region::Custom {
                name: region.clone(),
                endpoint: endpoint.clone(),
            },
            (Some(endpoint), None) => rusoto_core::Region::Custom {
                name: "default".to_string(),
                endpoint: endpoint.clone(),
            },
            (None, Some(region)) => {
                rusoto_core::Region::from_str(region).expect("Region should be valid")
            }
            _ => unreachable!(),
        };

        event!(Level::DEBUG, "Using client with region: {:?}", region);

        S3Client::new_with(http_client, radosgw_credential_provider, region)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn put_object(
        &self,
        key: String,
        object_metadata: &ProviderObjectMetadata,
        size: i64,
        body: ByteStream,
    ) -> Result<PutObjectOutput, RusotoError<PutObjectError>> {
        let put_object_request = PutObjectRequest {
            body: Some(body),
            key,
            bucket: self
                .bucket
                .clone()
                .expect("put_object should have a bucket"),
            content_length: Some(size),
            acl: if object_metadata.acl_public {
                Some("public-read".to_string())
            } else {
                None
            },
            cache_control: object_metadata.cache_control.clone(),
            content_disposition: object_metadata.content_disposition.clone(),
            content_encoding: object_metadata.content_encoding.clone(),
            content_language: object_metadata.content_language.clone(),
            content_md5: object_metadata.content_md5.clone(),
            content_type: object_metadata.content_type.clone(),
            expires: object_metadata.expires.clone(),
            ..Default::default()
        };

        self.client.put_object(put_object_request).await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_multipart_upload(
        &self,
        key: String,
        object_metadata: &ProviderObjectMetadata,
    ) -> Result<CreateMultipartUploadOutput, RusotoError<CreateMultipartUploadError>> {
        let multipart_upload_request = CreateMultipartUploadRequest {
            key,
            bucket: self
                .bucket
                .clone()
                .expect("create_multipart_upload should have a bucket"),
            acl: if object_metadata.acl_public {
                Some("public-read".to_string())
            } else {
                None
            },
            // We don't have the content_md5 in this list but I don't think we really care
            cache_control: object_metadata.cache_control.clone(),
            content_disposition: object_metadata.content_disposition.clone(),
            content_encoding: object_metadata.content_encoding.clone(),
            content_language: object_metadata.content_language.clone(),
            content_type: object_metadata.content_type.clone(),
            expires: object_metadata.expires.clone(),
            ..Default::default()
        };

        self.client
            .create_multipart_upload(multipart_upload_request)
            .await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn put_object_part(
        &self,
        key: String,
        size: i64,
        body: ByteStream,
        upload_id: String,
        part_number: i64,
    ) -> Result<UploadPartOutput, RusotoError<UploadPartError>> {
        let part_upload_request = UploadPartRequest {
            key,
            bucket: self
                .bucket
                .clone()
                .expect("put_object_part should have a bucket"),
            body: Some(body),
            upload_id,
            part_number,
            content_length: Some(size),
            ..Default::default()
        };

        self.client.upload_part(part_upload_request).await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn complete_multipart_upload(
        &self,
        key: String,
        upload_id: String,
        parts: Vec<(usize, UploadPartOutput)>,
    ) -> Result<CompleteMultipartUploadOutput, RusotoError<CompleteMultipartUploadError>> {
        let completed_multipart_upload_parts = CompletedMultipartUpload {
            parts: Some(
                parts
                    .iter()
                    .map(|(part_number, part)| CompletedPart {
                        e_tag: part.e_tag.clone(),
                        part_number: Some(*part_number as i64),
                    })
                    .collect(),
            ),
        };

        let complete_multipart_upload_request = CompleteMultipartUploadRequest {
            key,
            bucket: self
                .bucket
                .clone()
                .expect("complete_multipart_upload should have a bucket"),
            multipart_upload: Some(completed_multipart_upload_parts),
            upload_id,
            ..Default::default()
        };

        self.client
            .complete_multipart_upload(complete_multipart_upload_request)
            .await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn abort_multipart_upload(
        &self,
        key: String,
        upload_id: String,
    ) -> Result<AbortMultipartUploadOutput, RusotoError<AbortMultipartUploadError>> {
        let abort_multipart_upload_request = AbortMultipartUploadRequest {
            key,
            bucket: self
                .bucket
                .clone()
                .expect("abort_multipart_upload should have a bucket"),
            upload_id,
            ..Default::default()
        };

        self.client
            .abort_multipart_upload(abort_multipart_upload_request)
            .await
    }

    #[instrument(skip(self), level = "trace")]
    async fn list_objects(
        &self,
        max_results: Option<i64>,
        start_after: Option<String>,
    ) -> anyhow::Result<Vec<rusoto_s3::Object>> {
        // Keep track of retries
        let mut retries = 0;

        loop {
            if retries > REQUESTS_MAX_RETRIES {
                event!(Level::ERROR, "We've hit max retries when listing objects. Check warning logs for more details");
                return Err(anyhow::anyhow!("MaxRetriesHit when listing objects"));
            }

            let list_objects_request = ListObjectsV2Request {
                bucket: self
                    .bucket
                    .clone()
                    .expect("list_objects should have a bucket"),
                start_after: start_after.clone(),
                max_keys: max_results,
                ..Default::default()
            };

            event!(
                Level::TRACE,
                "Sending ListObjectV2Request: {:x?}",
                list_objects_request
            );
            let objects = self.client
                .list_objects_v2(list_objects_request.clone())
                .await
                .map(|res| res.contents.unwrap_or_default());
            event!(
                Level::TRACE,
                "Got ListObjectV2Request result result: {:?}",
                objects
            );

            // If we get an HTTP error (timeout, connexion reset, ...), just retry
            if let Err(error) = objects {
                match error {
                    RusotoError::HttpDispatch(_) => {
                        event!(Level::WARN, "Got error when listing objects: {:?}", error);
                        retries += 1;
                        continue;
                    }
                    _ => return Err(anyhow::Error::from(error)),
                }
            }

            let objects = objects.unwrap();

            event!(Level::TRACE, "{:?}", objects.last());

            return Ok(objects);
        }
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn delete_object(
        &self,
        object: ProviderObject,
    ) -> Result<ProviderObject, RusotoError<DeleteObjectError>> {
        let delete_object_request = DeleteObjectRequest {
            bucket: self
                .bucket
                .clone()
                .expect("delete_object should have a bucket"),
            key: object.get_key(),
            ..Default::default()
        };

        self.client
            .delete_object(delete_object_request)
            .await
            .map(|_| object)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn list_buckets(&self) -> anyhow::Result<Vec<Bucket>> {
        self.client
            .list_buckets()
            .await
            .map_err(anyhow::Error::from)
            .map(|result| result.buckets.unwrap_or_default())
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_bucket(
        &self,
        bucket: String,
    ) -> Result<(), RusotoError<CreateBucketError>> {
        // TODO: check if original bucket is public and if it is, apply the same ACL here
        // There might also be some policies, we need to create them.
        let create_bucket_request = CreateBucketRequest {
            bucket,
            ..Default::default()
        };

        self.client
            .create_bucket(create_bucket_request)
            .await
            .map(|_| ())
    }
    #[instrument(skip(self), level = "debug")]
    pub async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<HeadObjectOutput> {
        let head_object_request = HeadObjectRequest {
            bucket: self
                .bucket
                .clone()
                .expect("get_object_metadata should have a bucket"),
            key: object.get_key(),
            ..Default::default()
        };

        self.client
            .head_object(head_object_request)
            .await
            .map_err(|error| {
                anyhow!(
                    "Error fetching object metadata {}: {:?}",
                    object.get_key(),
                    error
                )
            })
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn get_object(&self, object: &ProviderObject) -> anyhow::Result<GetObjectOutput> {
        let get_object_request = GetObjectRequest {
            bucket: self
                .bucket
                .clone()
                .expect("get_object should have a bucket"),
            key: object.get_key(),
            ..Default::default()
        };

        self.client
            .get_object(get_object_request)
            .await
            .map_err(|error| anyhow!("Error fetching object {}: {:?}", object.get_key(), error))
    }
}

struct RadosGWResponseInner {
    stream: ByteStream,
}

impl RadosGWResponseInner {
    pub fn new(stream: ByteStream) -> RadosGWResponseInner {
        RadosGWResponseInner { stream }
    }
}

impl Stream for RadosGWResponseInner {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug)]
pub struct RadosGWResponse {
    response: Option<Arc<Mutex<GetObjectOutput>>>,
    error: Option<anyhow::Error>,
}

impl RadosGWResponse {
    pub fn new(response: Result<GetObjectOutput, anyhow::Error>) -> RadosGWResponse {
        let (res, error) = match response {
            Ok(res) => (Some(Arc::new(Mutex::new(res))), None),
            Err(err) => (None, Some(err)),
        };
        RadosGWResponse {
            response: res,
            error,
        }
    }
}

impl ProviderResponse for RadosGWResponse {
    fn status(&self) -> u16 {
        match &self.error {
            None => 200,
            Some(err) => match err.downcast_ref::<GetObjectError>() {
                Some(GetObjectError::NoSuchKey(_)) => 404,
                Some(GetObjectError::InvalidObjectState(_)) => 500,
                None => unreachable!("Failed to downcast to a GetObjetError: {:?}", err),
            },
        }
    }

    fn body(
        &mut self,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>
    {
        match &self.error {
            None => {
                let response = self.response.take().expect("We should have a response");

                let mut lock = response.lock().expect("Should lock");

                match lock.body.take() {
                    Some(body) => Box::pin(RadosGWResponseInner::new(body)),
                    None => Box::pin(futures::stream::empty()),
                }
            }
            Some(_) => Box::pin(futures::stream::empty()),
        }
    }

    fn body_chunked(
        &mut self,
        chunk_size: usize,
    ) -> std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send>>
    {
        match &self.error {
            None => {
                let response = self.response.take().expect("We should have a response");

                let mut lock = response.lock().expect("Should lock");

                match lock.body.take() {
                    Some(body) => Box::pin(ProviderResponseStreamChunk::new(
                        Box::pin(RadosGWResponseInner::new(body)),
                        chunk_size,
                    )),
                    None => Box::pin(futures::stream::empty()),
                }
            }
            Some(_) => Box::pin(futures::stream::empty()),
        }
    }
}

#[async_trait]
impl Provider for RadosGW {
    async fn get_buckets(&self) -> anyhow::Result<Vec<String>> {
        self.list_buckets().await.map(|buckets| {
            buckets
                .iter()
                .map(|b| b.name.clone().expect("Bucket should have a name"))
                .collect()
        })
    }
    #[instrument(skip(self), level = "debug")]
    fn list_objects(
        &self,
        max_keys: Option<usize>,
        start_after: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<Vec<ProviderObject>>> + '_>> {
        Box::pin(futures::stream::unfold(
            (start_after, 0),
            move |(start_after, total_keys)| async move {
                let max_results = max_keys
                    .map(|max| {
                        if total_keys + MAX_FETCH_KEYS > max {
                            max - total_keys
                        } else {
                            MAX_FETCH_KEYS
                        }
                    })
                    .unwrap_or(MAX_FETCH_KEYS);
                event!(
                    Level::DEBUG,
                    "Listing objects (bucket={:?}): start_after={:?}, max_results={:?}, total_keys={}",
                    self.bucket,
                    start_after,
                    max_results,
                    total_keys
                );

                let objects: anyhow::Result<Vec<ProviderObject>> = self
                    .list_objects(Some(max_results as i64), start_after.clone())
                    .await
                    .map(|res| res.iter().map(|object| object.into()).collect());

                event!(
                    Level::DEBUG,
                    "Listing objects (bucket={:?}): Got {:?}",
                    self.bucket,
                    objects.as_ref().map(|r| format!("len={}", r.len()))
                );

                match objects {
                    Ok(objects) => {
                        if objects.is_empty() {
                            None
                        } else {
                            let last_key = objects.last().unwrap().get_key();

                            let len = objects.len();
                            Some((Ok(objects), (Some(last_key), total_keys + len)))
                        }
                    }
                    Err(error) => Some((Err(anyhow!(error)), (start_after, total_keys))),
                }
            },
        ))
    }
    async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<ProviderObjectMetadata> {
        self.get_object_metadata(object)
            .await
            .map(|response| response.into())
    }
    async fn get_object(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<Box<dyn ProviderResponse>> {
        let object = self.get_object(object).await;

        let x: Box<dyn ProviderResponse> = Box::new(RadosGWResponse::new(object));
        Ok(x)
    }
}
