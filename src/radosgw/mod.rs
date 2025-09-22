pub mod uploader;

use std::{
    pin::Pin,
    fmt::Debug,
};

use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::{SdkConfig, timeout::TimeoutConfig, retry::{RetryConfigBuilder, RetryMode}, BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{primitives::ByteStream, operation::{put_object::{PutObjectOutput, PutObjectError}, create_multipart_upload::{CreateMultipartUploadOutput, CreateMultipartUploadError}, upload_part::{UploadPartOutput, UploadPartError}, complete_multipart_upload::{CompleteMultipartUploadOutput, CompleteMultipartUploadError}, abort_multipart_upload::{AbortMultipartUploadOutput, AbortMultipartUploadError}, delete_object::DeleteObjectError, create_bucket::CreateBucketError, head_object::HeadObjectOutput, get_object::{GetObjectOutput, GetObjectError}}, error::SdkError, types::{CompletedMultipartUpload, CompletedPart, Bucket}};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_types_convert::date_time::DateTimeExt;
use bytes::Bytes;
use futures::Stream;
use tracing::{event, instrument, Level, error, debug};

use crate::provider::{
    Provider, ProviderObject, ProviderObjectMetadata, ProviderResponse, ProviderResponseStreamChunk, ProviderResponseStream,
};

type S3Client = aws_sdk_s3::Client;

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
        let creds = Credentials::from_keys(access_key, secret_key, None);
        let timeout_config = TimeoutConfig::builder()
            .operation_timeout(std::time::Duration::from_secs(30))
            .operation_attempt_timeout(std::time::Duration::from_secs(600)) // It might take some time for huge upload parts
            .connect_timeout(std::time::Duration::from_secs(3))
            .build();
        let retry_config = RetryConfigBuilder::new()
            .mode(RetryMode::Standard)
            .max_attempts(REQUESTS_MAX_RETRIES as _)
            .build();
        let mut sdk_config = SdkConfig::builder()
            .retry_config(retry_config)
            .timeout_config(timeout_config);
        //http_config.pool_idle_timeout(std::time::Duration::from_secs(10));
        //let http_client = rusoto_core::HttpClient::new_with_config(http_config).unwrap();

        match (&endpoint, &region) {
            // Can happen for other S3 like services
            (Some(endpoint), Some(region)) => {
                let region = aws_sdk_s3::config::Region::new(region.clone());
                sdk_config
                    .set_region(region)
                    .set_endpoint_url(Some(endpoint.clone()));
            },
            (Some(endpoint), None) => {
                sdk_config.set_endpoint_url(Some(endpoint.clone()));
                sdk_config.set_region(Some(Region::from_static("eu-west-1")));
            },
            (None, Some(region)) => {
                let region = aws_sdk_s3::config::Region::new(region.clone());
                sdk_config.set_region(region);
            }
            _ => unreachable!(),
        };

        event!(Level::DEBUG, "Using client with region: {:?}", region);

        let sdk_config = sdk_config.build();
        debug!("sdk_config: {:?}", sdk_config);
        let config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .credentials_provider(creds)
            .behavior_version(BehaviorVersion::v2023_11_09())
            .force_path_style(true) // TODO: make it configurable
            .build();
        S3Client::from_conf(config)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn put_object(
        &self,
        key: String,
        object_metadata: &ProviderObjectMetadata,
        size: i64,
        body: ByteStream,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError, HttpResponse>> {
        self.client.put_object()
            .body(body)
            .key(key)
            .bucket(self.bucket.clone().expect("put_object should have a bucket"))
            .content_length(size)
            .set_acl(if object_metadata.acl_public { Some(aws_sdk_s3::types::ObjectCannedAcl::PublicRead) } else { None })
            .set_cache_control(object_metadata.cache_control.clone())
            .set_content_disposition(object_metadata.content_disposition.clone())
            .set_content_encoding(object_metadata.content_encoding.clone())
            .set_content_language(object_metadata.content_language.clone())
            .set_content_md5(object_metadata.content_md5.clone())
            .set_content_type(object_metadata.content_type.clone())
            .set_expires(object_metadata.expires.map(aws_smithy_types::date_time::DateTime::from_chrono_utc))
            .send()
            .await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_multipart_upload(
        &self,
        key: String,
        object_metadata: &ProviderObjectMetadata,
    ) -> Result<CreateMultipartUploadOutput, SdkError<CreateMultipartUploadError, HttpResponse>> {
        self.client
            .create_multipart_upload()
            .key(key)
            .bucket(self.bucket.clone().expect("create_multipart_upload should have a bucket"))
            .set_acl(if object_metadata.acl_public { Some(aws_sdk_s3::types::ObjectCannedAcl::PublicRead) } else { None })
            .set_cache_control(object_metadata.cache_control.clone())
            .set_content_disposition(object_metadata.content_disposition.clone())
            .set_content_encoding(object_metadata.content_encoding.clone())
            .set_content_language(object_metadata.content_language.clone())
            .set_content_type(object_metadata.content_type.clone())
            .set_expires(object_metadata.expires.map(aws_smithy_types::date_time::DateTime::from_chrono_utc))
            .send()
            .await

    }

    #[instrument(skip(self), level = "debug")]
    pub async fn put_object_part(
        &self,
        key: String,
        size: i64,
        body: ByteStream,
        upload_id: String,
        part_number: i32,
    ) -> Result<UploadPartOutput, SdkError<UploadPartError, HttpResponse>> {
        self.client.upload_part()
        .key(key)
        .bucket(self.bucket.clone().expect("put_object_part should have a bucket"))
        .body(body)
        .upload_id(upload_id)
        .part_number(part_number)
        .content_length(size)
        .send()
        .await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn complete_multipart_upload(
        &self,
        key: String,
        upload_id: String,
        parts: Vec<(usize, UploadPartOutput)>,
    ) -> Result<CompleteMultipartUploadOutput, SdkError<CompleteMultipartUploadError, HttpResponse>> {
        let parts = parts
            .iter()
            .map(|(part_number, part)| {
                CompletedPart::builder()
                    .set_e_tag(part.e_tag.clone())
                    .part_number(*part_number as _)
                    .build()
            })
            .collect();

        let completed_multipart_upload_parts = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        self.client
            .complete_multipart_upload()
            .key(key)
            .bucket(self.bucket.clone().expect("complete_multipart_upload should have a bucket"))
            .multipart_upload(completed_multipart_upload_parts)
            .upload_id(upload_id)
            .send()
            .await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn abort_multipart_upload(
        &self,
        key: String,
        upload_id: String,
    ) -> Result<AbortMultipartUploadOutput, SdkError<AbortMultipartUploadError, HttpResponse>> {
        self.client
            .abort_multipart_upload()
            .key(key)
            .bucket(self.bucket.clone().expect("abort_multipart_upload should have a bucket"))
            .upload_id(upload_id)
            .send()
            .await
    }

    #[instrument(skip(self), level = "trace")]
    async fn list_objects(
        &self,
        max_results: Option<i32>,
        start_after: Option<String>,
    ) -> anyhow::Result<Vec<aws_sdk_s3::types::Object>> {
        event!(
            Level::TRACE,
            "Sending ListObjectV2Request: bucket={:?}, start_after={:?}, max_keys={:?}",
            self.bucket, start_after, max_results
        );

        let objects = self.client
            .list_objects_v2()
            .bucket(self.bucket.clone().expect("list_objects should have a bucket"))
            .set_start_after(start_after.clone())
            .set_max_keys(max_results)
            .send()
            .await
            .map(|res| res.contents.unwrap_or_default());

        event!(
            Level::TRACE,
            "Got ListObjectV2Request result result: {:?}",
            objects
        );

        let objects = objects.unwrap();

        event!(Level::TRACE, "{:?}", objects.last());

        return Ok(objects);
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn delete_object(
        &self,
        object: ProviderObject,
    ) -> Result<ProviderObject, SdkError<DeleteObjectError, HttpResponse>> {
        self.client
            .delete_object()
            .bucket(self.bucket.clone().expect("delete_object should have a bucket"))
            .key(object.get_key())
            .send()
            .await
            .map(|_| object)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn list_buckets(&self) -> anyhow::Result<Vec<Bucket>> {
        self.client
            .list_buckets()
            .send()
            .await
            .map_err(anyhow::Error::from)
            .map(|result| result.buckets.unwrap_or_default())
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_bucket(
        &self,
        bucket: String,
    ) -> Result<(), SdkError<CreateBucketError, HttpResponse>> {
        // TODO: check if original bucket is public and if it is, apply the same ACL here
        // There might also be some policies, we need to create them.
        self.client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map(|_| ())
    }
    #[instrument(skip(self), level = "debug")]
    pub async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<HeadObjectOutput> {
        self.client
            .head_object()
            .bucket(self.bucket.clone().expect("get_object_metadata should have a bucket"))
            .key(object.get_key())
            .send()
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
        self.client
            .get_object()
            .bucket(self.bucket.clone().expect("get_object should have a bucket"))
            .key(object.get_key())
            .send()
            .await
            .map_err(|error| anyhow!("Error fetching object {}: {:?}", object.get_key(), error))
    }
}

struct RadosGWResponseInner {
    output: GetObjectOutput,
}

impl RadosGWResponseInner {
    pub fn new(output: GetObjectOutput) -> RadosGWResponseInner {
        RadosGWResponseInner { output }
    }
}

impl Stream for RadosGWResponseInner {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.output.body).poll_next(cx)
            .map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct RadosGWResponse {
    response: Option<GetObjectOutput>,
    error: Option<anyhow::Error>,
}

impl RadosGWResponse {
    pub fn new(response: Result<GetObjectOutput, anyhow::Error>) -> RadosGWResponse {
        let (res, error) = match response {
            Ok(res) => (Some(res), None),
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
            Some(err) => match err.downcast_ref::<SdkError<GetObjectError, HttpResponse>>() {
                Some(downcast_error) => match downcast_error {
                    SdkError::ServiceError(err) => match err.err() {
                        GetObjectError::NoSuchKey(_) => 404,
                        GetObjectError::InvalidObjectState(_) => 500,
                        e => {
                            error!("Unknown GetObjectError {:?}", e);
                            500
                        }
                    }
                    e => {
                        error!("Unknown S3 error: {:?} on GetObjectError", e);
                        500
                    }
                }
                None => {
                    // Usually, network errors or stuff like that
                    error!("Failed to downcast to a GetObjectError: {:?}", err);
                    500
                }
            },
        }
    }

    fn body(
        &mut self,
    ) -> ProviderResponseStream
    {
        match &self.error {
            None => {
                let response = self.response.take().expect("We should have a response");

                Box::pin(RadosGWResponseInner::new(response))
            }
            Some(_) => Box::pin(futures::stream::empty()),
        }
    }

    fn body_chunked(
        &mut self,
        chunk_size: usize,
    ) -> ProviderResponseStream
    {
        match &self.error {
            None => {
                let response = self.response.take().expect("We should have a response");

                Box::pin(ProviderResponseStreamChunk::new(
                    Box::pin(RadosGWResponseInner::new(response)),
                    chunk_size,
                ))
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
                    .list_objects(Some(max_results as _), start_after.clone())
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
