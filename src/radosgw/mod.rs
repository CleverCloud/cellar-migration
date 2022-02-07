pub mod awscredentials;
pub mod uploader;

use std::collections::HashMap;

use rusoto_core::{ByteStream, RusotoError};
use rusoto_s3::{
    AbortMultipartUploadError, AbortMultipartUploadOutput, AbortMultipartUploadRequest, Bucket,
    CompleteMultipartUploadError, CompleteMultipartUploadOutput, CompleteMultipartUploadRequest,
    CompletedMultipartUpload, CompletedPart, CreateBucketError, CreateBucketRequest,
    CreateMultipartUploadError, CreateMultipartUploadOutput, CreateMultipartUploadRequest,
    DeleteObjectError, DeleteObjectRequest, ListBucketsError, ListObjectsV2Error,
    ListObjectsV2Request, Object, PutObjectError, PutObjectOutput, PutObjectRequest, S3Client,
    UploadPartError, UploadPartOutput, UploadPartRequest, S3,
};
use tracing::{event, instrument, Level};

use crate::riakcs::dto::ObjectMetadataResponse;

#[derive(Debug, Clone)]
pub struct RadosGW {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: Option<String>,
}

impl RadosGW {
    pub fn new(
        endpoint: String,
        access_key: String,
        secret_key: String,
        bucket: Option<String>,
    ) -> RadosGW {
        RadosGW {
            endpoint,
            access_key,
            secret_key,
            bucket,
        }
    }

    #[instrument(skip(self), level = "trace")]
    fn get_client(&self) -> S3Client {
        let radosgw_credential_provider = awscredentials::AWSCredentialsProvider::new(
            self.access_key.clone(),
            self.secret_key.clone(),
        );
        let http_client = rusoto_core::HttpClient::new().unwrap();

        S3Client::new_with(
            http_client,
            radosgw_credential_provider,
            rusoto_core::Region::Custom {
                name: "RadosGW".to_string(),
                endpoint: self.endpoint.clone(),
            },
        )
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn put_object(
        &self,
        key: String,
        object_metadata: &ObjectMetadataResponse,
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
            cache_control: object_metadata.metadata.cache_control.clone(),
            content_disposition: object_metadata.metadata.content_disposition.clone(),
            content_encoding: object_metadata.metadata.content_encoding.clone(),
            content_language: object_metadata.metadata.content_language.clone(),
            content_md5: object_metadata.metadata.content_md5.clone(),
            content_type: object_metadata.metadata.content_type.clone(),
            expires: object_metadata.metadata.expires.clone(),
            ..Default::default()
        };

        let client = self.get_client();
        client.put_object(put_object_request).await
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_multipart_upload(
        &self,
        key: String,
        object_metadata: &ObjectMetadataResponse,
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
            cache_control: object_metadata.metadata.cache_control.clone(),
            content_disposition: object_metadata.metadata.content_disposition.clone(),
            content_encoding: object_metadata.metadata.content_encoding.clone(),
            content_language: object_metadata.metadata.content_language.clone(),
            content_type: object_metadata.metadata.content_type.clone(),
            expires: object_metadata.metadata.expires.clone(),
            ..Default::default()
        };

        let client = self.get_client();
        client
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

        let client = self.get_client();
        client.upload_part(part_upload_request).await
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

        let client = self.get_client();
        client
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

        let client = self.get_client();
        client
            .abort_multipart_upload(abort_multipart_upload_request)
            .await
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn list_objects(
        &self,
        max_results: Option<i64>,
    ) -> Result<HashMap<String, rusoto_s3::Object>, RusotoError<ListObjectsV2Error>> {
        let mut results = HashMap::new();
        let mut start_after = None;
        let mut total_keys: i64 = 0;

        loop {
            let list_objects_request = ListObjectsV2Request {
                bucket: self
                    .bucket
                    .clone()
                    .expect("list_objects should have a bucket"),
                start_after,
                max_keys: max_results.map(|max| std::cmp::min(max, 1000)),
                ..Default::default()
            };

            let client = self.get_client();
            let objects = client
                .list_objects_v2(list_objects_request.clone())
                .await
                .map(|res| res.contents.unwrap_or_default())?;

            if objects.is_empty() {
                break;
            }

            event!(Level::TRACE, "{:?}", objects.last());

            total_keys += objects.len() as i64;

            start_after = objects.last().and_then(|o| o.key.clone());

            for object in objects {
                results.insert(
                    object.key.clone().expect("Object should have a key"),
                    object,
                );
            }

            if let Some(max_results) = max_results {
                if total_keys >= max_results {
                    break;
                }
            }
        }

        Ok(results)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn delete_object(
        &self,
        object: Object,
    ) -> Result<Object, RusotoError<DeleteObjectError>> {
        let client = self.get_client();
        let delete_object_request = DeleteObjectRequest {
            bucket: self
                .bucket
                .clone()
                .expect("delete_object should have a bucket"),
            key: object.key.clone().unwrap(),
            ..Default::default()
        };

        client
            .delete_object(delete_object_request)
            .await
            .map(|_| object)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn list_buckets(&self) -> Result<Vec<Bucket>, RusotoError<ListBucketsError>> {
        let client = self.get_client();
        client
            .list_buckets()
            .await
            .map(|result| result.buckets.unwrap_or_default())
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn create_bucket(
        &self,
        bucket: String,
    ) -> Result<(), RusotoError<CreateBucketError>> {
        let client = self.get_client();
        // TODO: check if original bucket is public and if it is, apply the same ACL here
        // There might also be some policies, we need to create them.
        let create_bucket_request = CreateBucketRequest {
            bucket,
            ..Default::default()
        };

        client
            .create_bucket(create_bucket_request)
            .await
            .map(|_| ())
    }
}
