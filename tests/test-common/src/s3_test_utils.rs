//! S3 test utilities for bucket management and operations

use crate::{TestConfig, TestFile};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object_acl::GetObjectAclOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use base64::Engine;
use std::error::Error;
use std::path::Path;
use std::time::Instant;
use tokio::time::{sleep, Duration};

/// S3 test client wrapper with utilities for testing
pub struct S3TestClient {
    client: Client,
}

impl S3TestClient {
    /// Create a new S3 test client for source bucket operations
    pub async fn new_source(config: TestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Use the same approach as the main migration tool's RadosGW client
        use aws_config::{
            retry::RetryConfig, retry::RetryMode, timeout::TimeoutConfig, BehaviorVersion,
            SdkConfig,
        };
        use aws_sdk_s3::config::retry::RetryConfigBuilder;

        let creds = Credentials::from_keys(
            config.src_access_key.clone(),
            config.src_secret_key.clone(),
            None,
        );
        let timeout_config = TimeoutConfig::builder()
            .operation_timeout(std::time::Duration::from_secs(30))
            .operation_attempt_timeout(std::time::Duration::from_secs(600))
            .connect_timeout(std::time::Duration::from_secs(3))
            .build();
        let retry_config = RetryConfigBuilder::new()
            .mode(RetryMode::Standard)
            .max_attempts(5)
            .build();
        let mut sdk_config = SdkConfig::builder()
            .retry_config(retry_config)
            .timeout_config(timeout_config);

        let region = aws_sdk_s3::config::Region::new("us-east-1".to_string()); // Use us-east-1 for Cellar compatibility
        sdk_config
            .set_region(region)
            .set_endpoint_url(Some(config.src_endpoint.clone()));

        let sdk_config = sdk_config.build();
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .credentials_provider(creds)
            .behavior_version(BehaviorVersion::v2023_11_09())
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client })
    }

    /// Create a new S3 test client for destination bucket operations
    pub async fn new_destination(config: TestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Use the same approach as the main migration tool's RadosGW client
        use aws_config::{
            retry::RetryConfig, retry::RetryMode, timeout::TimeoutConfig, BehaviorVersion,
            SdkConfig,
        };
        use aws_sdk_s3::config::retry::RetryConfigBuilder;

        let creds = Credentials::from_keys(
            config.dst_access_key.clone(),
            config.dst_secret_key.clone(),
            None,
        );
        let timeout_config = TimeoutConfig::builder()
            .operation_timeout(std::time::Duration::from_secs(30))
            .operation_attempt_timeout(std::time::Duration::from_secs(600))
            .connect_timeout(std::time::Duration::from_secs(3))
            .build();
        let retry_config = RetryConfigBuilder::new()
            .mode(RetryMode::Standard)
            .max_attempts(5)
            .build();
        let mut sdk_config = SdkConfig::builder()
            .retry_config(retry_config)
            .timeout_config(timeout_config);

        let region = aws_sdk_s3::config::Region::new("us-east-1".to_string()); // Default region for Cellar
        sdk_config
            .set_region(region)
            .set_endpoint_url(Some(config.dst_endpoint.clone()));

        let sdk_config = sdk_config.build();
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .credentials_provider(creds)
            .behavior_version(BehaviorVersion::v2023_11_09())
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            )
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client })
    }

    /// Create a bucket for testing
    pub async fn create_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .create_bucket()
            .bucket(bucket_name)
            .send()
            .await?;
        Ok(())
    }

    /// Delete a bucket and all its contents
    pub async fn delete_bucket(&self, bucket_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // First delete all objects in the bucket
        let mut continuation_token: Option<String> = None;

        loop {
            let mut list_request = self
                .client
                .list_objects_v2()
                .bucket(bucket_name)
                .max_keys(1000);

            if let Some(token) = &continuation_token {
                list_request = list_request.continuation_token(token);
            }

            let response = list_request.send().await?;

            let objects = response.contents();
            if !objects.is_empty() {
                // Delete objects in batches
                for chunk in objects.chunks(1000) {
                    for object in chunk {
                        if let Some(key) = &object.key {
                            self.client
                                .delete_object()
                                .bucket(bucket_name)
                                .key(key)
                                .send()
                                .await?;
                        }
                    }
                }
            }

            if !response.is_truncated().unwrap_or(false) {
                break;
            }

            continuation_token = response.next_continuation_token().map(|s| s.to_string());
        }

        // Now delete the bucket itself
        self.client
            .delete_bucket()
            .bucket(bucket_name)
            .send()
            .await?;

        Ok(())
    }

    /// Upload a test file to S3 with metadata
    pub async fn upload_test_file(
        &self,
        bucket_name: &str,
        test_file: &TestFile,
        file_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();

        // Log upload start for larger files or files with metadata
        if test_file.size > 1_000_000 || test_file.content_type.is_some() {
            println!(
                "Uploading: {} -> s3://{}/{} (size: {} bytes, content_type: {:?})",
                file_path.display(),
                bucket_name,
                test_file.key(),
                test_file.size,
                test_file.content_type.as_deref().unwrap_or("none")
            );
        }

        const MAX_ATTEMPTS: usize = 5;
        const INITIAL_BACKOFF_MS: u64 = 200;

        for attempt in 1..=MAX_ATTEMPTS {
            if attempt > 1 {
                println!(
                    "Retrying upload for {} (attempt {}/{})",
                    test_file.key(),
                    attempt,
                    MAX_ATTEMPTS
                );
            }

            // Read file content to calculate MD5 for compatibility with older S3 implementations
            let file_content = std::fs::read(file_path)?;
            let md5_hash = md5::compute(&file_content);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(md5_hash.as_ref());

            let body = ByteStream::from(file_content);

            let mut put_request = self
                .client
                .put_object()
                .bucket(bucket_name)
                .key(test_file.key())
                .body(body)
                .content_md5(&content_md5); // Set Content-MD5 explicitly for compatibility

            // Add metadata if present
            if let Some(content_type) = &test_file.content_type {
                put_request = put_request.content_type(content_type);
            }

            if let Some(cache_control) = &test_file.cache_control {
                put_request = put_request.cache_control(cache_control);
            }

            if let Some(content_disposition) = &test_file.content_disposition {
                put_request = put_request.content_disposition(content_disposition);
            }

            if let Some(content_encoding) = &test_file.content_encoding {
                put_request = put_request.content_encoding(content_encoding);
            }

            if let Some(content_language) = &test_file.content_language {
                put_request = put_request.content_language(content_language);
            }

            if let Some(expires) = &test_file.expires {
                // Convert chrono DateTime to timestamp and then to aws_smithy_types::DateTime
                let timestamp = expires.timestamp();
                put_request = put_request.expires(aws_smithy_types::DateTime::from_secs(timestamp));
            }

            if !test_file.metadata.is_empty() {
                for (key, value) in &test_file.metadata {
                    put_request = put_request.metadata(key.clone(), value.clone());
                }
            }

            if test_file.acl_public {
                put_request = put_request.acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead);
            }

            // Try setting Content-MD5 manually to avoid newer checksum algorithms
            // that might not be supported by older S3-compatible services

            match put_request.send().await {
                Ok(_) => {
                    if attempt > 1 {
                        println!(
                            "Upload succeeded for {} after {} attempts",
                            test_file.key(),
                            attempt
                        );
                    }
                    break;
                }
                Err(err) => {
                    let (retryable, detail) = match &err {
                        SdkError::DispatchFailure(dispatch_err) => {
                            let mut detail_parts = Vec::new();
                            if dispatch_err.is_timeout() {
                                detail_parts.push("timeout".to_string());
                            }
                            if dispatch_err.is_io() {
                                detail_parts.push("io".to_string());
                            }
                            if dispatch_err.is_other() {
                                detail_parts.push("other".to_string());
                            }
                            if dispatch_err.is_user() {
                                detail_parts.push("user".to_string());
                            }

                            if let Some(connector_error) = dispatch_err.as_connector_error() {
                                detail_parts.push(format!("connector: {}", connector_error));

                                if let Some(source) = connector_error.source() {
                                    let source_msg = source.to_string();
                                    if !source_msg.is_empty() {
                                        detail_parts.push(format!("source: {}", source_msg));
                                    }
                                }
                            }

                            let retryable = !dispatch_err.is_user();
                            let detail = if detail_parts.is_empty() {
                                "dispatch failure".to_string()
                            } else {
                                format!("dispatch failure ({})", detail_parts.join(", "))
                            };

                            (retryable, detail)
                        }
                        SdkError::TimeoutError(_) => (true, "timeout error".to_string()),
                        SdkError::ResponseError(_) => (true, "response parsing error".to_string()),
                        SdkError::ServiceError(service_err) => {
                            use aws_smithy_types::error::metadata::ProvideErrorMetadata;

                            let status = service_err.raw().status();
                            let code = service_err.err().code();
                            let message = service_err.err().message();
                            let retryable = status.is_server_error()
                                || status.as_u16() == 429
                                || matches!(
                                    code,
                                    Some("SlowDown")
                                        | Some("RequestTimeout")
                                        | Some("Throttling")
                                        | Some("InternalError")
                                );

                            let mut detail = format!("service error (status {})", status);
                            if let Some(code) = code {
                                detail.push_str(&format!(" code {}", code));
                            }
                            if let Some(message) = message {
                                detail.push_str(&format!(" message {}", message));
                            }

                            (retryable, detail)
                        }
                        SdkError::ConstructionFailure(_) => {
                            (false, "request construction failure".to_string())
                        }
                        other => (false, format!("unhandled error: {other:?}")),
                    };
                    if attempt == MAX_ATTEMPTS || !retryable {
                        return Err(format!(
                            "PutObject {} -> {} failed after {} attempts: {}",
                            test_file.key(),
                            bucket_name,
                            attempt,
                            detail
                        )
                        .into());
                    }

                    let backoff_ms = INITIAL_BACKOFF_MS * (1u64 << (attempt - 1));
                    println!(
                        "Upload attempt {} for {} failed: {}; retrying in {} ms",
                        attempt,
                        test_file.key(),
                        detail,
                        backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }

        // Log completion for larger files
        if test_file.size > 1_000_000 {
            println!(
                "Uploaded {} in {:.2}s",
                test_file.key(),
                start_time.elapsed().as_secs_f64()
            );
        }

        Ok(())
    }

    /// Download an object from S3
    pub async fn download_object(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let response = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await?;

        let bytes = response.body.collect().await?.into_bytes();
        Ok(bytes.to_vec())
    }

    /// Get object metadata
    pub async fn get_object_metadata(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<aws_sdk_s3::operation::head_object::HeadObjectOutput, Box<dyn std::error::Error>>
    {
        let response = self
            .client
            .head_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await?;

        Ok(response)
    }

    /// Retrieve the ACL for an object
    pub async fn get_object_acl(
        &self,
        bucket_name: &str,
        key: &str,
    ) -> Result<GetObjectAclOutput, Box<dyn std::error::Error>> {
        let response = self
            .client
            .get_object_acl()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await?;

        Ok(response)
    }

    /// List all objects in a bucket
    pub async fn list_all_objects(
        &self,
        bucket_name: &str,
    ) -> Result<Vec<aws_sdk_s3::types::Object>, Box<dyn std::error::Error>> {
        const MAX_ATTEMPTS: usize = 5;
        const INITIAL_BACKOFF_MS: u64 = 200;

        println!("Starting to list all objects in bucket: {}", bucket_name);
        println!("Client region: {:?}", self.client.config().region());
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut page_count = 0;
        let mut attempt = 0usize;

        loop {
            page_count += 1;
            println!(
                "Listing objects - page {}, current total: {} objects",
                page_count,
                objects.len()
            );

            let mut list_request = self
                .client
                .list_objects_v2()
                .bucket(bucket_name)
                .max_keys(1000);

            if let Some(token) = &continuation_token {
                println!("Using continuation token: {}", token);
                list_request = list_request.continuation_token(token);
            } else {
                println!("No continuation token (first page)");
            }

            let response = loop {
                println!("Sending list_objects_v2 request...");
                println!(
                    "Request details: bucket={}, max_keys=1000, continuation_token={:?}",
                    bucket_name, continuation_token
                );

                match list_request.clone().send().await {
                    Ok(response) => {
                        println!("✓ Received successful response from list_objects_v2");
                        println!(
                            "Response details: is_truncated={:?}, key_count={:?}, max_keys={:?}",
                            response.is_truncated(),
                            response.key_count(),
                            response.max_keys()
                        );
                        attempt = 0;
                        break response;
                    }
                    Err(err) => {
                        attempt += 1;

                        let retryable = match &err {
                            SdkError::DispatchFailure(dispatch_err) => !dispatch_err.is_user(),
                            SdkError::TimeoutError(_) => true,
                            SdkError::ResponseError(_) => true,
                            SdkError::ServiceError(service_err) => {
                                let status = service_err.raw().status();
                                let code = service_err.err().code();
                                status.is_server_error()
                                    || status.as_u16() == 429
                                    || matches!(
                                        code,
                                        Some("SlowDown")
                                            | Some("RequestTimeout")
                                            | Some("Throttling")
                                            | Some("InternalError")
                                    )
                            }
                            SdkError::ConstructionFailure(_) => false,
                            _ => false,
                        };

                        if !retryable || attempt > MAX_ATTEMPTS {
                            println!("✗ list_objects_v2 request failed: {}", err);
                            return Err(err.into());
                        }

                        let backoff = INITIAL_BACKOFF_MS * (1 << (attempt - 1));
                        println!(
                            "⚠ list_objects_v2 attempt {} failed ({}); retrying in {} ms",
                            attempt, err, backoff
                        );
                        sleep(Duration::from_millis(backoff)).await;
                    }
                }
            };

            let contents = response.contents();
            println!("Page {} returned {} objects", page_count, contents.len());
            objects.extend_from_slice(contents);

            let is_truncated = response.is_truncated().unwrap_or(false);
            println!("Response is_truncated: {}", is_truncated);

            if !is_truncated {
                println!("Response not truncated, breaking pagination loop");
                break;
            }

            continuation_token = response.next_continuation_token().map(|s| s.to_string());
            println!("Next continuation token: {:?}", continuation_token);

            if continuation_token.is_none() {
                println!("Warning: Response is truncated but no continuation token provided");
                break;
            }
        }

        println!(
            "Completed listing all objects in bucket: {} (total: {} objects, {} pages)",
            bucket_name,
            objects.len(),
            page_count
        );
        Ok(objects)
    }

    /// Check if bucket exists
    pub async fn bucket_exists(&self, bucket_name: &str) -> bool {
        self.client
            .head_bucket()
            .bucket(bucket_name)
            .send()
            .await
            .is_ok()
    }

    /// Wait until a bucket exists (useful right after create_bucket on eventually consistent backends)
    pub async fn wait_until_bucket_exists(
        &self,
        bucket_name: &str,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        loop {
            if self.bucket_exists(bucket_name).await {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(
                    format!("Timeout waiting for bucket '{}' to exist", bucket_name).into(),
                );
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Enable versioning on a bucket
    pub async fn enable_bucket_versioning(
        &self,
        bucket_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .put_bucket_versioning()
            .bucket(bucket_name)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }

    /// Get bucket versioning status
    pub async fn get_bucket_versioning(
        &self,
        bucket_name: &str,
    ) -> Result<aws_sdk_s3::types::BucketVersioningStatus, Box<dyn std::error::Error>> {
        let response = self
            .client
            .get_bucket_versioning()
            .bucket(bucket_name)
            .send()
            .await?;

        Ok(response
            .status()
            .cloned()
            .unwrap_or(aws_sdk_s3::types::BucketVersioningStatus::Suspended))
    }

    /// List all versions of objects in a bucket
    pub async fn list_object_versions(
        &self,
        bucket_name: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<aws_sdk_s3::types::ObjectVersion>, Box<dyn std::error::Error>> {
        let mut versions = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_id_marker: Option<String> = None;

        loop {
            let mut list_request = self
                .client
                .list_object_versions()
                .bucket(bucket_name)
                .max_keys(1000);

            if let Some(prefix) = prefix {
                list_request = list_request.prefix(prefix);
            }

            if let Some(key_marker) = &key_marker {
                list_request = list_request.key_marker(key_marker);
            }

            if let Some(version_id_marker) = &version_id_marker {
                list_request = list_request.version_id_marker(version_id_marker);
            }

            let response = list_request.send().await?;

            let object_versions = response.versions();
            versions.extend_from_slice(object_versions);

            let is_truncated = response.is_truncated().unwrap_or(false);
            if !is_truncated {
                break;
            }

            key_marker = response.next_key_marker().map(|s| s.to_string());
            version_id_marker = response.next_version_id_marker().map(|s| s.to_string());
        }

        Ok(versions)
    }

    /// Get object content for a specific version
    pub async fn get_object_version(
        &self,
        bucket_name: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let response = self
            .client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;

        let bytes = response.body.collect().await?.into_bytes();
        Ok(bytes.to_vec())
    }

    /// Get object metadata for a specific version
    pub async fn get_object_metadata_version(
        &self,
        bucket_name: &str,
        key: &str,
        version_id: &str,
    ) -> Result<aws_sdk_s3::operation::head_object::HeadObjectOutput, Box<dyn std::error::Error>>
    {
        let response = self
            .client
            .head_object()
            .bucket(bucket_name)
            .key(key)
            .version_id(version_id)
            .send()
            .await?;

        Ok(response)
    }

    /// Upload a test file to S3 with no metadata, returning the version ID
    pub async fn upload_test_file_versioned(
        &self,
        bucket_name: &str,
        object_key: &str,
        file_path: &Path,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        let file_size = std::fs::metadata(file_path)?.len();

        if file_size > 1_000_000 {
            println!(
                "Uploading versioned: {} -> s3://{}/{} (size: {} bytes)",
                file_path.display(),
                bucket_name,
                object_key,
                file_size
            );
        }

        const MAX_ATTEMPTS: usize = 5;
        const INITIAL_BACKOFF_MS: u64 = 200;

        for attempt in 1..=MAX_ATTEMPTS {
            if attempt > 1 {
                println!(
                    "Retrying versioned upload for {} (attempt {}/{})",
                    object_key,
                    attempt,
                    MAX_ATTEMPTS
                );
            }

            let file_content = std::fs::read(file_path)?;
            let md5_hash = md5::compute(&file_content);
            let content_md5 = base64::engine::general_purpose::STANDARD.encode(md5_hash.as_ref());

            let body = ByteStream::from(file_content);

            // Clean upload with no metadata - only essential S3 parameters
            let put_request = self
                .client
                .put_object()
                .bucket(bucket_name)
                .key(object_key)
                .body(body)
                .content_md5(&content_md5);

            match put_request.send().await {
                Ok(output) => {
                    if attempt > 1 {
                        println!(
                            "Versioned upload succeeded for {} after {} attempts",
                            object_key,
                            attempt
                        );
                    }

                    let version_id = output
                        .version_id()
                        .ok_or_else(|| {
                            format!("No version ID returned for upload of {}", object_key)
                        })?
                        .to_string();

                    if file_size > 1_000_000 {
                        println!(
                            "Uploaded versioned {} (version: {}) in {:.2}s",
                            object_key,
                            version_id,
                            start_time.elapsed().as_secs_f64()
                        );
                    }

                    return Ok(version_id);
                }
                Err(err) => {
                    let (retryable, detail) = match &err {
                        SdkError::DispatchFailure(dispatch_err) => {
                            let mut detail_parts = Vec::new();
                            if dispatch_err.is_timeout() {
                                detail_parts.push("timeout".to_string());
                            }
                            if dispatch_err.is_io() {
                                detail_parts.push("io".to_string());
                            }
                            if dispatch_err.is_other() {
                                detail_parts.push("other".to_string());
                            }
                            if dispatch_err.is_user() {
                                detail_parts.push("user".to_string());
                            }

                            if let Some(connector_error) = dispatch_err.as_connector_error() {
                                detail_parts.push(format!("connector: {}", connector_error));

                                if let Some(source) = connector_error.source() {
                                    let source_msg = source.to_string();
                                    if !source_msg.is_empty() {
                                        detail_parts.push(format!("source: {}", source_msg));
                                    }
                                }
                            }

                            let retryable = !dispatch_err.is_user();
                            let detail = if detail_parts.is_empty() {
                                "dispatch failure".to_string()
                            } else {
                                format!("dispatch failure ({})", detail_parts.join(", "))
                            };

                            (retryable, detail)
                        }
                        SdkError::TimeoutError(_) => (true, "timeout error".to_string()),
                        SdkError::ResponseError(_) => (true, "response parsing error".to_string()),
                        SdkError::ServiceError(service_err) => {
                            use aws_smithy_types::error::metadata::ProvideErrorMetadata;

                            let status = service_err.raw().status();
                            let code = service_err.err().code();
                            let message = service_err.err().message();
                            let retryable = status.is_server_error()
                                || status.as_u16() == 429
                                || matches!(
                                    code,
                                    Some("SlowDown")
                                        | Some("RequestTimeout")
                                        | Some("Throttling")
                                        | Some("InternalError")
                                );

                            let mut detail = format!("service error (status {})", status);
                            if let Some(code) = code {
                                detail.push_str(&format!(" code {}", code));
                            }
                            if let Some(message) = message {
                                detail.push_str(&format!(" message {}", message));
                            }

                            (retryable, detail)
                        }
                        SdkError::ConstructionFailure(_) => {
                            (false, "request construction failure".to_string())
                        }
                        other => (false, format!("unhandled error: {other:?}")),
                    };
                    if attempt == MAX_ATTEMPTS || !retryable {
                        return Err(format!(
                            "PutObject versioned {} -> {} failed after {} attempts: {}",
                            object_key,
                            bucket_name,
                            attempt,
                            detail
                        )
                        .into());
                    }

                    let backoff_ms = INITIAL_BACKOFF_MS * (1u64 << (attempt - 1));
                    println!(
                        "Versioned upload attempt {} for {} failed: {}; retrying in {} ms",
                        attempt,
                        object_key,
                        detail,
                        backoff_ms
                    );
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }

        Err("Versioned upload failed after all attempts".into())
    }
}

/// Test bucket manager for creating and cleaning up test buckets
pub struct TestBucketManager {
    source_client: S3TestClient,
    dest_client: S3TestClient,
    config: TestConfig,
    created_buckets: Vec<String>,
}

impl TestBucketManager {
    /// Create a new test bucket manager
    pub async fn new(config: TestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let source_client = S3TestClient::new_source(config.clone()).await?;
        let dest_client = S3TestClient::new_destination(config.clone()).await?;

        Ok(Self {
            source_client,
            dest_client,
            config,
            created_buckets: Vec::new(),
        })
    }

    /// Create a test bucket pair (source and destination)
    pub async fn create_test_buckets(
        &mut self,
        test_name: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        println!("[{}] Preparing test buckets", test_name);
        let src_bucket = self.config.generate_bucket_name(test_name, "src");
        let dst_bucket = self.config.generate_bucket_name(test_name, "dst");

        println!(
            "[{}] Initializing source bucket: {} (endpoint: {})",
            test_name, src_bucket, self.config.src_endpoint
        );
        self.source_client.create_bucket(&src_bucket).await?;
        println!("[{}] Source bucket created", test_name);

        println!(
            "[{}] Initializing destination bucket: {} (endpoint: {})",
            test_name, dst_bucket, self.config.dst_endpoint
        );
        self.dest_client.create_bucket(&dst_bucket).await?;
        println!("[{}] Destination bucket created", test_name);

        // Some S3-compatible providers may take a short time to make new buckets visible.
        // Wait until both buckets are ready before proceeding with uploads/lists.
        println!(
            "[{}] Waiting for source bucket to become available",
            test_name
        );
        self.source_client
            .wait_until_bucket_exists(&src_bucket, Duration::from_secs(10))
            .await?;
        println!("[{}] Source bucket ready", test_name);

        println!(
            "[{}] Waiting for destination bucket to become available",
            test_name
        );
        self.dest_client
            .wait_until_bucket_exists(&dst_bucket, Duration::from_secs(10))
            .await?;
        println!("[{}] Destination bucket ready", test_name);

        self.created_buckets.push(src_bucket.clone());
        self.created_buckets.push(dst_bucket.clone());

        println!(
            "[{}] Test buckets ready: source={}, destination={}",
            test_name, src_bucket, dst_bucket
        );
        Ok((src_bucket, dst_bucket))
    }

    /// Create versioned test buckets (source and destination with versioning enabled)
    pub async fn create_versioned_test_buckets(
        &mut self,
        test_name: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error>> {
        println!("[{}] Preparing versioned test buckets", test_name);
        let (src_bucket, dst_bucket) = self.create_test_buckets(test_name).await?;

        // Enable versioning on source bucket
        println!("[{}] Enabling versioning on source bucket", test_name);
        self.source_client.enable_bucket_versioning(&src_bucket).await?;

        // Enable versioning on destination bucket
        println!("[{}] Enabling versioning on destination bucket", test_name);
        self.dest_client.enable_bucket_versioning(&dst_bucket).await?;

        // Verify versioning is enabled
        let src_versioning = self.source_client.get_bucket_versioning(&src_bucket).await?;
        let dst_versioning = self.dest_client.get_bucket_versioning(&dst_bucket).await?;

        if src_versioning != aws_sdk_s3::types::BucketVersioningStatus::Enabled {
            return Err(format!(
                "Failed to enable versioning on source bucket: {:?}",
                src_versioning
            )
            .into());
        }

        if dst_versioning != aws_sdk_s3::types::BucketVersioningStatus::Enabled {
            return Err(format!(
                "Failed to enable versioning on destination bucket: {:?}",
                dst_versioning
            )
            .into());
        }

        println!(
            "[{}] Versioning enabled on both buckets: source={}, destination={}",
            test_name, src_bucket, dst_bucket
        );

        Ok((src_bucket, dst_bucket))
    }

    /// Get the source client
    pub fn source_client(&self) -> &S3TestClient {
        &self.source_client
    }

    /// Get the destination client
    pub fn dest_client(&self) -> &S3TestClient {
        &self.dest_client
    }

    /// Clean up all created buckets
    pub async fn cleanup(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.config.skip_cleanup {
            println!("Skipping cleanup as TEST_SKIP_CLEANUP=true");
            return Ok(());
        }

        for bucket_name in &self.created_buckets {
            // Try to delete from both clients (one might fail if bucket doesn't exist on that provider)
            let _ = self.source_client.delete_bucket(bucket_name).await;
            let _ = self.dest_client.delete_bucket(bucket_name).await;
        }

        Ok(())
    }
}
