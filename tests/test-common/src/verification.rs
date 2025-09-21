//! Content and metadata verification utilities for migration tests

use crate::{S3TestClient, TestFile};
use aws_sdk_s3::operation::get_object_acl::GetObjectAclOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::Permission;
use chrono::{DateTime, Utc};

/// Verification result for comparing source and destination objects
#[derive(Debug, PartialEq)]
pub enum VerificationResult {
    Match,
    ContentMismatch,
    MetadataMismatch(String),
    SizeMismatch { expected: usize, actual: usize },
}

fn normalize_etag(etag: Option<&str>) -> Option<String> {
    etag.map(|value| value.trim_matches('"').to_string())
}

fn acl_has_public_read(acl: &GetObjectAclOutput) -> bool {
    acl.grants().iter().any(|grant| {
        matches!(grant.permission(), Some(Permission::Read))
            && grant
                .grantee()
                .and_then(|grantee| grantee.uri())
                .map(|uri| uri == "http://acs.amazonaws.com/groups/global/AllUsers")
                .unwrap_or(false)
    })
}

/// Allow second-level precision differences when comparing expiry metadata
fn expires_matches(expected: &DateTime<Utc>, actual: Option<&DateTime<Utc>>) -> bool {
    match actual {
        Some(actual_dt) => actual_dt == expected || actual_dt.timestamp() == expected.timestamp(),
        None => false,
    }
}

/// Utilities for verifying migration results
pub struct MigrationVerifier;

impl MigrationVerifier {
    /// Verify that a file was migrated correctly by comparing source and destination S3 objects only
    /// This method intentionally avoids verifying content against the locally generated file on disk.
    pub async fn verify_migration_s3_only(
        source_client: &S3TestClient,
        dest_client: &S3TestClient,
        src_bucket: &str,
        dst_bucket: &str,
        test_file: &TestFile,
    ) -> Result<VerificationResult, Box<dyn std::error::Error>> {
        println!("Verifying migration: {}", test_file.key());

        // Download content from both source and destination and compare byte-by-byte
        println!("  Downloading content from source and destination for comparison...");
        let src_content = source_client
            .download_object(src_bucket, &test_file.key())
            .await?;
        let dst_content = dest_client
            .download_object(dst_bucket, &test_file.key())
            .await?;

        if src_content != dst_content {
            println!("  ✗ Content mismatch for {}", test_file.key());
            return Ok(VerificationResult::ContentMismatch);
        }
        println!("  ✓ Content matches ({} bytes)", src_content.len());

        // Fetch metadata from both sides
        println!("  Comparing metadata...");
        let src_metadata = source_client
            .get_object_metadata(src_bucket, &test_file.key())
            .await?;
        let dst_metadata = dest_client
            .get_object_metadata(dst_bucket, &test_file.key())
            .await?;

        if let Some(error) = Self::verify_metadata(test_file, &src_metadata, &dst_metadata) {
            println!("  ✗ {}", error);
            return Ok(VerificationResult::MetadataMismatch(error));
        }

        // Compare user metadata in detail for logging clarity
        if !test_file.metadata.is_empty() {
            println!("  ✓ User metadata preserved: {:?}", test_file.metadata);
        }

        // Verify ACL state matches expectations (e.g., public read)
        let src_acl = source_client
            .get_object_acl(src_bucket, &test_file.key())
            .await?;
        let dst_acl = dest_client
            .get_object_acl(dst_bucket, &test_file.key())
            .await?;

        let expected_public = test_file.acl_public;
        let src_public = acl_has_public_read(&src_acl);
        let dst_public = acl_has_public_read(&dst_acl);

        if src_public != expected_public {
            let msg = format!(
                "Source ACL mismatch: expected public={}, got {}",
                expected_public, src_public
            );
            println!("  ✗ {}", msg);
            return Ok(VerificationResult::MetadataMismatch(msg));
        }

        if dst_public != expected_public {
            let msg = format!(
                "Destination ACL mismatch: expected public={}, got {}",
                expected_public, dst_public
            );
            println!("  ✗ {}", msg);
            return Ok(VerificationResult::MetadataMismatch(msg));
        }

        if src_public != dst_public {
            let msg = format!(
                "Source/Destination ACL mismatch: src_public={}, dst_public={}",
                src_public, dst_public
            );
            println!("  ✗ {}", msg);
            return Ok(VerificationResult::MetadataMismatch(msg));
        }

        // Compare ETag/MD5 between source and destination when multipart uploads are not involved
        let src_etag = normalize_etag(src_metadata.e_tag());
        let dst_etag = normalize_etag(dst_metadata.e_tag());
        if let (Some(src_etag), Some(dst_etag)) = (&src_etag, &dst_etag) {
            if !src_etag.contains('-') && !dst_etag.contains('-') && src_etag != dst_etag {
                let msg = format!(
                    "ETag mismatch despite single-part upload: src={}, dst={}",
                    src_etag, dst_etag
                );
                println!("  ✗ {}", msg);
                return Ok(VerificationResult::MetadataMismatch(msg));
            }
        }

        println!("  ✓ Metadata matches");
        println!("✓ Migration verified successfully: {}", test_file.key());

        Ok(VerificationResult::Match)
    }

    /// Verify that metadata was preserved correctly during migration
    fn verify_metadata(
        test_file: &TestFile,
        src_metadata: &HeadObjectOutput,
        dst_metadata: &HeadObjectOutput,
    ) -> Option<String> {
        // Verify Content-Type
        if let Some(expected_content_type) = &test_file.content_type {
            if src_metadata.content_type() != Some(expected_content_type) {
                return Some(format!(
                    "Source Content-Type mismatch: expected {}, got {:?}",
                    expected_content_type,
                    src_metadata.content_type()
                ));
            }
            if dst_metadata.content_type() != Some(expected_content_type) {
                return Some(format!(
                    "Destination Content-Type mismatch: expected {}, got {:?}",
                    expected_content_type,
                    dst_metadata.content_type()
                ));
            }
        }

        // Verify Cache-Control
        if let Some(expected_cache_control) = &test_file.cache_control {
            if src_metadata.cache_control() != Some(expected_cache_control) {
                return Some(format!(
                    "Source Cache-Control mismatch: expected {}, got {:?}",
                    expected_cache_control,
                    src_metadata.cache_control()
                ));
            }
            if dst_metadata.cache_control() != Some(expected_cache_control) {
                return Some(format!(
                    "Destination Cache-Control mismatch: expected {}, got {:?}",
                    expected_cache_control,
                    dst_metadata.cache_control()
                ));
            }
        }

        // Verify Content-Disposition
        if let Some(expected_content_disposition) = &test_file.content_disposition {
            if src_metadata.content_disposition() != Some(expected_content_disposition) {
                return Some(format!(
                    "Source Content-Disposition mismatch: expected {}, got {:?}",
                    expected_content_disposition,
                    src_metadata.content_disposition()
                ));
            }
            if dst_metadata.content_disposition() != Some(expected_content_disposition) {
                return Some(format!(
                    "Destination Content-Disposition mismatch: expected {}, got {:?}",
                    expected_content_disposition,
                    dst_metadata.content_disposition()
                ));
            }
        }

        // Verify Content-Encoding
        if let Some(expected_content_encoding) = &test_file.content_encoding {
            if src_metadata.content_encoding() != Some(expected_content_encoding) {
                return Some(format!(
                    "Source Content-Encoding mismatch: expected {}, got {:?}",
                    expected_content_encoding,
                    src_metadata.content_encoding()
                ));
            }
            if dst_metadata.content_encoding() != Some(expected_content_encoding) {
                return Some(format!(
                    "Destination Content-Encoding mismatch: expected {}, got {:?}",
                    expected_content_encoding,
                    dst_metadata.content_encoding()
                ));
            }
        }

        // Verify Content-Language
        if let Some(expected_content_language) = &test_file.content_language {
            if src_metadata.content_language() != Some(expected_content_language) {
                return Some(format!(
                    "Source Content-Language mismatch: expected {}, got {:?}",
                    expected_content_language,
                    src_metadata.content_language()
                ));
            }
            if dst_metadata.content_language() != Some(expected_content_language) {
                return Some(format!(
                    "Destination Content-Language mismatch: expected {}, got {:?}",
                    expected_content_language,
                    dst_metadata.content_language()
                ));
            }
        }

        // Verify Content-Length
        let expected_length = test_file.size as i64;
        if src_metadata.content_length() != Some(expected_length) {
            return Some(format!(
                "Source Content-Length mismatch: expected {}, got {:?}",
                expected_length,
                src_metadata.content_length()
            ));
        }
        if dst_metadata.content_length() != Some(expected_length) {
            return Some(format!(
                "Destination Content-Length mismatch: expected {}, got {:?}",
                expected_length,
                dst_metadata.content_length()
            ));
        }

        // Verify Expires header when provided
        if let Some(expected_expires) = &test_file.expires {
            let src_expires = src_metadata
                .expires_string()
                .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            if !expires_matches(expected_expires, src_expires.as_ref()) {
                return Some(format!(
                    "Source Expires mismatch: expected {}, got {:?}",
                    expected_expires, src_expires
                ));
            }

            let dst_expires = dst_metadata
                .expires_string()
                .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            if !expires_matches(expected_expires, dst_expires.as_ref()) {
                return Some(format!(
                    "Destination Expires mismatch: expected {}, got {:?}",
                    expected_expires, dst_expires
                ));
            }
        }

        // Verify user-defined metadata (x-amz-meta-*)
        if !test_file.metadata.is_empty() {
            let src_meta_map = src_metadata.metadata();
            let dst_meta_map = dst_metadata.metadata();

            for (key, expected_value) in &test_file.metadata {
                let src_value = src_meta_map
                    .and_then(|map| map.get(key))
                    .map(|value| value.as_str());
                if src_value != Some(expected_value.as_str()) {
                    return Some(format!(
                        "Source user metadata mismatch for {}: expected {}, got {:?}",
                        key, expected_value, src_value
                    ));
                }

                let dst_value = dst_meta_map
                    .and_then(|map| map.get(key))
                    .map(|value| value.as_str());
                if dst_value != Some(expected_value.as_str()) {
                    return Some(format!(
                        "Destination user metadata mismatch for {}: expected {}, got {:?}",
                        key, expected_value, dst_value
                    ));
                }
            }
        }

        // Verify ETag/MD5 when explicitly requested and single-part upload is expected
        if let Some(expected_md5) = &test_file.expected_md5 {
            let src_etag = normalize_etag(src_metadata.e_tag());
            if src_etag.as_ref() != Some(expected_md5) {
                return Some(format!(
                    "Source ETag mismatch: expected {}, got {:?}",
                    expected_md5, src_etag
                ));
            }

            let dst_etag = normalize_etag(dst_metadata.e_tag());
            if dst_etag.as_ref() != Some(expected_md5) {
                return Some(format!(
                    "Destination ETag mismatch: expected {}, got {:?}",
                    expected_md5, dst_etag
                ));
            }
        }

        // Note: We skip ETag comparison for multipart uploads as mentioned in the test plan
        // Note: We skip Last-Modified comparison as destination sets its own timestamp

        None
    }

    /// Verify that all objects from source exist in destination
    pub async fn verify_all_objects_migrated(
        source_client: &S3TestClient,
        dest_client: &S3TestClient,
        src_bucket: &str,
        dst_bucket: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        println!(
            "Verifying all objects migrated from {} to {}",
            src_bucket, dst_bucket
        );

        let src_objects = source_client.list_all_objects(src_bucket).await?;
        let dst_objects = dest_client.list_all_objects(dst_bucket).await?;

        println!(
            "Source bucket {} contains {} objects",
            src_bucket,
            src_objects.len()
        );
        println!(
            "Destination bucket {} contains {} objects",
            dst_bucket,
            dst_objects.len()
        );

        // Create a set of destination keys for quick lookup
        let dst_keys: std::collections::HashSet<String> = dst_objects
            .iter()
            .filter_map(|obj| obj.key.clone())
            .collect();

        let mut missing_objects = Vec::new();

        // Check that all source objects exist in destination
        for src_obj in &src_objects {
            if let Some(src_key) = &src_obj.key {
                if !dst_keys.contains(src_key) {
                    missing_objects.push(src_key.clone());
                }
            }
        }

        if !missing_objects.is_empty() {
            println!("Missing {} objects in destination:", missing_objects.len());
            for (i, key) in missing_objects.iter().enumerate() {
                if i < 5 {
                    // Show first 5 missing objects
                    println!("  - {}", key);
                } else if i == 5 {
                    println!("  - ... and {} more", missing_objects.len() - 5);
                    break;
                }
            }
            return Ok(false);
        }

        // Check that object counts match
        let counts_match = src_objects.len() == dst_objects.len();
        if counts_match {
            println!("✓ All {} objects successfully migrated", src_objects.len());
        } else {
            println!(
                "✗ Object count mismatch: source has {}, destination has {}",
                src_objects.len(),
                dst_objects.len()
            );
        }

        Ok(counts_match)
    }

    /// Verify pagination behavior by checking object count consistency
    pub async fn verify_pagination_consistency(
        client: &S3TestClient,
        bucket_name: &str,
        expected_count: usize,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        println!(
            "Verifying pagination consistency for bucket: {}",
            bucket_name
        );
        println!("  Expected object count: {}", expected_count);

        let objects = client.list_all_objects(bucket_name).await?;
        let actual_count = objects.len();

        println!("  Actual object count: {}", actual_count);

        let is_consistent = actual_count == expected_count;
        if is_consistent {
            println!(
                "✓ Pagination consistency verified: {} objects",
                actual_count
            );
        } else {
            println!(
                "✗ Pagination consistency failed: expected {}, found {}",
                expected_count, actual_count
            );
        }

        Ok(is_consistent)
    }

    /// Verify that bucket creation was successful
    pub async fn verify_bucket_exists(client: &S3TestClient, bucket_name: &str) -> bool {
        println!("Verifying bucket exists: {}", bucket_name);
        let exists = client.bucket_exists(bucket_name).await;
        if exists {
            println!("✓ Bucket {} exists", bucket_name);
        } else {
            println!("✗ Bucket {} does not exist", bucket_name);
        }
        exists
    }
}
