//! Single file migration integration tests
//! These tests verify basic migration functionality with various configurations

use chrono::{Duration, Timelike, Utc};
use test_common::*;
use tokio::test;

/// Baseline smoke test for single-object migrations
///
/// **Test Setup:**
/// - Source bucket: One 5 MB file with simple metadata
/// - Destination bucket: Empty initially
///
/// **What it tests:**
/// - CLI succeeds with default configuration
/// - File contents and metadata match post-migration
///
/// **Expected Result:**
/// - Migration finishes successfully
/// - Verification confirms parity between source and destination
#[test]
async fn test_basic_single_object_copy() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = format!("single-file-{}mb-{}t-{}b", 5, num_cpus::get(), 5_000_000);
    let file_generator = FileGenerator::new_for_test(&test_name)?;

    run_single_file_migration(
        &config,
        &file_generator,
        5,               // chunk size MB
        num_cpus::get(), // thread count
        5_000_000,       // 5 MB
    )
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

async fn run_single_file_migration(
    config: &TestConfig,
    file_generator: &FileGenerator,
    chunk_size_mb: usize,
    thread_count: usize,
    file_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let test_name = format!(
        "single-file-{}mb-{}t-{}b",
        chunk_size_mb, thread_count, file_size
    );

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(&test_name).await?;

    // Generate test file
    let test_file = TestFile::new("test-single-file.txt", file_size)
        .with_content_type("text/plain")
        .with_cache_control("max-age=3600");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source bucket
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Run migration using the CLI
    let migration_result = run_basic_migration(
        config,
        &src_bucket,
        &dst_bucket,
        chunk_size_mb,
        thread_count,
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Create new clients for verification to avoid stale connection issues
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Verify migration results (S3-only: compare source vs destination, ignore local disk)
    let verification_result = MigrationVerifier::verify_migration_s3_only(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &test_file,
    )
    .await?;

    if verification_result != VerificationResult::Match {
        return Err(format!("Migration verification failed: {:?}", verification_result).into());
    }

    // Verify bucket existence and object count
    if !MigrationVerifier::verify_bucket_exists(&verification_dest_client, &dst_bucket).await {
        return Err("Destination bucket does not exist after migration".into());
    }

    if !MigrationVerifier::verify_all_objects_migrated(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
    )
    .await?
    {
        return Err("Not all objects were migrated successfully".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;

    Ok(())
}

async fn run_single_file_metadata_case<F>(
    config: &TestConfig,
    file_generator: &FileGenerator,
    test_name: &str,
    configure: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(TestFile) -> TestFile,
{
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    let mut test_file = configure(TestFile::new("metadata-object.txt", 16_384));
    let file_path = file_generator.generate_file(&test_file)?;

    if test_file.expected_md5.is_none() {
        let checksum = file_generator.compute_md5(&file_path)?;
        test_file.set_expected_md5(checksum);
    }

    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    let migration_result =
        run_basic_migration(config, &src_bucket, &dst_bucket, 10, num_cpus::get()).await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    let verification_result = MigrationVerifier::verify_migration_s3_only(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &test_file,
    )
    .await?;

    if verification_result != VerificationResult::Match {
        return Err(format!(
            "Metadata verification failed for {}: {:?}",
            test_file.key(),
            verification_result
        )
        .into());
    }

    bucket_manager.cleanup().await?;

    Ok(())
}

#[test]
async fn test_preserves_content_type() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-content-type";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_content_type("application/json")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_cache_control() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-cache-control";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_cache_control("max-age=3600, must-revalidate")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_content_disposition() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-content-disposition";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_content_disposition("attachment; filename=\"data.txt\"")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_content_encoding() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-content-encoding";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_content_encoding("gzip")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_content_language() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-content-language";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_content_language("fr-FR")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_expires_header() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-expires";
    let file_generator = FileGenerator::new_for_test(test_name)?;
    let expires_at = (Utc::now() + Duration::hours(6))
        .with_nanosecond(0)
        .unwrap();

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_expires(expires_at)
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_user_metadata() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-user-tags";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    run_single_file_metadata_case(&config, &file_generator, test_name, |file| {
        file.with_user_metadata("feature", "cellar-migration")
            .with_user_metadata("env", "integration-test")
    })
    .await?;

    file_generator.cleanup()?;
    Ok(())
}

#[test]
async fn test_preserves_acl() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "metadata-acl";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    let test_file = TestFile::new("acl-object.txt", 8_192)
        .with_content_type("text/plain")
        .with_public_acl();

    let file_path = file_generator.generate_file(&test_file)?;
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    let migration_result =
        run_basic_migration(&config, &src_bucket, &dst_bucket, 10, num_cpus::get()).await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    let verification_result = MigrationVerifier::verify_migration_s3_only(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &test_file,
    )
    .await?;

    assert_eq!(
        VerificationResult::Match,
        verification_result,
        "ACL propagation should preserve public-read flag"
    );

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test multipart upload behavior with a single configuration
///
/// **Test Setup:**
/// - Source bucket: Single 15 MB file that requires multipart uploads when chunked
/// - Destination bucket: Empty initially
/// - Migration executed with 5 MB chunks, producing a three-part upload
///
/// **What it tests:**
/// - CLI successfully uploads a multipart object with the configured chunk size
/// - Resulting destination object matches the source content and metadata despite multipart ETags
///
/// **Expected Result:**
/// - Migration command succeeds
/// - Verification confirms the multipart upload matches the source object
#[test]
async fn test_multipart_upload_behavior() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "multipart-behavior";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Test multipart behavior by using small chunk size with larger file
    // This forces multipart upload behavior without needing huge files
    let test_file = TestFile::new("multipart-test.bin", 15_000_000) // 15MB file
        .with_content_type("application/octet-stream");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source bucket
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Run migration with small chunk size to force multipart
    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks (file is 15MB, so 3 parts)
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Create new clients for verification to avoid stale connection issues
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Verify migration using S3-only verification (ETags may differ due to multipart)
    let verification_result = MigrationVerifier::verify_migration_s3_only(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &test_file,
    )
    .await?;

    if verification_result != VerificationResult::Match {
        return Err(format!(
            "Multipart migration verification failed: {:?}",
            verification_result
        )
        .into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}
