//! Multiple files migration integration tests
//! These tests verify batch migration functionality and pagination behavior

use chrono::{Duration, Utc};
use futures::future::join_all;
use test_common::*;
use tokio::test;

/// End-to-end scenario covering multiple feature interactions in one run
///
/// **Test Setup:**
/// - Source bucket: Mixed bag of objects (metadata-rich, multipart-sized, zero-byte, unicode key)
/// - Destination bucket: Empty initially
///
/// **What it tests:**
/// - CLI migrates heterogeneous workloads in one execution
/// - Verifies content, metadata, ACL, multipart scenarios, and key preservation together
///
/// **Expected Result:**
/// - Migration succeeds
/// - Destination matches source for every object
#[test]
async fn test_end_to_end_migration_scenario() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "end-to-end";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Assemble fixtures explicitly so the scenario is easy to reason about.
    let mut test_files = Vec::new();

    // Metadata-rich JSON object with ACL.
    let expires_at = Utc::now() + Duration::hours(2);
    let mut metadata_file = TestFile::new("fixtures/metadata.json", 25_000)
        .with_content_type("application/json")
        .with_cache_control("max-age=86400, public")
        .with_content_disposition("attachment; filename=\"metadata.json\"")
        .with_content_language("en-US")
        .with_content_encoding("gzip")
        .with_expires(expires_at)
        .with_public_acl()
        .with_user_metadata("feature", "metadata");
    let metadata_path = file_generator.generate_file(&metadata_file)?;
    let metadata_md5 = file_generator.compute_md5(&metadata_path)?;
    metadata_file.set_expected_md5(metadata_md5);
    test_files.push(metadata_file);

    // Multipart-sized binary blob (forces multipart at 5 MB chunks).
    test_files.push(
        TestFile::new("fixtures/multipart.bin", 15_000_000)
            .with_content_type("application/octet-stream"),
    );

    // Zero-byte sentinel file to cover empty object handling.
    test_files.push(TestFile::new("fixtures/empty.txt", 0).with_content_type("text/plain"));

    // Unicode key to ensure complex names survive the run.
    test_files
        .push(TestFile::new("fixtures/🎉-unicodé.txt", 4_096).with_content_type("text/plain"));

    // Generate and upload each file.
    for test_file in &test_files {
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;
    }

    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5, // 5 MB chunks to trigger multipart for the large file
        num_cpus::get(),
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    if !MigrationVerifier::verify_all_objects_migrated(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
    )
    .await?
    {
        return Err("Not all objects were migrated in end-to-end scenario".into());
    }

    for test_file in &test_files {
        let verification_result = MigrationVerifier::verify_migration_s3_only(
            &verification_source_client,
            &verification_dest_client,
            &src_bucket,
            &dst_bucket,
            test_file,
        )
        .await?;

        if verification_result != VerificationResult::Match {
            return Err(format!(
                "End-to-end verification failed for {}: {:?}",
                test_file.key(),
                verification_result
            )
            .into());
        }
    }

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Pagination regression test that scales based on optional stress flag
///
/// **Test Setup:**
/// - Source bucket: Many 1 KB objects; default 2 000 but can be increased via env var
/// - Destination bucket: Empty initially
/// - Migration executed with small `max-keys` to force continuation tokens
///
/// **What it tests:**
/// - S3 listing pagination and continuation handling
/// - Completeness of object transfer under pagination pressure
///
/// **Expected Result:**
/// - Migration succeeds
/// - Destination object count matches source and passes verification
#[test]
async fn test_pagination_handles_large_list() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "pagination-regression";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let file_count = 2_000;
    let max_keys = 100;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    let test_files = file_generator.generate_pagination_files(file_count)?;

    const BATCH_SIZE: usize = 100;
    for (batch_num, batch) in test_files.chunks(BATCH_SIZE).enumerate() {
        let upload_tasks = batch.iter().map(|test_file| {
            let file_generator = &file_generator;
            let bucket_manager = &bucket_manager;
            let src_bucket = &src_bucket;
            async move {
                let file_path = file_generator.generate_file(test_file)?;
                bucket_manager
                    .source_client()
                    .upload_test_file(src_bucket, test_file, &file_path)
                    .await
            }
        });

        let results = join_all(upload_tasks).await;
        for (i, result) in results.into_iter().enumerate() {
            result.map_err(|e| {
                format!(
                    "Failed to upload file {} in batch {}: {}",
                    batch_num * BATCH_SIZE + i,
                    batch_num + 1,
                    e
                )
            })?;
        }
    }

    if !MigrationVerifier::verify_pagination_consistency(
        bucket_manager.source_client(),
        &src_bucket,
        file_count,
    )
    .await?
    {
        return Err("Source bucket does not have the expected number of files".into());
    }

    let migration_result = run_migration_with_max_keys(
        &config,
        &src_bucket,
        &dst_bucket,
        10,
        num_cpus::get(),
        max_keys,
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Pagination regression migration failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    if !MigrationVerifier::verify_pagination_consistency(
        &verification_dest_client,
        &dst_bucket,
        file_count,
    )
    .await?
    {
        return Err("Destination bucket does not have the expected number of files".into());
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

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test file migration with Unicode and special characters in filenames
///
/// **Test Setup:**
/// - Source bucket: Files whose keys include Unicode (`测试.txt`, `café.json`, `файл.bin`, `🎉emoji.txt`, `ñoño-español.data`) and special characters (`file with spaces.txt`, `file-with-dashes.txt`, `file_with_underscores.txt`, `file.with.dots.txt`, `file[with]brackets.txt`)
/// - Destination bucket: Empty initially
///
/// **What it tests:**
/// - CLI preserves complex key names during upload and verification
/// - Unicode and special character keys remain accessible after migration
///
/// **Expected Result:**
/// - Migration succeeds
/// - Destination contains every file with an exact key match and identical content
#[test]
async fn test_unicode_and_special_character_files() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "unicode-special-chars";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    println!("=== Test Setup: Unicode and Special Character Files ===");

    // Create test bucket manager
    println!("Creating test buckets...");
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;
    println!("✓ Created buckets: {} -> {}", src_bucket, dst_bucket);

    // Generate files with unicode names
    let mut test_files = file_generator.generate_unicode_files()?;
    // Add files with special characters
    test_files.extend(file_generator.generate_special_char_files()?);

    println!("Total files to process: {}", test_files.len());

    // Upload all files to source bucket
    println!("Uploading files with Unicode and special characters...");
    for (i, test_file) in test_files.iter().enumerate() {
        println!(
            "Uploading file {}/{}: '{}'",
            i + 1,
            test_files.len(),
            test_file.key()
        );
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;
    }
    println!("✓ All {} files uploaded successfully", test_files.len());

    println!("\n=== Running Migration ===");
    // Run migration
    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks
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
    println!("✓ Migration completed successfully");

    println!("\n=== Verification Phase ===");
    // Create new clients for verification to avoid stale connection issues
    println!("Creating verification clients...");
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;
    println!("✓ Verification clients created");

    // Verify all files were migrated correctly (S3-only verification)
    println!("Verifying Unicode and special character files...");
    for (i, test_file) in test_files.iter().enumerate() {
        println!(
            "Verifying file {}/{}: '{}'",
            i + 1,
            test_files.len(),
            test_file.key()
        );
        let verification_result = MigrationVerifier::verify_migration_s3_only(
            &verification_source_client,
            &verification_dest_client,
            &src_bucket,
            &dst_bucket,
            test_file,
        )
        .await?;

        if verification_result != VerificationResult::Match {
            return Err(format!(
                "Unicode/special char file {} verification failed: {:?}",
                test_file.key(),
                verification_result
            )
            .into());
        }
    }
    println!(
        "✓ All {} Unicode/special character files verified successfully",
        test_files.len()
    );

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}
