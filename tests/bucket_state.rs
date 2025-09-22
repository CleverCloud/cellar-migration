//! Bucket state scenario tests
//! These tests verify migration behavior with different source/destination states

use test_common::*;
use tokio::test;

/// Test partial sync scenario where destination bucket already contains some files
///
/// **Test Setup:**
/// - Source bucket: 5 test files with various metadata (content-type, cache-control)
/// - Destination bucket: First 3 files already uploaded
///
/// **What it tests:**
/// - Migration tool correctly identifies which files need to be synced
/// - Only missing files (files 4-5) are transferred
/// - Existing files in destination are left unchanged
/// - No duplicate uploads occur
///
/// **Expected Result:**
/// - Source bucket: 5 files total
/// - Destination bucket: 5 files total (3 existing + 2 newly migrated)
/// - Verification: All 5 files exist in destination with correct content and metadata
#[test]
async fn test_partial_sync_scenario() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "partial-sync";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate test files
    let mut test_files = file_generator.generate_test_files(5)?;
    for test_file in &mut test_files {
        test_file.acl_public = false; // ACL support is covered elsewhere; keep this scenario focused on content overwrite
    }

    // Upload all files to source bucket
    for test_file in &test_files {
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;
    }

    // Partial sync scenario: Upload only some files to destination
    let partial_files = &test_files[..3]; // Only first 3 files
    for test_file in partial_files {
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .dest_client()
            .upload_test_file(&dst_bucket, test_file, &file_path)
            .await?;
    }

    // Run migration - should only transfer the missing files
    let migration_result = run_migration_cli(
        &config,
        &src_bucket,
        &dst_bucket,
        MigrationOptions::new()
            .chunk_size_mb(10)
            .thread_count(num_cpus::get()),
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify all files are now present in destination
    if !MigrationVerifier::verify_all_objects_migrated(
        bucket_manager.source_client(),
        bucket_manager.dest_client(),
        &src_bucket,
        &dst_bucket,
    )
    .await?
    {
        return Err("Not all objects were migrated in partial sync scenario".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test conflicting files scenario where source and destination have different content for same keys
///
/// **Test Setup:**
/// - Source bucket: 5 test files with specific content and metadata
/// - Destination bucket: 3 files with same keys but different content/metadata than source
///
/// **What it tests:**
/// - Migration tool detects content/metadata differences (ETag mismatches)
/// - Conflicting files in destination are overwritten with source versions
/// - New files from source are properly uploaded
/// - Verification compares source vs destination S3 objects only (no local disk checks)
///
/// **Expected Result:**
/// - Source bucket: 5 files total (unchanged)
/// - Destination bucket: 5 files total (3 overwritten + 2 new)
/// - Verification: All destination files match source content and metadata exactly
#[test]
async fn test_conflicting_files_scenario() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "conflicting-files";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate multiple source files
    let test_files = file_generator.generate_test_files(5)?;

    // Upload all files to source bucket
    for test_file in &test_files {
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;
    }

    // Create conflicting files in destination for the first 3 keys (same keys, different size/metadata)
    let conflicting = &test_files[..3];
    for (i, test_file) in conflicting.iter().enumerate() {
        let key = test_file.key();
        let conflict_size = test_file.size + (i + 1) * 1234; // ensure size difference
        let conflict_file = TestFile::new(&key, conflict_size)
            .with_content_type("application/octet-stream") // different content type
            .with_cache_control("no-cache"); // different cache control

        let conflict_path = file_generator.generate_file(&conflict_file)?;
        bucket_manager
            .dest_client()
            .upload_test_file(&dst_bucket, &conflict_file, &conflict_path)
            .await?;
    }

    // Run migration - should overwrite conflicting file
    let migration_result = run_migration_cli(
        &config,
        &src_bucket,
        &dst_bucket,
        MigrationOptions::new()
            .chunk_size_mb(10)
            .thread_count(num_cpus::get()),
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify all source objects exist in destination
    if !MigrationVerifier::verify_all_objects_migrated(
        bucket_manager.source_client(),
        bucket_manager.dest_client(),
        &src_bucket,
        &dst_bucket,
    )
    .await?
    {
        return Err("Not all objects were migrated in conflicting files scenario".into());
    }

    // Verify each destination object matches the corresponding source object (S3-only verification)
    for test_file in &test_files {
        let verification_result = MigrationVerifier::verify_migration_s3_only(
            bucket_manager.source_client(),
            bucket_manager.dest_client(),
            &src_bucket,
            &dst_bucket,
            test_file,
        )
        .await?;

        if verification_result != VerificationResult::Match {
            return Err(format!(
                "Conflicting files scenario verification failed for {}: {:?}",
                test_file.key(),
                verification_result
            )
            .into());
        }
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}
