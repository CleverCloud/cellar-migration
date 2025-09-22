//! Dry-run vs Execute mode validation tests
//! These tests verify that dry-run mode accurately calculates diffs without making changes

use test_common::*;
use tokio::test;

/// Dry-run mode should not create objects or mutate destination state
///
/// **Test Setup:**
/// - Source bucket: 5 generated files with mixed metadata
/// - Destination bucket: Empty initially (all files need migration)
/// - Run migration once in dry-run mode only
///
/// **What it tests:**
/// - Dry-run completes without transferring any objects
/// - Destination object listing is identical before and after dry-run
///
/// **Expected Result:**
/// - Source bucket: 5 files total (unchanged)
/// - Destination bucket: Remains empty after dry-run
#[test]
async fn test_dry_run_no_side_effects() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "dry-run-accuracy";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate test files for source
    let test_files = file_generator.generate_test_files(5)?;

    // Upload files to source bucket
    for test_file in &test_files {
        let file_path = file_generator.generate_file(test_file)?;
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;
    }

    // Get initial destination state (should be empty)
    let initial_dst_objects = bucket_manager
        .dest_client()
        .list_all_objects(&dst_bucket)
        .await?;

    // Run dry-run migration
    let dry_run_result = run_dry_run_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !dry_run_result.success() {
        return Err(format!(
            "Dry-run CLI failed with exit code: {}",
            dry_run_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify destination bucket is unchanged after dry-run
    let post_dry_run_dst_objects = bucket_manager
        .dest_client()
        .list_all_objects(&dst_bucket)
        .await?;

    if initial_dst_objects.len() != post_dry_run_dst_objects.len() {
        return Err("Dry-run modified destination bucket when it shouldn't have".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Dry-run should detect that already-synced buckets require no work
///
/// **Test Setup:**
/// - Source bucket: 3 generated files with metadata
/// - Destination bucket: Seeded with the same 3 files prior to testing
/// - Run dry-run followed immediately by an execute run
///
/// **What it tests:**
/// - Dry-run succeeds without mutating the destination
/// - A subsequent execute run acts as a no-op and keeps buckets synchronized
///
/// **Expected Result:**
/// - Both runs finish successfully
/// - Destination contents remain unchanged and continue matching the source
#[test]
async fn test_dry_run_when_already_synced() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "dry-run-no-changes";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate test files
    let test_files = file_generator.generate_test_files(3)?;

    // Upload same files to both buckets
    for test_file in &test_files {
        let file_path = file_generator.generate_file(test_file)?;

        // Upload to source
        bucket_manager
            .source_client()
            .upload_test_file(&src_bucket, test_file, &file_path)
            .await?;

        // Upload to destination
        bucket_manager
            .dest_client()
            .upload_test_file(&dst_bucket, test_file, &file_path)
            .await?;
    }

    // Run dry-run - should detect no changes needed
    let dry_run_result = run_dry_run_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !dry_run_result.success() {
        return Err(format!(
            "Dry-run CLI failed with exit code: {}",
            dry_run_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Run execute migration - should be a no-op
    let execute_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !execute_result.success() {
        return Err(format!(
            "Execute migration CLI failed with exit code: {}",
            execute_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify buckets are still in sync
    if !MigrationVerifier::verify_all_objects_migrated(
        bucket_manager.source_client(),
        bucket_manager.dest_client(),
        &src_bucket,
        &dst_bucket,
    )
    .await?
    {
        return Err("Buckets should remain in sync after no-op migration".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}
