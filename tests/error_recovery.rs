//! Error recovery and resilience tests
//! These tests verify migration tool behavior under various error conditions

use std::process::{Command, Stdio};
use test_common::*;
use tokio::test;

/// Test proper error handling when destination credentials are invalid
///
/// **Test Setup:**
/// - Source bucket: Provisioned with a single test file using valid credentials
/// - Destination bucket: Created with valid configuration but CLI invoked with fake destination keys
/// - Migration invoked with the invalid destination credentials
///
/// **What it tests:**
/// - CLI surfaces authentication failures as a non-success exit status
/// - Migration stops without attempting to write data with bad credentials
///
/// **Expected Result:**
/// - Migration command returns a non-zero exit code signaling authentication failure
#[test]
async fn test_invalid_credentials_handling() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "invalid-credentials";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager with valid credentials
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate test file
    let test_file = TestFile::new("test-file.txt", 5000).with_content_type("text/plain");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source with valid credentials
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Run migration with invalid destination credentials
    let migration_result = run_migration_with_invalid_credentials_cli(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    // Should fail gracefully with authentication error
    if migration_result.success() {
        return Err("Migration should have failed with invalid credentials".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test error handling when source bucket does not exist
///
/// **Test Setup:**
/// - Destination bucket: Created normally for the test run
/// - Source bucket: Randomly generated name that is guaranteed to be absent
///
/// **What it tests:**
/// - CLI detects a missing source bucket and fails cleanly
///
/// **Expected Result:**
/// - Migration command exits with a non-zero status because the source bucket is missing
#[test]
async fn test_nonexistent_source_bucket() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "nonexistent-source";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create only destination bucket
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (_, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    let nonexistent_src_bucket = format!(
        "nonexistent-bucket-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Run migration with nonexistent source bucket
    let migration_result = run_basic_migration(
        &config,
        &nonexistent_src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    // Should fail gracefully with bucket not found error
    if migration_result.success() {
        return Err("Migration should have failed with nonexistent source bucket".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test automatic destination bucket creation when destination bucket is missing
///
/// **Test Setup:**
/// - Source bucket: Created with a single file ready for migration
/// - Destination bucket: Name chosen to ensure it does not exist before the run
///
/// **What it tests:**
/// - CLI creates the destination bucket on demand and continues the migration
/// - Migrated files land in the newly created bucket
///
/// **Expected Result:**
/// - Migration command succeeds
/// - Destination bucket exists afterward and contains the migrated file
#[test]
async fn test_bucket_creation_on_missing_destination() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "bucket-creation";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create only source bucket
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, _) = bucket_manager.create_test_buckets(test_name).await?;

    // Generate test file
    let test_file = TestFile::new("test-file.txt", 5000).with_content_type("text/plain");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Create a unique destination bucket name that doesn't exist yet
    let nonexistent_dst_bucket = format!(
        "auto-created-bucket-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Run migration - should auto-create destination bucket
    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &nonexistent_dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Migration with bucket auto-creation failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify destination bucket was created and file was migrated
    let dest_client = S3TestClient::new_destination(config.clone()).await?;
    if !dest_client.bucket_exists(&nonexistent_dst_bucket).await {
        return Err("Destination bucket was not auto-created".into());
    }

    // Verify file was migrated
    let dest_objects = dest_client
        .list_all_objects(&nonexistent_dst_bucket)
        .await?;
    if dest_objects.len() != 1 {
        return Err("File was not migrated to auto-created bucket".into());
    }

    // Manual cleanup of auto-created bucket
    let _ = dest_client.delete_bucket(&nonexistent_dst_bucket).await;

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test proper handling of empty source buckets
///
/// **Test Setup:**
/// - Source bucket: Created without uploading any files
/// - Destination bucket: Empty initially
///
/// **What it tests:**
/// - CLI completes successfully when there are no objects to migrate
/// - Destination bucket remains empty after the run
///
/// **Expected Result:**
/// - Migration command succeeds
/// - Destination still contains zero objects at the end of the test
#[test]
async fn test_empty_bucket_handling() -> Result<(), Box<dyn std::error::Error>> {
    // Validate environment variables first
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "empty-bucket";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    // Don't upload any files - test empty bucket migration

    // Run migration on empty source bucket
    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    if !migration_result.success() {
        return Err(format!(
            "Empty bucket migration failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify destination bucket is also empty
    let dest_objects = bucket_manager
        .dest_client()
        .list_all_objects(&dst_bucket)
        .await?;

    if !dest_objects.is_empty() {
        return Err("Destination bucket should be empty after migrating empty source".into());
    }

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Run migration CLI with invalid credentials
async fn run_migration_with_invalid_credentials_cli(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
) -> Result<std::process::ExitStatus, Box<dyn std::error::Error>> {
    let binary_path = get_binary_path("cellar-migration")
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    let mut cmd = Command::new(&binary_path);
    cmd.args([
        "migrate",
        "--source-access-key",
        &config.src_access_key,
        "--source-secret-key",
        &config.src_secret_key,
        "--source-bucket",
        src_bucket,
        "--source-endpoint",
        &config.src_endpoint,
        "--source-provider",
        "cellar",
        "--destination-access-key",
        "invalid-access-key",
        "--destination-secret-key",
        "invalid-secret-key",
        "--destination-bucket",
        dst_bucket,
        "--destination-endpoint",
        &config.dst_endpoint,
        "--multipart-chunk-size-mb",
        &chunk_size_mb.to_string(),
        "--threads",
        &thread_count.to_string(),
        "--execute",
    ]);

    // Capture stdout and stderr from child process
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let output = cmd.output()?;

    // Print captured output to test's stdout/stderr
    // This will be subject to --nocapture behavior
    // Note: This test expects failure, so we still print output for debugging
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));

    Ok(output.status)
}
