//! Creation date preservation integration tests
//! These tests verify that the --preserve-last-modified-timestamps flag correctly preserves object timestamps

// Fun fact: In Ceph, if an object gets written in an RGW S3 bucket and that write is close to the end of a second (say 10:00:01.950ms)
// then the LastModified fields between the ListObjectV2 and HeadObject calls might have one second difference
// That's because the ListObjectV2 operation returns the LastModified metadata entry stored in the bucket index
// while the LastModified entry for the HeadObject operation is retrieved by reading the mtime of the underlying rados object for the S3 object
// So when the S3 object gets uploaded, the bucket index entry is first created with the current second (01 here) but, because of latency and all that,
// the rados object might only be created a few milliseconds later so its mtime will now be in the `10:00:02` second

use chrono::{DateTime, Timelike, Utc};
use test_common::*;
use tokio::test;

/// Comprehensive helper function to verify timestamps match across all S3 operations
/// This function checks:
/// 1. ListObjectsV2 source vs ListObjectsV2 destination timestamps match
/// 2. ListObjectsV2 source vs HeadObject destination timestamps match
///
/// Why this approach:
/// - Source bucket might have differences between ListObjectsV2 and HeadObject (up to 1 second in Ceph)
/// - Destination bucket will have consistent timestamps because we force them during migration
/// - By comparing source ListObjectsV2 with both dest ListObjectsV2 and dest HeadObject,
///   we verify the timestamp was correctly preserved in both operations
async fn assert_timestamps_match_comprehensive(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    object_key: &str,
    version_id: Option<&str>,
    context: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get destination HeadObject timestamp
    let dest_head = if let Some(vid) = version_id {
        dest_client
            .get_object_metadata_version(dst_bucket, object_key, vid)
            .await?
    } else {
        dest_client
            .get_object_metadata(dst_bucket, object_key)
            .await?
    };

    let dest_head_timestamp = dest_head
        .last_modified()
        .ok_or("Destination HeadObject should have LastModified timestamp")?;

    let dest_head_dt = DateTime::parse_from_rfc3339(
        &dest_head_timestamp.fmt(aws_smithy_types::date_time::Format::DateTime)?,
    )?
    .with_timezone(&Utc);

    // Get List operation timestamps
    if version_id.is_none() {
        // For non-versioned objects, use ListObjectsV2
        let source_list_objects = source_client.list_all_objects(src_bucket).await?;
        let dest_list_objects = dest_client.list_all_objects(dst_bucket).await?;

        let source_object = source_list_objects
            .iter()
            .find(|obj| obj.key().map(|k| k == object_key).unwrap_or(false))
            .ok_or_else(|| format!("Object {} not found in source ListObjectsV2", object_key))?;

        let dest_object = dest_list_objects
            .iter()
            .find(|obj| obj.key().map(|k| k == object_key).unwrap_or(false))
            .ok_or_else(|| {
                format!(
                    "Object {} not found in destination ListObjectsV2",
                    object_key
                )
            })?;

        let source_list_timestamp = source_object
            .last_modified()
            .ok_or("Source ListObjectsV2 object should have LastModified timestamp")?;
        let dest_list_timestamp = dest_object
            .last_modified()
            .ok_or("Destination ListObjectsV2 object should have LastModified timestamp")?;

        let source_list_dt = DateTime::parse_from_rfc3339(
            &source_list_timestamp.fmt(aws_smithy_types::date_time::Format::DateTime)?,
        )?
        .with_timezone(&Utc);

        let dest_list_dt = DateTime::parse_from_rfc3339(
            &dest_list_timestamp.fmt(aws_smithy_types::date_time::Format::DateTime)?,
        )?
        .with_timezone(&Utc);

        // Check 1: ListObjectsV2 source vs ListObjectsV2 destination
        if source_list_dt != dest_list_dt {
            return Err(format!(
                "{} - ListObjectsV2: Destination LastModified ({}) does not match source LastModified ({})",
                context, dest_list_dt, source_list_dt
            )
            .into());
        }
        println!(
            "{} - ✓ ListObjectsV2 src vs dst timestamps match: {}",
            context, source_list_dt
        );

        // Check 2: ListObjectsV2 source vs HeadObject destination
        // Note: HeadObject doesn't include milliseconds, so we truncate milliseconds from ListObjectsV2
        let source_list_dt_truncated = source_list_dt
            .with_nanosecond(0)
            .ok_or("Failed to truncate nanoseconds")?;
        let dest_head_dt_truncated = dest_head_dt
            .with_nanosecond(0)
            .ok_or("Failed to truncate nanoseconds")?;

        if source_list_dt_truncated != dest_head_dt_truncated {
            return Err(format!(
                "{} - ListObjectsV2 src vs HeadObject dst: Destination HeadObject ({}) does not match source ListObjectsV2 ({})",
                context, dest_head_dt_truncated, source_list_dt_truncated
            )
            .into());
        }
        println!(
            "{} - ✓ ListObjectsV2 src vs HeadObject dst timestamps match (seconds): {}",
            context, source_list_dt_truncated
        );
    } else {
        // For versioned objects, use ListObjectVersions
        let source_versions = source_client
            .list_object_versions(src_bucket, Some(object_key))
            .await?;
        let dest_versions = dest_client
            .list_object_versions(dst_bucket, Some(object_key))
            .await?;

        let source_version = source_versions
            .iter()
            .find(|v| {
                v.version_id()
                    .map(|vid| vid == version_id.unwrap())
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                format!(
                    "Version {} of {} not found in source ListObjectVersions",
                    version_id.unwrap(),
                    object_key
                )
            })?;

        let dest_version = dest_versions
            .iter()
            .find(|v| {
                v.version_id()
                    .map(|vid| vid == version_id.unwrap())
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                format!(
                    "Version {} of {} not found in destination ListObjectVersions",
                    version_id.unwrap(),
                    object_key
                )
            })?;

        let source_version_timestamp = source_version
            .last_modified()
            .ok_or("Source ListObjectVersions version should have LastModified timestamp")?;
        let dest_version_timestamp = dest_version
            .last_modified()
            .ok_or("Destination ListObjectVersions version should have LastModified timestamp")?;

        let source_version_dt = DateTime::parse_from_rfc3339(
            &source_version_timestamp.fmt(aws_smithy_types::date_time::Format::DateTime)?,
        )?
        .with_timezone(&Utc);

        let dest_version_dt = DateTime::parse_from_rfc3339(
            &dest_version_timestamp.fmt(aws_smithy_types::date_time::Format::DateTime)?,
        )?
        .with_timezone(&Utc);

        // Check 1: ListObjectVersions source vs ListObjectVersions destination
        if source_version_dt != dest_version_dt {
            return Err(format!(
                "{} - ListObjectVersions: Destination LastModified ({}) does not match source LastModified ({})",
                context, dest_version_dt, source_version_dt
            )
            .into());
        }
        println!(
            "{} - ✓ ListObjectVersions src vs dst timestamps match: {}",
            context, source_version_dt
        );

        // Check 2: ListObjectVersions source vs HeadObject destination
        // Note: HeadObject doesn't include milliseconds, so we truncate milliseconds from ListObjectVersions
        let source_version_dt_truncated = source_version_dt
            .with_nanosecond(0)
            .ok_or("Failed to truncate nanoseconds")?;
        let dest_head_dt_truncated = dest_head_dt
            .with_nanosecond(0)
            .ok_or("Failed to truncate nanoseconds")?;

        if source_version_dt_truncated != dest_head_dt_truncated {
            return Err(format!(
                "{} - ListObjectVersions src vs HeadObject dst: Destination HeadObject ({}) does not match source ListObjectVersions ({})",
                context, dest_head_dt_truncated, source_version_dt_truncated
            )
            .into());
        }
        println!(
            "{} - ✓ ListObjectVersions src vs HeadObject dst timestamps match (seconds): {}",
            context, source_version_dt_truncated
        );
    }

    println!("{} - ✓ All timestamp checks passed", context);
    Ok(())
}

/// Test single-part upload with creation date preservation
///
/// **Test Setup:**
/// - Source bucket: One 5 MB file uploaded with a specific timestamp
/// - Destination bucket: Empty initially
/// - Migration executed with --preserve-last-modified-timestamps flag
///
/// **What it tests:**
/// - CLI successfully migrates with the --preserve-last-modified-timestamps flag
/// - Destination object's LastModified matches source object's LastModified
/// - The timestamp is preserved and not set to migration time
///
/// **Expected Result:**
/// - Migration finishes successfully
/// - Destination object has the same LastModified as source object
#[test]
async fn test_single_part_creation_date_preservation() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "creation-date-single-part";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    println!("[{}] Setting up test file", test_name);

    // Generate test file (5MB - well below multipart threshold)
    let test_file =
        TestFile::new("test-creation-date.txt", 5_000_000).with_content_type("text/plain");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source bucket
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Wait a few seconds to ensure migration time is different from upload time
    println!(
        "[{}] Waiting 3 seconds to ensure migration timestamp differs from upload timestamp",
        test_name
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Run migration WITH --preserve-last-modified-timestamps flag
    println!(
        "[{}] Running migration with --preserve-last-modified-timestamps",
        test_name
    );
    let (first_run, second_run) = run_basic_migration_with_flags(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks (won't trigger multipart for 5MB file)
        num_cpus::get(), // Use all available CPUs
        false,           // preserve_version_ids
        true,            // preserve_last_modified_timestamps
    )
    .await?;

    if !first_run.success() {
        return Err(format!(
            "First migration run failed with exit code: {}",
            first_run.code().unwrap_or(-1)
        )
        .into());
    }

    if !second_run.success() {
        return Err(format!(
            "Second migration run failed with exit code: {}",
            second_run.code().unwrap_or(-1)
        )
        .into());
    }

    // Verify timestamps match across HeadObject, ListObjectsV2, and between source/destination
    assert_timestamps_match_comprehensive(
        bucket_manager.source_client(),
        bucket_manager.dest_client(),
        &src_bucket,
        &dst_bucket,
        &test_file.key(),
        None, // Not a versioned object
        test_name,
    )
    .await?;

    // Verify idempotency
    assert_eq!(
        second_run.files_to_sync,
        Some(0),
        "Second migration run should sync 0 files (idempotency check), but got: {:?}",
        second_run.files_to_sync
    );

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test multipart upload with creation date preservation
///
/// **Test Setup:**
/// - Source bucket: One 15 MB file that forces multipart upload
/// - Destination bucket: Empty initially
/// - Migration executed with --preserve-last-modified-timestamps flag and 5MB chunks
///
/// **What it tests:**
/// - CLI successfully migrates multipart objects with --preserve-last-modified-timestamps
/// - Destination object's LastModified matches source object's LastModified
/// - The timestamp is preserved even for multipart uploads
///
/// **Expected Result:**
/// - Migration finishes successfully
/// - Destination object has the same LastModified as source object
#[test]
async fn test_multipart_creation_date_preservation() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "creation-date-multipart";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager.create_test_buckets(test_name).await?;

    println!("[{}] Setting up test file for multipart upload", test_name);

    // Generate test file (15MB - forces multipart with 5MB chunks)
    let test_file = TestFile::new("test-multipart-creation-date.bin", 15_000_000)
        .with_content_type("application/octet-stream");

    let file_path = file_generator.generate_file(&test_file)?;

    // Upload file to source bucket
    bucket_manager
        .source_client()
        .upload_test_file(&src_bucket, &test_file, &file_path)
        .await?;

    // Wait a few seconds to ensure migration time is different from upload time
    println!(
        "[{}] Waiting 3 seconds to ensure migration timestamp differs from upload timestamp",
        test_name
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Run migration WITH --preserve-last-modified-timestamps flag and small chunk size to force multipart
    println!(
        "[{}] Running migration with --preserve-last-modified-timestamps (5MB chunks)",
        test_name
    );
    let (first_run, second_run) = run_basic_migration_with_flags(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks (forces multipart for 15MB file)
        num_cpus::get(), // Use all available CPUs
        false,           // preserve_version_ids
        true,            // preserve_last_modified_timestamps
    )
    .await?;

    if !first_run.success() {
        return Err(format!(
            "First migration run failed with exit code: {}\nStderr: {}",
            first_run.code().unwrap_or(-1),
            first_run.stderr
        )
        .into());
    }

    if !second_run.success() {
        return Err(format!(
            "Second migration run failed with exit code: {}\nStderr: {}",
            second_run.code().unwrap_or(-1),
            second_run.stderr
        )
        .into());
    }

    // Verify timestamps match across HeadObject, ListObjectsV2, and between source/destination
    assert_timestamps_match_comprehensive(
        bucket_manager.source_client(),
        bucket_manager.dest_client(),
        &src_bucket,
        &dst_bucket,
        &test_file.key(),
        None, // Not a versioned object
        test_name,
    )
    .await?;

    // Verify idempotency
    assert_eq!(
        second_run.files_to_sync,
        Some(0),
        "Second migration run should sync 0 files (idempotency check), but got: {:?}",
        second_run.files_to_sync
    );

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Test versioned objects with creation date preservation
///
/// **Test Setup:**
/// - Source bucket: Versioned bucket with 1 object having 3 versions
/// - Each version uploaded at different times with distinct timestamps
/// - Destination bucket: Versioned, empty initially
/// - Migration executed with --preserve-last-modified-timestamps flag
///
/// **What it tests:**
/// - CLI successfully migrates versioned objects with --preserve-last-modified-timestamps
/// - Each version's LastModified timestamp is preserved independently
/// - The timestamp preservation works correctly across all versions
///
/// **Expected Result:**
/// - Migration finishes successfully
/// - All versions preserve their original LastModified timestamps
#[test]
async fn test_versioned_creation_date_preservation() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "creation-date-versioned";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create versioned test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    println!("[{}] Setting up versioned test objects", test_name);

    let object_key = "test-versioned-creation-date.txt";
    let mut version_ids = Vec::new();
    let mut version_timestamps = Vec::new();

    // Create 3 versions with delays between them to ensure different timestamps
    for version_num in 1..=3 {
        let version_content = format!("Version {} content", version_num);
        let content = format!(
            "{}\n{}",
            version_content,
            "x".repeat(500_000 - version_content.len() - 1)
        );

        let test_file = TestFile::new(&format!("{}-v{}", object_key, version_num), content.len());

        let file_path =
            file_generator.generate_file_with_content(&test_file, content.as_bytes())?;

        // Upload version
        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned(&src_bucket, object_key, &file_path)
            .await?;

        println!(
            "[{}] Created version {} with ID: {}",
            test_name, version_num, version_id
        );

        version_ids.push(version_id.clone());

        // Get the timestamp for this version
        let metadata = bucket_manager
            .source_client()
            .get_object_metadata_version(&src_bucket, object_key, &version_id)
            .await?;

        let timestamp = metadata
            .last_modified()
            .ok_or("Version should have LastModified timestamp")?
            .to_owned();

        version_timestamps.push((version_id, timestamp));

        // Wait 2 seconds between versions to ensure distinct timestamps
        if version_num < 3 {
            println!(
                "[{}] Waiting 2 seconds before creating next version",
                test_name
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }

    println!(
        "[{}] Created {} versions, waiting 3 seconds before migration",
        test_name,
        version_ids.len()
    );
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Run migration WITH --preserve-last-modified-timestamps flag
    println!(
        "[{}] Running migration with --preserve-last-modified-timestamps for versioned objects",
        test_name
    );
    let (first_run, second_run) = run_basic_migration_with_flags(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks
        num_cpus::get(), // Use all available CPUs
        true,            // preserve_version_ids (required for versioned objects)
        true,            // preserve_last_modified_timestamps
    )
    .await?;

    if !first_run.success() {
        return Err(format!(
            "First migration run failed with exit code: {}\nStderr: {}",
            first_run.code().unwrap_or(-1),
            first_run.stderr
        )
        .into());
    }

    if !second_run.success() {
        return Err(format!(
            "Second migration run failed with exit code: {}\nStderr: {}",
            second_run.code().unwrap_or(-1),
            second_run.stderr
        )
        .into());
    }

    // Verify each version's timestamp is preserved
    println!(
        "[{}] Verifying timestamps for all {} versions",
        test_name,
        version_timestamps.len()
    );

    for (version_id, _) in &version_timestamps {
        // Verify timestamps match across HeadObject, ListObjectVersions, and between source/destination
        let context = format!("[{}] Version {}", test_name, version_id);
        assert_timestamps_match_comprehensive(
            bucket_manager.source_client(),
            bucket_manager.dest_client(),
            &src_bucket,
            &dst_bucket,
            object_key,
            Some(version_id),
            &context,
        )
        .await?;
    }

    println!(
        "[{}] ✓ All {} version timestamps preserved successfully",
        test_name,
        version_timestamps.len()
    );

    // Verify idempotency
    assert_eq!(
        second_run.files_to_sync,
        Some(0),
        "Second migration run should sync 0 files (idempotency check), but got: {:?}",
        second_run.files_to_sync
    );

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}
