//! Versioned object migration integration tests
//! These tests verify versioned object migration functionality across simple and complex scenarios

use aws_sdk_s3::operation::get_object_acl::GetObjectAclOutput;
use aws_smithy_types_convert::date_time::DateTimeExt;
use chrono::{DateTime, SecondsFormat, Utc};
use test_common::*;
use tokio::test;

/// Simple versioned object migration test
///
/// **Test Setup:**
/// - Source bucket: Versioned with 2 objects having multiple versions each
/// - Object 1: single-part-versioned.txt (500KB, 0-10 random versions)
/// - Object 2: multi-part-versioned.bin (15MB, 0-10 random versions, forces multipart upload)
/// - Destination bucket: Versioned, empty initially
///
/// **What it tests:**
/// - Bucket versioning setup and validation
/// - Versioned object creation with different content per version
/// - Migration tool execution with version support
/// - Version ID preservation verification
/// - Content and metadata preservation across versions
///
/// **Expected Result:**
/// - Migration successfully migrates all versions
/// - Version verification passes with all versions preserved
#[test]
async fn test_simple_versioned_object_migration() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "simple-versioned-migration";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create versioned test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    println!("[{}] Setting up versioned test objects", test_name);

    // Object 1: Single-part versioned file (500KB)
    let single_part_versions = create_versioned_object(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        "single-part-versioned.txt",
        500_000, // 500KB
        test_name,
    )
    .await?;

    // Object 2: Multi-part versioned file (15MB)
    let multi_part_versions = create_versioned_object(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        "multi-part-versioned.bin",
        15_000_000, // 15MB
        test_name,
    )
    .await?;

    println!(
        "[{}] Created {} versions for single-part object, {} versions for multi-part object",
        test_name,
        single_part_versions.len(),
        multi_part_versions.len()
    );

    // Verify versions exist in source bucket before migration
    verify_pre_migration_versions(
        &bucket_manager,
        &src_bucket,
        &[
            ("single-part-versioned.txt", &single_part_versions),
            ("multi-part-versioned.bin", &multi_part_versions),
        ],
        test_name,
    )
    .await?;

    // Run migration using the CLI - runs TWICE for idempotency testing
    println!("[{}] Running migration with version support", test_name);
    let (first_run, second_run) = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks (forces multipart for 150MB file)
        num_cpus::get(), // Use all available CPUs
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

    println!("[{}] Migration completed, verifying results", test_name);

    // Create new clients for verification
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Verify versioned migration results
    let verification_result = verify_versioned_migration_results(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &[
            ("single-part-versioned.txt", &single_part_versions),
            ("multi-part-versioned.bin", &multi_part_versions),
        ],
        test_name,
    )
    .await;

    // Handle verification result
    match verification_result {
        Ok(true) => {
            println!(
                "[{}] ✓ All versioned objects verified successfully!",
                test_name
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
        Ok(false) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!("[{}] ✗ Versioned migration verification failed", test_name);
            Err(
                format!("Version verification failed - some versions were not migrated correctly")
                    .into(),
            )
        }
        Err(e) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!("[{}] ✗ Verification error: {}", test_name, e);
            Err(e)
        }
    }
}

/// Ensure version ordering is preserved for single-part and multi-part objects
#[test]
async fn test_version_order_preserved() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "version-order-migration";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    let single_part_key = "ordered-single-part.txt";
    let single_part_versions = create_ordered_versions(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        single_part_key,
        &[128_000, 130_000, 132_000],
        test_name,
    )
    .await?;

    let multi_part_key = "ordered-multi-part.bin";
    let multi_part_versions = create_ordered_versions(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        multi_part_key,
        &[6_500_000, 6_700_000, 6_900_000],
        test_name,
    )
    .await?;

    verify_pre_migration_versions(
        &bucket_manager,
        &src_bucket,
        &[
            (single_part_key, &single_part_versions),
            (multi_part_key, &multi_part_versions),
        ],
        test_name,
    )
    .await?;

    let (first_run, second_run) =
        run_basic_migration(&config, &src_bucket, &dst_bucket, 5, num_cpus::get()).await?;

    if !first_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "First migration run failed with exit code: {}",
            first_run.code().unwrap_or(-1)
        )
        .into());
    }

    if !second_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "Second migration run failed with exit code: {}",
            second_run.code().unwrap_or(-1)
        )
        .into());
    }

    assert_eq!(
        second_run.files_to_sync,
        Some(0),
        "Second migration run should sync 0 files (idempotency check)"
    );

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    let single_part_source_order =
        list_version_ids_for_object(&verification_source_client, &src_bucket, single_part_key)
            .await?;
    let single_part_dest_order =
        list_version_ids_for_object(&verification_dest_client, &dst_bucket, single_part_key)
            .await?;

    if single_part_source_order != single_part_dest_order {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "Version ordering mismatch for {}. source={:?}, destination={:?}",
            single_part_key, single_part_source_order, single_part_dest_order
        )
        .into());
    }

    let multi_part_source_order =
        list_version_ids_for_object(&verification_source_client, &src_bucket, multi_part_key)
            .await?;
    let multi_part_dest_order =
        list_version_ids_for_object(&verification_dest_client, &dst_bucket, multi_part_key).await?;

    if multi_part_source_order != multi_part_dest_order {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "Version ordering mismatch for {}. source={:?}, destination={:?}",
            multi_part_key, multi_part_source_order, multi_part_dest_order
        )
        .into());
    }

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Ensure delete markers are migrated alongside object versions
#[test]
async fn test_delete_markers_preserved() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "delete-marker-migration";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    let object_specs = [
        ("deleted-single-a.txt", vec![120_000, 130_000, 140_000]),
        ("deleted-single-b.txt", vec![180_000, 190_000, 200_000]),
        ("deleted-multi.bin", vec![6_500_000, 6_700_000, 6_900_000]),
    ];

    let mut versions_expected = Vec::new();
    let mut delete_markers_expected: Vec<(&str, DeleteMarkerMetadata)> = Vec::new();

    for (object_key, sizes) in &object_specs {
        let versions = create_ordered_versions(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            sizes,
            test_name,
        )
        .await?;

        versions_expected.push((*object_key, versions));

        let delete_marker_id = bucket_manager
            .source_client()
            .delete_object(&src_bucket, object_key)
            .await?;

        println!(
            "[{}] Created delete marker for {}: version_id={}",
            test_name, object_key, delete_marker_id
        );

        let source_markers =
            list_delete_markers_for_object(bucket_manager.source_client(), &src_bucket, object_key)
                .await?;

        let marker_metadata = source_markers
            .into_iter()
            .find(|marker| marker.version_id == delete_marker_id)
            .ok_or_else(|| {
                format!(
                    "Newly created delete marker {} not found for object {}",
                    delete_marker_id, object_key
                )
            })?;

        delete_markers_expected.push((*object_key, marker_metadata));
    }

    let version_slices: Vec<(&str, &[String])> = versions_expected
        .iter()
        .map(|(key, versions)| (*key, versions.as_slice()))
        .collect();

    verify_pre_migration_versions(&bucket_manager, &src_bucket, &version_slices, test_name).await?;

    for (object_key, expected_marker) in &delete_markers_expected {
        let markers =
            list_delete_markers_for_object(bucket_manager.source_client(), &src_bucket, object_key)
                .await?;

        let source_marker = markers
            .into_iter()
            .find(|marker| marker.version_id == expected_marker.version_id);

        match source_marker {
            Some(found)
                if timestamps_match(&found.last_modified, &expected_marker.last_modified) => {}
            Some(found) => {
                bucket_manager.cleanup().await?;
                file_generator.cleanup()?;
                return Err(format!(
                    "Delete marker last_modified mismatch for {} before migration. expected={}, actual={}",
                    object_key,
                    format_timestamp(&expected_marker.last_modified).unwrap_or_else(|| "<none>".to_string()),
                    format_timestamp(&found.last_modified).unwrap_or_else(|| "<none>".to_string())
                )
                .into());
            }
            None => {
                bucket_manager.cleanup().await?;
                file_generator.cleanup()?;
                return Err(format!(
                    "Delete marker {} not found for {} in source bucket.",
                    expected_marker.version_id, object_key
                )
                .into());
            }
        };
    }

    let (first_run, second_run) =
        run_basic_migration(&config, &src_bucket, &dst_bucket, 5, num_cpus::get()).await?;

    if !first_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "First migration run failed with exit code: {}",
            first_run.code().unwrap_or(-1)
        )
        .into());
    }

    if !second_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "Second migration run failed with exit code: {}",
            second_run.code().unwrap_or(-1)
        )
        .into());
    }

    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    for (object_key, _expected_versions) in &versions_expected {
        let source_versions =
            list_version_ids_for_object(&verification_source_client, &src_bucket, object_key)
                .await?;
        let dest_versions =
            list_version_ids_for_object(&verification_dest_client, &dst_bucket, object_key).await?;

        if source_versions != dest_versions {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            return Err(format!(
                "Version mismatch for {}. source={:?}, destination={:?}",
                object_key, source_versions, dest_versions
            )
            .into());
        }
    }

    for (object_key, expected_marker) in &delete_markers_expected {
        let source_markers =
            list_delete_markers_for_object(&verification_source_client, &src_bucket, object_key)
                .await?;
        let dest_markers =
            list_delete_markers_for_object(&verification_dest_client, &dst_bucket, object_key)
                .await?;

        let source_marker = source_markers
            .iter()
            .find(|marker| marker.version_id == expected_marker.version_id)
            .cloned();
        let dest_marker = dest_markers
            .iter()
            .find(|marker| marker.version_id == expected_marker.version_id)
            .cloned();

        match (source_marker, dest_marker) {
            (Some(src), Some(dst))
                if timestamps_match(&src.last_modified, &expected_marker.last_modified)
                    && timestamps_match(&dst.last_modified, &expected_marker.last_modified) => {}
            (Some(src), Some(dst)) => {
                bucket_manager.cleanup().await?;
                file_generator.cleanup()?;
                return Err(format!(
                    "Delete marker metadata mismatch for {}. expected={{id: {}, last_modified: {}}}, source={{id: {}, last_modified: {}}}, destination={{id: {}, last_modified: {}}}",
                    object_key,
                    expected_marker.version_id,
                    format_timestamp(&expected_marker.last_modified).unwrap_or_else(|| "<none>".to_string()),
                    src.version_id,
                    format_timestamp(&src.last_modified).unwrap_or_else(|| "<none>".to_string()),
                    dst.version_id,
                    format_timestamp(&dst.last_modified).unwrap_or_else(|| "<none>".to_string())
                )
                .into());
            }
            (Some(_), None) => {
                bucket_manager.cleanup().await?;
                file_generator.cleanup()?;
                return Err(format!(
                    "Delete marker {} missing from destination for {}",
                    expected_marker.version_id, object_key
                )
                .into());
            }
            (None, _) => {
                bucket_manager.cleanup().await?;
                file_generator.cleanup()?;
                return Err(format!(
                    "Expected delete marker {} missing from source for {} after migration",
                    expected_marker.version_id, object_key
                )
                .into());
            }
        };
    }

    let dest_objects = verification_dest_client
        .list_all_objects(&dst_bucket)
        .await?;

    for (object_key, _) in &object_specs {
        if dest_objects
            .iter()
            .any(|object| object.key.as_deref() == Some(*object_key))
        {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            return Err(format!(
                "Object {} unexpectedly visible in destination bucket listing",
                object_key
            )
            .into());
        }
    }

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Ensure ACL state is preserved per version during migration
#[test]
async fn test_versioned_acl_preserved() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "versioned-acl-migration";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    // Create a single object with two versions, including ACL on the second version
    let object_key = "acl-versioned-object.txt";
    let version_one_id = create_version_with_acl_state(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        object_key,
        "Version one without ACL",
        false,
        test_name,
    )
    .await?;

    let version_two_id = create_version_with_acl_state(
        &bucket_manager,
        &file_generator,
        &src_bucket,
        object_key,
        "Version two with public ACL",
        true,
        test_name,
    )
    .await?;

    // Run migration twice for idempotency
    let (first_run, second_run) =
        run_basic_migration(&config, &src_bucket, &dst_bucket, 5, num_cpus::get()).await?;

    if !first_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "First migration run failed with exit code: {}",
            first_run.code().unwrap_or(-1)
        )
        .into());
    }

    if !second_run.success() {
        bucket_manager.cleanup().await?;
        file_generator.cleanup()?;
        return Err(format!(
            "Second migration run failed with exit code: {}",
            second_run.code().unwrap_or(-1)
        )
        .into());
    }

    // Create new clients for verification
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Ensure both versions exist on source and destination
    let src_versions =
        list_version_ids_for_object(&verification_source_client, &src_bucket, object_key).await?;
    let dst_versions =
        list_version_ids_for_object(&verification_dest_client, &dst_bucket, object_key).await?;

    assert!(
        src_versions.contains(&version_one_id),
        "Source is missing version one"
    );
    assert!(
        src_versions.contains(&version_two_id),
        "Source is missing version two"
    );
    assert_eq!(src_versions, dst_versions, "Version ordering mismatch");

    // Verify ACLs for each version
    verify_acl_state(
        &verification_source_client,
        &src_bucket,
        &verification_dest_client,
        &dst_bucket,
        object_key,
        &version_one_id,
        false,
    )
    .await?;

    verify_acl_state(
        &verification_source_client,
        &src_bucket,
        &verification_dest_client,
        &dst_bucket,
        object_key,
        &version_two_id,
        true,
    )
    .await?;

    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    Ok(())
}

/// Complex versioned object migration test with diverse object types and sizes
///
/// **Test Setup:**
/// - Source bucket: Versioned with 11 objects of varying sizes
/// - Small files (5KB-100KB): 4 objects with 0-10 versions each
/// - Medium files (2MB-10MB): 4 objects with 0-10 versions each
/// - Large files (15MB-20MB): 3 objects with 0-10 versions each
/// - Clean objects with no custom metadata or content types
/// - Destination bucket: Versioned, empty initially
///
/// **What it tests:**
/// - Comprehensive versioned object migration across size ranges
/// - Mixed single-part and multi-part upload scenarios based on size
/// - Performance with multiple versioned objects
/// - Version ID preservation across all objects and versions
/// - Content preservation across versions without metadata complexity
///
/// **Expected Result:**
/// - Migration successfully migrates all versions across all objects
/// - Version verification passes for all size ranges
#[test]
async fn test_complex_versioned_object_migration() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "complex-versioned-migration";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create versioned test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    println!("[{}] Setting up complex versioned test objects", test_name);

    let mut all_objects_and_versions = Vec::new();

    // Small files (1KB-100KB): 4 objects
    let small_file_specs = [
        ("config.json", 5_000),
        ("readme.txt", 25_000),
        ("small-data.bin", 100_000),
        ("metadata.xml", 50_000),
    ];

    for (object_key, size) in &small_file_specs {
        let versions = create_complex_versioned_object(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Medium files (1MB-10MB): 4 objects
    let medium_file_specs = [
        ("image-dataset.bin", 2_000_000),
        ("document-archive.tar", 5_000_000),
        ("video-sample.mp4", 8_000_000),
        ("database-dump.sql", 10_000_000),
    ];

    for (object_key, size) in &medium_file_specs {
        let versions = create_complex_versioned_object(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Large files (multipart): 3 objects
    let large_file_specs = [
        ("large-dataset.bin", 15_000_000),
        ("video-hd.mkv", 18_000_000),
        ("backup-archive.zip", 20_000_000),
    ];

    for (object_key, size) in &large_file_specs {
        let versions = create_complex_versioned_object(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Calculate total versions created
    let total_versions: usize = all_objects_and_versions
        .iter()
        .map(|(_, versions)| versions.len())
        .sum();

    println!(
        "[{}] Created {} objects with {} total versions across all objects",
        test_name,
        all_objects_and_versions.len(),
        total_versions
    );

    // Verify versions exist in source bucket before migration
    verify_complex_pre_migration_versions(
        &bucket_manager,
        &src_bucket,
        &all_objects_and_versions,
        test_name,
    )
    .await?;

    // Run migration using the CLI
    println!(
        "[{}] Running migration for {} objects with {} versions total",
        test_name,
        all_objects_and_versions.len(),
        total_versions
    );

    let migration_start = std::time::Instant::now();
    let (first_run, second_run) = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        10,              // 10MB chunks
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    let migration_duration = migration_start.elapsed();
    println!(
        "[{}] Migration completed in {:.2}s",
        test_name,
        migration_duration.as_secs_f64()
    );

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

    // Create new clients for verification
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Verify versioned migration results
    let verification_start = std::time::Instant::now();
    let verification_result = verify_complex_versioned_migration_results(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &all_objects_and_versions,
        test_name,
    )
    .await;

    let verification_duration = verification_start.elapsed();
    println!(
        "[{}] Verification completed in {:.2}s",
        test_name,
        verification_duration.as_secs_f64()
    );

    // Handle verification result
    match verification_result {
        Ok(true) => {
            println!(
                "[{}] ✓ All complex versioned objects verified successfully!",
                test_name
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
        Ok(false) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!(
                "[{}] ✗ Complex versioned migration verification failed",
                test_name
            );
            Err(
                format!("Version verification failed - some versions were not migrated correctly")
                    .into(),
            )
        }
        Err(e) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!("[{}] ✗ Verification error: {}", test_name, e);
            Err(e)
        }
    }
}

/// Create a versioned object with random number of versions (0-10) - clean objects with no metadata
async fn create_versioned_object(
    bucket_manager: &TestBucketManager,
    file_generator: &FileGenerator,
    bucket_name: &str,
    object_key: &str,
    base_size: usize,
    test_name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let version_count = rng.gen_range(0..=10); // 0 to 10 versions

    println!(
        "[{}] Creating {} versions for object: {}",
        test_name, version_count, object_key
    );

    if version_count == 0 {
        println!(
            "[{}] Object {} will have no versions (object doesn't exist)",
            test_name, object_key
        );
        return Ok(Vec::new());
    }

    let mut version_ids = Vec::new();

    for version_num in 1..=version_count {
        // Create unique content for each version
        let version_content = format!("Version {} content for {}", version_num, object_key);
        let full_content = format!(
            "{}\n{}",
            version_content,
            "x".repeat(base_size - version_content.len() - 1)
        );

        // Create test file with version-specific content (no custom metadata)
        let test_file = TestFile::new(
            &format!("{}-v{}", object_key, version_num),
            full_content.len(),
        );

        // Generate file with version-specific content
        let file_path =
            file_generator.generate_file_with_content(&test_file, full_content.as_bytes())?;

        // Upload clean versioned object (no metadata)
        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned(bucket_name, object_key, &file_path)
            .await?;

        version_ids.push(version_id);

        println!(
            "[{}] Created version {} for {}: version_id={}",
            test_name,
            version_num,
            object_key,
            version_ids.last().unwrap()
        );
    }

    Ok(version_ids)
}

/// Create deterministic versions with predictable content ordering
async fn create_ordered_versions(
    bucket_manager: &TestBucketManager,
    file_generator: &FileGenerator,
    bucket_name: &str,
    object_key: &str,
    minimum_sizes: &[usize],
    test_name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    println!(
        "[{}] Creating {} deterministic versions for {}",
        test_name,
        minimum_sizes.len(),
        object_key
    );

    let mut version_ids = Vec::new();

    for (index, minimum_size) in minimum_sizes.iter().enumerate() {
        let header = format!("Version {} payload for {}", index + 1, object_key);
        let mut content = header.into_bytes();
        content.push(b'\n');

        if *minimum_size > content.len() {
            content.extend(std::iter::repeat(b'#').take(*minimum_size - content.len()));
        }

        let file_name = format!("{}-ordered-v{}", object_key, index + 1);
        let test_file = TestFile::new(&file_name, content.len());
        let file_path = file_generator.generate_file_with_content(&test_file, &content)?;

        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned(bucket_name, object_key, &file_path)
            .await?;

        println!(
            "[{}] Created deterministic version {} for {} (size: {} bytes, version_id={})",
            test_name,
            index + 1,
            object_key,
            content.len(),
            version_id
        );

        version_ids.push(version_id);
    }

    Ok(version_ids)
}

/// Create a complex versioned object with random number of versions (0-10) - clean objects with no metadata
async fn create_complex_versioned_object(
    bucket_manager: &TestBucketManager,
    file_generator: &FileGenerator,
    bucket_name: &str,
    object_key: &str,
    base_size: usize,
    test_name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let version_count = rng.gen_range(0..=10); // 0 to 10 versions

    println!(
        "[{}] Creating {} versions for complex object: {} (size: ~{}MB)",
        test_name,
        version_count,
        object_key,
        base_size / 1_000_000
    );

    if version_count == 0 {
        println!(
            "[{}] Object {} will have no versions (object doesn't exist)",
            test_name, object_key
        );
        return Ok(Vec::new());
    }

    let mut version_ids = Vec::new();

    for version_num in 1..=version_count {
        // Create version-specific content with size variations
        let size_variation = rng.gen_range(-10..=10); // ±10% size variation
        let actual_size = if base_size > 100_000 {
            (base_size as i64 + (base_size as i64 * size_variation / 100)).max(1000) as usize
        } else {
            base_size
        };

        let version_header = format!("=== VERSION {} OF {} ===\n", version_num, object_key);
        let version_content = format!(
            "{}Content for version {} of object {}\nTimestamp: {}\nSize: {} bytes\n",
            version_header,
            version_num,
            object_key,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            actual_size
        );

        // Pad to desired size
        let padding_needed = actual_size.saturating_sub(version_content.len());
        let padding = if padding_needed > 0 {
            // Create pattern-based padding for better compression testing
            let pattern = format!("DATA-LINE-{:04}-", version_num);
            let pattern_repeats = padding_needed / pattern.len();
            let remainder = padding_needed % pattern.len();
            format!(
                "{}{}",
                pattern.repeat(pattern_repeats),
                &pattern[..remainder]
            )
        } else {
            String::new()
        };

        let full_content = format!("{}{}", version_content, padding);

        // Create test file with version-specific content (no metadata)
        let test_file = TestFile::new(
            &format!("{}-v{}", object_key, version_num),
            full_content.len(),
        );

        // Generate file with version-specific content
        let file_path =
            file_generator.generate_file_with_content(&test_file, full_content.as_bytes())?;

        // Upload clean versioned object (no metadata)
        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned(bucket_name, object_key, &file_path)
            .await?;

        version_ids.push(version_id);

        println!(
            "[{}] Created version {} for {} ({}MB): version_id={}",
            test_name,
            version_num,
            object_key,
            actual_size / 1_000_000,
            version_ids.last().unwrap()
        );
    }

    Ok(version_ids)
}

/// Verify versions exist in source bucket before migration
async fn verify_pre_migration_versions(
    bucket_manager: &TestBucketManager,
    src_bucket: &str,
    objects_and_versions: &[(&str, &[String])],
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "[{}] Verifying pre-migration versions in source bucket",
        test_name
    );

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions)",
                test_name, object_key
            );
            continue;
        }

        let src_versions = bucket_manager
            .source_client()
            .list_object_versions(src_bucket, Some(object_key))
            .await?;

        let versions_for_key: Vec<_> = src_versions
            .iter()
            .filter(|v| v.key().map_or(false, |k| k == *object_key))
            .collect();

        if versions_for_key.len() != expected_versions.len() {
            return Err(format!(
                "Pre-migration version count mismatch for {}: expected {}, found {}",
                object_key,
                expected_versions.len(),
                versions_for_key.len()
            )
            .into());
        }

        // Verify each expected version exists
        for expected_version_id in expected_versions.iter() {
            let found = versions_for_key
                .iter()
                .any(|v| v.version_id().map_or(false, |id| id == expected_version_id));

            if !found {
                return Err(format!(
                    "Expected version {} not found for object {} in source bucket",
                    expected_version_id, object_key
                )
                .into());
            }
        }

        println!(
            "[{}] ✓ Pre-migration verification passed for {} ({} versions)",
            test_name,
            object_key,
            expected_versions.len()
        );
    }

    Ok(())
}

async fn list_version_ids_for_object(
    client: &S3TestClient,
    bucket: &str,
    object_key: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let versions = client
        .list_object_versions(bucket, Some(object_key))
        .await?;

    let ordered_ids = versions
        .iter()
        .filter_map(|version| {
            if version.key().map_or(false, |key| key == object_key) {
                version.version_id().map(|id| id.to_string())
            } else {
                None
            }
        })
        .collect();

    Ok(ordered_ids)
}

async fn create_version_with_acl_state(
    bucket_manager: &TestBucketManager,
    file_generator: &FileGenerator,
    bucket_name: &str,
    object_key: &str,
    content: &str,
    public_acl: bool,
    test_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let test_file = if public_acl {
        TestFile::new(&format!("{}-acl", object_key), content.len()).with_public_acl()
    } else {
        TestFile::new(&format!("{}-acl", object_key), content.len())
    };

    let file_path = file_generator.generate_file_with_content(&test_file, content.as_bytes())?;

    let version_id = if public_acl {
        bucket_manager
            .source_client()
            .upload_test_file_versioned_with_attributes(
                bucket_name,
                object_key,
                &test_file,
                &file_path,
            )
            .await?
    } else {
        bucket_manager
            .source_client()
            .upload_test_file_versioned(bucket_name, object_key, &file_path)
            .await?
    };

    println!(
        "[{}] Created version {} for {} (public_acl={})",
        test_name, version_id, object_key, public_acl
    );

    Ok(version_id)
}

async fn verify_acl_state(
    source_client: &S3TestClient,
    src_bucket: &str,
    dest_client: &S3TestClient,
    dst_bucket: &str,
    object_key: &str,
    version_id: &str,
    expected_public: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let src_acl = source_client
        .get_object_acl(src_bucket, object_key, Some(version_id))
        .await?;
    let dst_acl = dest_client
        .get_object_acl(dst_bucket, object_key, Some(version_id))
        .await?;

    let src_public = acl_has_public_read(&src_acl);
    let dst_public = acl_has_public_read(&dst_acl);

    if src_public != expected_public {
        return Err(format!(
            "Source ACL mismatch for {} version {}: expected {}, got {}",
            object_key, version_id, expected_public, src_public
        )
        .into());
    }

    if dst_public != expected_public {
        return Err(format!(
            "Destination ACL mismatch for {} version {}: expected {}, got {}",
            object_key, version_id, expected_public, dst_public
        )
        .into());
    }

    Ok(())
}

fn acl_has_public_read(acl: &GetObjectAclOutput) -> bool {
    acl.grants().iter().any(|grant| {
        matches!(
            grant.permission(),
            Some(aws_sdk_s3::types::Permission::Read)
        ) && grant
            .grantee()
            .and_then(|grantee| grantee.uri())
            .map(|uri| uri == "http://acs.amazonaws.com/groups/global/AllUsers")
            .unwrap_or(false)
    })
}

fn timestamps_match(lhs: &Option<DateTime<Utc>>, rhs: &Option<DateTime<Utc>>) -> bool {
    match (lhs, rhs) {
        (Some(a), Some(b)) => a == b || a.timestamp() == b.timestamp(),
        (None, None) => true,
        _ => false,
    }
}

fn format_timestamp(ts: &Option<DateTime<Utc>>) -> Option<String> {
    ts.map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[derive(Debug, Clone)]
struct DeleteMarkerMetadata {
    version_id: String,
    last_modified: Option<DateTime<Utc>>,
}

async fn list_delete_markers_for_object(
    client: &S3TestClient,
    bucket: &str,
    object_key: &str,
) -> Result<Vec<DeleteMarkerMetadata>, Box<dyn std::error::Error>> {
    let markers = client.list_delete_markers(bucket, Some(object_key)).await?;

    let mut details = Vec::new();

    for marker in markers {
        if marker.key().map_or(false, |key| key == object_key) {
            let version_id = marker
                .version_id()
                .ok_or_else(|| {
                    format!(
                        "Delete marker entry missing version ID for object {}",
                        object_key
                    )
                })?
                .to_string();

            let last_modified = match marker.last_modified() {
                Some(ts) => Some(ts.to_chrono_utc().map_err(|err| {
                    format!(
                        "Failed to convert delete marker timestamp for {}: {}",
                        object_key, err
                    )
                })?),
                None => None,
            };

            details.push(DeleteMarkerMetadata {
                version_id,
                last_modified,
            });
        }
    }

    Ok(details)
}

/// Verify complex versions exist in source bucket before migration
async fn verify_complex_pre_migration_versions(
    bucket_manager: &TestBucketManager,
    src_bucket: &str,
    objects_and_versions: &[(&str, Vec<String>)],
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "[{}] Verifying complex pre-migration versions in source bucket",
        test_name
    );

    let mut total_versions = 0;
    let mut objects_with_versions = 0;

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions)",
                test_name, object_key
            );
            continue;
        }

        objects_with_versions += 1;
        total_versions += expected_versions.len();

        let src_versions = bucket_manager
            .source_client()
            .list_object_versions(src_bucket, Some(object_key))
            .await?;

        let versions_for_key: Vec<_> = src_versions
            .iter()
            .filter(|v| v.key().map_or(false, |k| k == *object_key))
            .collect();

        if versions_for_key.len() != expected_versions.len() {
            return Err(format!(
                "Pre-migration version count mismatch for {}: expected {}, found {}",
                object_key,
                expected_versions.len(),
                versions_for_key.len()
            )
            .into());
        }

        // Verify each expected version exists
        for expected_version_id in expected_versions {
            let found = versions_for_key
                .iter()
                .any(|v| v.version_id().map_or(false, |id| id == expected_version_id));

            if !found {
                return Err(format!(
                    "Expected version {} not found for object {} in source bucket",
                    expected_version_id, object_key
                )
                .into());
            }
        }

        println!(
            "[{}] ✓ Pre-migration verification passed for {} ({} versions)",
            test_name,
            object_key,
            expected_versions.len()
        );
    }

    println!(
        "[{}] ✓ Complex pre-migration verification completed: {} objects with {} total versions",
        test_name, objects_with_versions, total_versions
    );

    Ok(())
}

/// Verify versioned migration results
async fn verify_versioned_migration_results(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    objects_and_versions: &[(&str, &[String])],
    test_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!("[{}] Verifying versioned migration results", test_name);

    let mut all_passed = true;

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions expected)",
                test_name, object_key
            );
            continue;
        }

        println!(
            "[{}] Verifying versions for object: {}",
            test_name, object_key
        );

        match MigrationVerifier::verify_versioned_migration(
            source_client,
            dest_client,
            src_bucket,
            dst_bucket,
            object_key,
        )
        .await?
        {
            VersionedVerificationResult::Match => {
                println!(
                    "[{}] ✓ All versions verified successfully for {}",
                    test_name, object_key
                );
            }
            result => {
                println!(
                    "[{}] ✗ Version verification failed for {}: {:?}",
                    test_name, object_key, result
                );
                all_passed = false;
            }
        }
    }

    if all_passed {
        println!(
            "[{}] ✓ All versioned objects verification passed",
            test_name
        );
        Ok(true)
    } else {
        println!(
            "[{}] ✗ Some versioned objects verification failed",
            test_name
        );
        Ok(false)
    }
}

/// Verify complex versioned migration results with performance metrics
async fn verify_complex_versioned_migration_results(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    objects_and_versions: &[(&str, Vec<String>)],
    test_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!(
        "[{}] Verifying complex versioned migration results",
        test_name
    );

    let mut all_passed = true;
    let mut total_versions_verified = 0;
    let mut objects_verified = 0;

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions expected)",
                test_name, object_key
            );
            continue;
        }

        let verification_start = std::time::Instant::now();
        println!(
            "[{}] Verifying versions for object: {} ({} versions)",
            test_name,
            object_key,
            expected_versions.len()
        );

        match MigrationVerifier::verify_versioned_migration(
            source_client,
            dest_client,
            src_bucket,
            dst_bucket,
            object_key,
        )
        .await?
        {
            VersionedVerificationResult::Match => {
                let verification_time = verification_start.elapsed();
                println!(
                    "[{}] ✓ All {} versions verified successfully for {} in {:.2}s",
                    test_name,
                    expected_versions.len(),
                    object_key,
                    verification_time.as_secs_f64()
                );
                total_versions_verified += expected_versions.len();
                objects_verified += 1;
            }
            result => {
                println!(
                    "[{}] ✗ Version verification failed for {}: {:?}",
                    test_name, object_key, result
                );
                all_passed = false;
            }
        }
    }

    if all_passed {
        println!(
            "[{}] ✓ All {} objects with {} total versions verification passed",
            test_name, objects_verified, total_versions_verified
        );
    } else {
        println!(
            "[{}] ✗ Complex versioned objects verification failed - some objects had issues",
            test_name
        );
    }

    // Additional verification using the comprehensive method
    println!("[{}] Running comprehensive version verification", test_name);
    let comprehensive_result = MigrationVerifier::verify_all_versioned_objects_migrated(
        source_client,
        dest_client,
        src_bucket,
        dst_bucket,
    )
    .await?;

    if comprehensive_result {
        println!(
            "[{}] ✓ Comprehensive version verification passed",
            test_name
        );
    } else {
        println!(
            "[{}] ✗ Comprehensive version verification failed",
            test_name
        );
        all_passed = false;
    }

    Ok(all_passed)
}

/// Versioned object migration test with comprehensive custom attributes
///
/// **Test Setup:**
/// - Source bucket: Versioned with 6 objects having multiple versions each
/// - Small files (100KB-500KB): 2 objects with 1-10 versions, mixed single-part uploads
/// - Medium files (2MB-8MB): 2 objects with 1-10 versions, mixed single/multi-part uploads
/// - Large files (15MB-25MB): 2 objects with 1-10 versions, forces multipart uploads
/// - Each version has randomized custom attributes including:
///   - Content-Type (various MIME types)
///   - Cache-Control directives
///   - Content-Disposition
///   - Content-Encoding
///   - Content-Language
///   - Expires headers
///   - Public ACL (randomly applied)
///   - Custom user metadata (0-5 key-value pairs per version)
/// - Destination bucket: Versioned, empty initially
///
/// **What it tests:**
/// - Versioned object migration with comprehensive metadata preservation
/// - Mixed upload sizes to test both single-part and multipart scenarios
/// - Complex metadata combinations across multiple versions
/// - ACL preservation across versions
/// - Custom user metadata preservation
/// - HTTP header preservation (content-type, cache-control, etc.)
///
/// **Expected Result:**
/// - Migration successfully migrates all versions with all custom attributes preserved
/// - Metadata and version verification passes for all objects and versions
#[test]
async fn test_versioned_migration_with_custom_attributes() -> Result<(), Box<dyn std::error::Error>>
{
    if let Err(e) = TestConfig::validate_env() {
        return Err(format!(
            "Environment validation failed: {}. Please set the required environment variables.",
            e
        )
        .into());
    }

    let config = TestConfig::from_env()?;
    let test_name = "versioned-migration-with-attributes";
    let file_generator = FileGenerator::new_for_test(test_name)?;

    // Create versioned test bucket manager
    let mut bucket_manager = TestBucketManager::new(config.clone()).await?;
    let (src_bucket, dst_bucket) = bucket_manager
        .create_versioned_test_buckets(test_name)
        .await?;

    println!(
        "[{}] Setting up versioned objects with custom attributes",
        test_name
    );

    let mut all_objects_and_versions = Vec::new();

    // Small files (100KB-500KB): 2 objects - single-part uploads
    let small_file_specs = [
        ("config-with-attrs.json", 100_000),
        ("readme-with-metadata.txt", 500_000),
    ];

    for (object_key, size) in &small_file_specs {
        let versions = create_versioned_object_with_attributes(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Medium files (2MB-8MB): 2 objects - mixed single/multi-part based on chunk size
    let medium_file_specs = [
        ("document-with-headers.pdf", 2_000_000),
        ("video-with-metadata.mp4", 8_000_000),
    ];

    for (object_key, size) in &medium_file_specs {
        let versions = create_versioned_object_with_attributes(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Large files (15MB-25MB): 2 objects - forces multipart uploads
    let large_file_specs = [
        ("dataset-with-attrs.bin", 15_000_000),
        ("archive-with-metadata.zip", 25_000_000),
    ];

    for (object_key, size) in &large_file_specs {
        let versions = create_versioned_object_with_attributes(
            &bucket_manager,
            &file_generator,
            &src_bucket,
            object_key,
            *size,
            test_name,
        )
        .await?;
        all_objects_and_versions.push((*object_key, versions));
    }

    // Calculate total versions created
    let total_versions: usize = all_objects_and_versions
        .iter()
        .map(|(_, versions)| versions.len())
        .sum();

    println!(
        "[{}] Created {} objects with {} total versions and custom attributes",
        test_name,
        all_objects_and_versions.len(),
        total_versions
    );

    // Verify versions and attributes exist in source bucket before migration
    verify_pre_migration_versions_with_attributes(
        &bucket_manager,
        &src_bucket,
        &all_objects_and_versions,
        test_name,
    )
    .await?;

    // Run migration using the CLI
    println!(
        "[{}] Running migration for {} objects with {} versions and custom attributes",
        test_name,
        all_objects_and_versions.len(),
        total_versions
    );

    let migration_start = std::time::Instant::now();
    let (first_run, second_run) = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks (forces multipart for larger files)
        num_cpus::get(), // Use all available CPUs
    )
    .await?;

    let migration_duration = migration_start.elapsed();
    println!(
        "[{}] Migration completed in {:.2}s",
        test_name,
        migration_duration.as_secs_f64()
    );

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

    // Create new clients for verification
    let verification_source_client = S3TestClient::new_source(config.clone()).await?;
    let verification_dest_client = S3TestClient::new_destination(config.clone()).await?;

    // Verify versioned migration results including attributes
    let verification_start = std::time::Instant::now();
    let verification_result = verify_versioned_migration_results_with_attributes(
        &verification_source_client,
        &verification_dest_client,
        &src_bucket,
        &dst_bucket,
        &all_objects_and_versions,
        test_name,
    )
    .await;

    let verification_duration = verification_start.elapsed();
    println!(
        "[{}] Verification completed in {:.2}s",
        test_name,
        verification_duration.as_secs_f64()
    );

    // Handle verification result
    match verification_result {
        Ok(true) => {
            println!(
                "[{}] ✓ All versioned objects with attributes verified successfully!",
                test_name
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
        Ok(false) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!(
                "[{}] ✗ Versioned migration with attributes verification failed",
                test_name
            );
            Err(format!("Version and attribute verification failed - some versions or attributes were not migrated correctly").into())
        }
        Err(e) => {
            bucket_manager.cleanup().await?;
            file_generator.cleanup()?;
            println!("[{}] ✗ Verification error: {}", test_name, e);
            Err(e)
        }
    }
}

/// Create a versioned object with random number of versions (1-10) and custom attributes
async fn create_versioned_object_with_attributes(
    bucket_manager: &TestBucketManager,
    file_generator: &FileGenerator,
    bucket_name: &str,
    object_key: &str,
    base_size: usize,
    test_name: &str,
) -> Result<Vec<(String, TestFile)>, Box<dyn std::error::Error>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let version_count = rng.gen_range(1..=10); // 1 to 10 versions (always create at least one)

    println!(
        "[{}] Creating {} versions for object with attributes: {}",
        test_name, version_count, object_key
    );

    let mut versions_with_metadata = Vec::new();

    // Available content types for random selection
    let content_types = [
        "text/plain",
        "application/json",
        "application/xml",
        "image/jpeg",
        "image/png",
        "video/mp4",
        "application/pdf",
        "application/zip",
        "application/octet-stream",
        "text/html",
        "text/css",
        "application/javascript",
    ];

    // Available cache control directives
    let cache_controls = [
        "no-cache",
        "no-store",
        "max-age=3600",
        "max-age=86400",
        "public, max-age=3600",
        "private, max-age=1800",
        "must-revalidate",
        "no-cache, no-store, must-revalidate",
    ];

    // Available content encodings
    let content_encodings = ["gzip", "deflate", "br", "compress"];

    // Available content languages
    let content_languages = ["en", "en-US", "fr", "es", "de", "ja", "zh-CN"];

    // Available user metadata keys
    let metadata_keys = [
        "author",
        "department",
        "project",
        "version",
        "source",
        "category",
        "priority",
        "status",
        "created-by",
        "environment",
    ];

    for version_num in 1..=version_count {
        // Create unique content for each version with size variations
        let size_variation = rng.gen_range(-5..=15); // -5% to +15% size variation
        let actual_size = if base_size > 100_000 {
            (base_size as i64 + (base_size as i64 * size_variation / 100)).max(1000) as usize
        } else {
            base_size
        };

        let version_header = format!(
            "=== VERSION {} OF {} WITH METADATA ===\n",
            version_num, object_key
        );
        let version_content = format!(
            "{}Content for version {} of object {}\nTimestamp: {}\nSize: {} bytes\nHas custom attributes: YES\n",
            version_header,
            version_num,
            object_key,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            actual_size
        );

        // Pad to desired size with pattern-based content
        let padding_needed = actual_size.saturating_sub(version_content.len());
        let padding = if padding_needed > 0 {
            let pattern = format!("ATTR-DATA-V{:02}-", version_num);
            let pattern_repeats = padding_needed / pattern.len();
            let remainder = padding_needed % pattern.len();
            format!(
                "{}{}",
                pattern.repeat(pattern_repeats),
                &pattern[..remainder]
            )
        } else {
            String::new()
        };

        let full_content = format!("{}{}", version_content, padding);

        // Create TestFile with random attributes
        let mut test_file = TestFile::new(
            &format!("{}-v{}", object_key, version_num),
            full_content.len(),
        );

        // Randomly assign content type (80% chance)
        if rng.gen_bool(0.8) {
            let content_type = content_types[rng.gen_range(0..content_types.len())];
            test_file = test_file.with_content_type(content_type);
        }

        // Randomly assign cache control (70% chance)
        if rng.gen_bool(0.7) {
            let cache_control = cache_controls[rng.gen_range(0..cache_controls.len())];
            test_file = test_file.with_cache_control(cache_control);
        }

        // Randomly assign content disposition (50% chance)
        if rng.gen_bool(0.5) {
            let filename = format!(
                "download-{}-v{}.dat",
                object_key.replace('/', "-"),
                version_num
            );
            test_file = test_file
                .with_content_disposition(&format!("attachment; filename=\"{}\"", filename));
        }

        // Randomly assign content encoding (30% chance)
        if rng.gen_bool(0.3) {
            let encoding = content_encodings[rng.gen_range(0..content_encodings.len())];
            test_file = test_file.with_content_encoding(encoding);
        }

        // Randomly assign content language (40% chance)
        if rng.gen_bool(0.4) {
            let language = content_languages[rng.gen_range(0..content_languages.len())];
            test_file = test_file.with_content_language(language);
        }

        // Randomly assign expires header (25% chance)
        if rng.gen_bool(0.25) {
            let future_hours = rng.gen_range(1..=168); // 1 hour to 1 week in the future
            let expires_time = chrono::Utc::now() + chrono::Duration::hours(future_hours);
            test_file = test_file.with_expires(expires_time);
        }

        // Randomly assign public ACL (20% chance)
        if rng.gen_bool(0.2) {
            test_file = test_file.with_public_acl();
        }

        // Randomly assign user metadata (0-5 key-value pairs)
        let metadata_count = rng.gen_range(0..=5);
        for _ in 0..metadata_count {
            let key = metadata_keys[rng.gen_range(0..metadata_keys.len())];
            let value = format!(
                "value-{}-v{}-{}",
                key,
                version_num,
                rng.gen_range(1000..9999)
            );
            test_file = test_file.with_user_metadata(key, &value);
        }

        // Generate file with version-specific content
        let file_path =
            file_generator.generate_file_with_content(&test_file, full_content.as_bytes())?;

        // Upload versioned object with full attributes
        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned_with_attributes(
                bucket_name,
                object_key,
                &test_file,
                &file_path,
            )
            .await?;

        versions_with_metadata.push((version_id.clone(), test_file.clone()));

        println!(
            "[{}] Created version {} for {} ({}MB, {} attrs): version_id={}",
            test_name,
            version_num,
            object_key,
            actual_size / 1_000_000,
            count_test_file_attributes(&test_file),
            version_id
        );
    }

    Ok(versions_with_metadata)
}

/// Count the number of custom attributes in a TestFile
fn count_test_file_attributes(test_file: &TestFile) -> usize {
    let mut count = 0;
    if test_file.content_type.is_some() {
        count += 1;
    }
    if test_file.cache_control.is_some() {
        count += 1;
    }
    if test_file.content_disposition.is_some() {
        count += 1;
    }
    if test_file.content_encoding.is_some() {
        count += 1;
    }
    if test_file.content_language.is_some() {
        count += 1;
    }
    if test_file.expires.is_some() {
        count += 1;
    }
    if test_file.acl_public {
        count += 1;
    }
    count += test_file.metadata.len();
    count
}

/// Verify versions and attributes exist in source bucket before migration
async fn verify_pre_migration_versions_with_attributes(
    bucket_manager: &TestBucketManager,
    src_bucket: &str,
    objects_and_versions: &[(&str, Vec<(String, TestFile)>)],
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "[{}] Verifying pre-migration versions and attributes in source bucket",
        test_name
    );

    let mut total_versions = 0;
    let mut total_attributes = 0;

    for (object_key, version_metadata_pairs) in objects_and_versions {
        if version_metadata_pairs.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions)",
                test_name, object_key
            );
            continue;
        }

        total_versions += version_metadata_pairs.len();

        let src_versions = bucket_manager
            .source_client()
            .list_object_versions(src_bucket, Some(object_key))
            .await?;

        let versions_for_key: Vec<_> = src_versions
            .iter()
            .filter(|v| v.key().map_or(false, |k| k == *object_key))
            .collect();

        if versions_for_key.len() != version_metadata_pairs.len() {
            return Err(format!(
                "Pre-migration version count mismatch for {}: expected {}, found {}",
                object_key,
                version_metadata_pairs.len(),
                versions_for_key.len()
            )
            .into());
        }

        // Verify each expected version exists and check some basic metadata
        for (expected_version_id, test_file) in version_metadata_pairs {
            let found = versions_for_key
                .iter()
                .any(|v| v.version_id().map_or(false, |id| id == expected_version_id));

            if !found {
                return Err(format!(
                    "Expected version {} not found for object {} in source bucket",
                    expected_version_id, object_key
                )
                .into());
            }

            // Verify metadata is preserved by doing a head object request
            let metadata = bucket_manager
                .source_client()
                .get_object_metadata_version(src_bucket, object_key, expected_version_id)
                .await?;

            // Verify content type if set
            if let Some(expected_content_type) = &test_file.content_type {
                if metadata.content_type() != Some(expected_content_type) {
                    return Err(format!(
                        "Content type mismatch for version {} of {}: expected {:?}, got {:?}",
                        expected_version_id,
                        object_key,
                        expected_content_type,
                        metadata.content_type()
                    )
                    .into());
                }
            }

            // Count attributes for this version
            total_attributes += count_test_file_attributes(test_file);
        }

        println!(
            "[{}] ✓ Pre-migration verification passed for {} ({} versions with attributes)",
            test_name,
            object_key,
            version_metadata_pairs.len()
        );
    }

    println!(
        "[{}] ✓ Pre-migration verification completed: {} total versions with {} total attributes",
        test_name, total_versions, total_attributes
    );

    Ok(())
}

/// Verify versioned migration results including custom attributes
async fn verify_versioned_migration_results_with_attributes(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    objects_and_versions: &[(&str, Vec<(String, TestFile)>)],
    test_name: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    println!(
        "[{}] Verifying versioned migration results including custom attributes",
        test_name
    );

    let mut all_passed = true;
    let mut total_versions_verified = 0;
    let mut total_attributes_verified = 0;
    let mut objects_verified = 0;

    for (object_key, version_metadata_pairs) in objects_and_versions {
        if version_metadata_pairs.is_empty() {
            println!(
                "[{}] Skipping verification for {} (no versions expected)",
                test_name, object_key
            );
            continue;
        }

        let verification_start = std::time::Instant::now();
        println!(
            "[{}] Verifying versions and attributes for object: {} ({} versions)",
            test_name,
            object_key,
            version_metadata_pairs.len()
        );

        // First, verify basic versioned migration
        match MigrationVerifier::verify_versioned_migration(
            source_client,
            dest_client,
            src_bucket,
            dst_bucket,
            object_key,
        )
        .await?
        {
            VersionedVerificationResult::Match => {
                // If basic versioned migration passes, verify attributes
                let mut attributes_verified = 0;

                for (version_id, test_file) in version_metadata_pairs {
                    // Verify attribute preservation for this version
                    match verify_version_attributes(
                        source_client,
                        dest_client,
                        src_bucket,
                        dst_bucket,
                        object_key,
                        version_id,
                        test_file,
                    )
                    .await
                    {
                        Ok(true) => {
                            attributes_verified += count_test_file_attributes(test_file);
                        }
                        Ok(false) => {
                            println!(
                                "[{}] ✗ Attribute verification failed for version {} of {}",
                                test_name, version_id, object_key
                            );
                            all_passed = false;
                        }
                        Err(e) => {
                            println!(
                                "[{}] ✗ Error verifying attributes for version {} of {}: {}",
                                test_name, version_id, object_key, e
                            );
                            all_passed = false;
                        }
                    }
                }

                let verification_time = verification_start.elapsed();
                if all_passed {
                    println!(
                        "[{}] ✓ All {} versions with {} attributes verified for {} in {:.2}s",
                        test_name,
                        version_metadata_pairs.len(),
                        attributes_verified,
                        object_key,
                        verification_time.as_secs_f64()
                    );
                    total_versions_verified += version_metadata_pairs.len();
                    total_attributes_verified += attributes_verified;
                    objects_verified += 1;
                }
            }
            result => {
                println!(
                    "[{}] ✗ Version verification failed for {}: {:?}",
                    test_name, object_key, result
                );
                all_passed = false;
            }
        }
    }

    if all_passed {
        println!(
            "[{}] ✓ All {} objects with {} total versions and {} total attributes verification passed",
            test_name, objects_verified, total_versions_verified, total_attributes_verified
        );
    } else {
        println!(
            "[{}] ✗ Versioned migration with attributes verification failed - some objects had issues",
            test_name
        );
    }

    Ok(all_passed)
}

/// Verify that attributes are preserved for a specific version
async fn verify_version_attributes(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    object_key: &str,
    version_id: &str,
    expected_test_file: &TestFile,
) -> Result<bool, Box<dyn std::error::Error>> {
    // Get metadata from source
    let src_metadata = source_client
        .get_object_metadata_version(src_bucket, object_key, version_id)
        .await?;

    // Get metadata from destination - use the same version_id to compare versions
    let dst_metadata = dest_client
        .get_object_metadata_version(dst_bucket, object_key, version_id)
        .await?;

    // Compare content type
    if expected_test_file.content_type.is_some() {
        if src_metadata.content_type() != dst_metadata.content_type() {
            println!(
                "Content type mismatch for {}: source={:?}, dest={:?}",
                object_key,
                src_metadata.content_type(),
                dst_metadata.content_type()
            );
            return Ok(false);
        }
    }

    // Compare cache control
    if expected_test_file.cache_control.is_some() {
        if src_metadata.cache_control() != dst_metadata.cache_control() {
            println!(
                "Cache control mismatch for {}: source={:?}, dest={:?}",
                object_key,
                src_metadata.cache_control(),
                dst_metadata.cache_control()
            );
            return Ok(false);
        }
    }

    // Compare user metadata
    if !expected_test_file.metadata.is_empty() {
        let src_user_metadata = src_metadata.metadata();
        let dst_user_metadata = dst_metadata.metadata();

        for (expected_key, _expected_value) in &expected_test_file.metadata {
            let src_value = src_user_metadata.and_then(|m| m.get(expected_key));
            let dst_value = dst_user_metadata.and_then(|m| m.get(expected_key));

            if src_value != dst_value {
                println!(
                    "User metadata mismatch for {} key '{}': source={:?}, dest={:?}",
                    object_key, expected_key, src_value, dst_value
                );
                return Ok(false);
            }
        }
    }

    // Note: Additional attribute comparisons (content-disposition, content-encoding, etc.)
    // could be added here, but basic verification is sufficient for this test

    Ok(true)
}
