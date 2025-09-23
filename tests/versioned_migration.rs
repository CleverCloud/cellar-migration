//! Versioned object migration integration tests
//! These tests verify versioned object migration functionality across simple and complex scenarios

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
/// - Migration tool execution (expected to not handle versions correctly yet)
/// - Version ID preservation verification (expected to fail initially)
/// - Content and metadata preservation across versions
///
/// **Expected Result (Current Implementation):**
/// - Migration finishes but likely only migrates latest versions
/// - Version verification fails due to missing version support
/// - Test documents current behavior gap
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
    let (src_bucket, dst_bucket) = bucket_manager.create_versioned_test_buckets(test_name).await?;

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

    // Run migration using the CLI (expected to not handle versions correctly yet)
    println!("[{}] Running migration (version support not implemented yet)", test_name);
    let migration_result = run_basic_migration(
        &config,
        &src_bucket,
        &dst_bucket,
        5,               // 5MB chunks (forces multipart for 150MB file)
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

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    // Handle verification result - for now, we expect it to fail since version support isn't implemented
    match verification_result {
        Ok(true) => {
            println!("[{}] ✓ All versioned objects verified successfully!", test_name);
            Ok(())
        }
        Ok(false) => {
            println!("[{}] ✗ Versioned migration verification failed (expected - version support not implemented)", test_name);
            println!("[{}] Test documents current behavior - version support needed", test_name);
            Err(format!("Version verification failed - migration tool doesn't support versioned objects yet").into())
        }
        Err(e) => {
            println!("[{}] ✗ Verification error: {}", test_name, e);
            Err(e)
        }
    }
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
/// **Expected Result (Current Implementation):**
/// - Migration finishes but likely only migrates latest versions
/// - Version verification fails due to missing version support
/// - Test documents comprehensive version migration requirements
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
    let (src_bucket, dst_bucket) = bucket_manager.create_versioned_test_buckets(test_name).await?;

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

    // Run migration using the CLI (expected to not handle versions correctly yet)
    println!(
        "[{}] Running migration for {} objects with {} versions total",
        test_name,
        all_objects_and_versions.len(),
        total_versions
    );

    let migration_start = std::time::Instant::now();
    let migration_result = run_basic_migration(
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

    if !migration_result.success() {
        return Err(format!(
            "Migration CLI failed with exit code: {}",
            migration_result.code().unwrap_or(-1)
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

    // Cleanup
    bucket_manager.cleanup().await?;
    file_generator.cleanup()?;

    // Handle verification result - for now, we expect it to fail since version support isn't implemented
    match verification_result {
        Ok(true) => {
            println!("[{}] ✓ All complex versioned objects verified successfully!", test_name);
            Ok(())
        }
        Ok(false) => {
            println!("[{}] ✗ Complex versioned migration verification failed (expected - version support not implemented)", test_name);
            println!("[{}] Test documents current behavior - comprehensive version support needed", test_name);
            Err(format!("Version verification failed - migration tool doesn't support versioned objects yet").into())
        }
        Err(e) => {
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
        println!("[{}] Object {} will have no versions (object doesn't exist)", test_name, object_key);
        return Ok(Vec::new());
    }

    let mut version_ids = Vec::new();

    for version_num in 1..=version_count {
        // Create unique content for each version
        let version_content = format!("Version {} content for {}", version_num, object_key);
        let full_content = format!("{}\n{}", version_content, "x".repeat(base_size - version_content.len() - 1));

        // Create test file with version-specific content (no custom metadata)
        let test_file = TestFile::new(&format!("{}-v{}", object_key, version_num), full_content.len());

        // Generate file with version-specific content
        let file_path = file_generator.generate_file_with_content(&test_file, full_content.as_bytes())?;

        // Upload clean versioned object (no metadata)
        let version_id = bucket_manager
            .source_client()
            .upload_test_file_versioned(bucket_name, object_key, &file_path)
            .await?;

        version_ids.push(version_id);

        println!(
            "[{}] Created version {} for {}: version_id={}",
            test_name, version_num, object_key, version_ids.last().unwrap()
        );
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
        println!("[{}] Object {} will have no versions (object doesn't exist)", test_name, object_key);
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
        let test_file = TestFile::new(&format!("{}-v{}", object_key, version_num), full_content.len());

        // Generate file with version-specific content
        let file_path = file_generator.generate_file_with_content(&test_file, full_content.as_bytes())?;

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
    println!("[{}] Verifying pre-migration versions in source bucket", test_name);

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!("[{}] Skipping verification for {} (no versions)", test_name, object_key);
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

/// Verify complex versions exist in source bucket before migration
async fn verify_complex_pre_migration_versions(
    bucket_manager: &TestBucketManager,
    src_bucket: &str,
    objects_and_versions: &[(&str, Vec<String>)],
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[{}] Verifying complex pre-migration versions in source bucket", test_name);

    let mut total_versions = 0;
    let mut objects_with_versions = 0;

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!("[{}] Skipping verification for {} (no versions)", test_name, object_key);
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
            println!("[{}] Skipping verification for {} (no versions expected)", test_name, object_key);
            continue;
        }

        println!("[{}] Verifying versions for object: {}", test_name, object_key);

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
        println!("[{}] ✓ All versioned objects verification passed", test_name);
        Ok(true)
    } else {
        println!("[{}] ✗ Some versioned objects verification failed", test_name);
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
    println!("[{}] Verifying complex versioned migration results", test_name);

    let mut all_passed = true;
    let mut total_versions_verified = 0;
    let mut objects_verified = 0;

    for (object_key, expected_versions) in objects_and_versions {
        if expected_versions.is_empty() {
            println!("[{}] Skipping verification for {} (no versions expected)", test_name, object_key);
            continue;
        }

        let verification_start = std::time::Instant::now();
        println!("[{}] Verifying versions for object: {} ({} versions)", test_name, object_key, expected_versions.len());

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
        println!("[{}] ✓ Comprehensive version verification passed", test_name);
    } else {
        println!("[{}] ✗ Comprehensive version verification failed", test_name);
        all_passed = false;
    }

    Ok(all_passed)
}