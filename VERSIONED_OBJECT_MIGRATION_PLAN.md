# Versioned Object Migration Implementation Plan

## Overview

This document outlines the comprehensive plan for implementing versioned object migration support in the Cellar C1 Migration Tool. The implementation will ensure that object versions are replicated from source bucket to destination bucket with exact version ID matching.

## Requirements

### Core Requirements
1. **Exact Version ID Matching**: Destination bucket must contain identical version IDs to source bucket
2. **Content Preservation**: Each version's content must match exactly between source and destination
3. **Metadata Preservation**: Each version's metadata must be preserved across migration
4. **Custom Implementation**: Use custom implementation (not AWS SDK) to achieve version ID preservation
5. **Version Ordering**: Order of versions can be different (doesn't impact version queries)

### Exclusions (Current Phase)
- Delete markers (versioned deletions) - not handled in this phase
- Version expiration policies
- Cross-region replication specific features

## Test Strategy

### Test Structure
Two complementary tests will be implemented to validate versioned object migration:

1. **Simple Test**: Basic validation with minimal complexity
2. **Complex Test**: Comprehensive validation with diverse scenarios

### Test Framework Integration
- Leverage existing test infrastructure (`TestBucketManager`, `S3TestClient`, `MigrationVerifier`)
- Extend verification utilities to handle versioned objects
- Maintain consistency with existing test patterns

## Test 1: Simple Versioned Object Migration

### Test Name
`test_simple_versioned_object_migration`

### Test Objectives
- Validate basic versioned object migration functionality
- Test both single-part and multi-part upload scenarios
- Verify version ID preservation in simple cases

### Test Objects

| Object Name | Size | Type | Version Count | Purpose |
|-------------|------|------|---------------|---------|
| `single-part-versioned.txt` | 1MB | Single-part | 0-10 (random) | Test single-part version migration |
| `multi-part-versioned.bin` | 150MB | Multi-part | 0-10 (random) | Test multi-part version migration |

### Version Creation Strategy
```
For each test object:
1. Determine random version count (0-10)
2. Create base content and metadata
3. Upload initial version
4. For each additional version:
   - Generate unique content (append version suffix)
   - Modify metadata if needed
   - Upload to same key (S3 auto-generates version ID)
5. Record all version IDs for verification
```

### Verification Process
1. **Pre-Migration Verification**:
   - List all versions in source bucket
   - Verify expected number of versions exist
   - Store version metadata for comparison

2. **Migration Execution**:
   - Run migration tool with existing `run_basic_migration` function
   - Capture migration results and logs

3. **Post-Migration Verification**:
   - List all versions in destination bucket
   - Verify exact version ID matching
   - Verify content matching for each version
   - Verify metadata preservation for each version

## Test 2: Complex Versioned Object Migration

### Test Name
`test_complex_versioned_object_migration`

### Test Objectives
- Comprehensive validation with diverse object types and sizes
- Test various content types and metadata combinations
- Validate performance with multiple versioned objects

### Test Objects (8-12 total)

#### Small Files (3-4 objects)
- Size range: 1KB - 1MB
- Version count: 0-10 (random per object)
- Content types: text/plain, application/json
- Metadata: Basic cache-control, content-disposition

#### Medium Files (3-4 objects)
- Size range: 5MB - 50MB
- Version count: 0-10 (random per object)
- Content types: application/json, image/jpeg (simulated)
- Metadata: Complex cache-control, custom user metadata

#### Large Files (2-3 objects)
- Size range: 100MB - 200MB
- Version count: 0-10 (random per object)
- Content types: application/octet-stream, video/mp4 (simulated)
- Metadata: Expires headers, content-encoding, custom metadata

### Version Diversity Strategy
- **Content Variation**: Each version has unique content (size may vary)
- **Metadata Variation**: Some versions have different metadata
- **Mixed Upload Types**: Automatic single-part vs multi-part based on size
- **Random Distribution**: Version counts randomly distributed across 0-10 range

### Verification Process
Same as simple test but applied to all objects with additional checks:
- Performance metrics (migration time, throughput)
- Memory usage validation
- Concurrent version processing verification

## Implementation Details

### New S3 Client Methods Required

#### Version Listing
```rust
pub async fn list_object_versions(
    &self,
    bucket_name: &str,
    prefix: Option<&str>,
) -> Result<Vec<ObjectVersion>, Box<dyn std::error::Error>>
```

#### Versioned Object Operations
```rust
pub async fn get_object_version(
    &self,
    bucket_name: &str,
    key: &str,
    version_id: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>>

pub async fn get_object_metadata_version(
    &self,
    bucket_name: &str,
    key: &str,
    version_id: &str,
) -> Result<HeadObjectOutput, Box<dyn std::error::Error>>
```

#### Bucket Versioning Management
```rust
pub async fn enable_bucket_versioning(
    &self,
    bucket_name: &str,
) -> Result<(), Box<dyn std::error::Error>>

pub async fn get_bucket_versioning(
    &self,
    bucket_name: &str,
) -> Result<BucketVersioningStatus, Box<dyn std::error::Error>>
```

### Verification Extensions

#### Version-Aware Verification
```rust
pub async fn verify_versioned_migration(
    source_client: &S3TestClient,
    dest_client: &S3TestClient,
    src_bucket: &str,
    dst_bucket: &str,
    object_key: &str,
) -> Result<VersionedVerificationResult, Box<dyn std::error::Error>>
```

#### Version Comparison Logic
```rust
// Core verification algorithm
for src_version in source_versions {
    // Find matching version in destination
    let dst_version = find_version_by_id(&dst_versions, &src_version.version_id)?;

    // Verify content matches
    verify_version_content(src_bucket, dst_bucket, key, version_id).await?;

    // Verify metadata matches
    verify_version_metadata(src_metadata, dst_metadata).await?;
}
```

## Current Progress

### ✅ Completed
- [x] Analyzed existing test structure and patterns
- [x] Designed comprehensive test plan
- [x] Documented requirements and specifications
- [x] Created detailed implementation roadmap

### 🔄 In Progress
- [ ] Documenting comprehensive test plan and progress

### ⏳ Pending
- [ ] Implement S3 client versioning extensions
- [ ] Create simple versioned object migration test
- [ ] Create complex versioned object migration test
- [ ] Extend verification utilities for versioned objects
- [ ] Add bucket versioning management utilities

## Technical Considerations

### Version ID Preservation Challenge
The AWS SDK typically doesn't support preserving version IDs during object uploads. The custom implementation mentioned will need to:
- Bypass standard AWS SDK upload methods
- Use direct S3 API calls with version ID specification
- Handle authentication and error handling manually
- Maintain compatibility with existing provider interface

### Memory and Performance
- Large objects with many versions could impact memory usage
- Consider streaming approaches for content verification
- Implement efficient version listing with pagination
- Monitor concurrent version processing performance

### Error Handling
- Version ID conflicts or mismatches
- Partial migration failures (some versions succeed, others fail)
- Source bucket versioning disabled scenarios
- Network failures during version operations

## Migration Tool Integration

### Expected Changes Required
1. **Provider Interface Extension**: Add version-aware methods to provider trait
2. **Migration Logic Update**: Extend sync logic to handle versions
3. **CLI Arguments**: Add version migration flags/options
4. **Progress Reporting**: Update progress indicators for version operations

### Test-Driven Development Approach
1. Implement tests first (demonstrate current gaps)
2. Tests will initially fail (migration tool doesn't handle versions)
3. Implement version support incrementally
4. Use tests to validate each implementation step
5. Achieve full test passing as completion criteria

## Success Criteria

### Test Passing Requirements
- Both tests execute without errors
- All version IDs match exactly between source and destination
- All version content matches byte-for-byte
- All version metadata preserved correctly
- Performance meets acceptable thresholds
- Memory usage remains within reasonable bounds

### Documentation and Maintainability
- Code follows existing project patterns
- Comprehensive error messages for debugging
- Clear separation between test and production code
- Maintainable test structure for future enhancements

## Next Steps

1. **Implement Test Infrastructure**:
   - Add S3 versioning utilities to `s3_test_utils.rs`
   - Extend verification utilities in `verification.rs`

2. **Create Test Files**:
   - `tests/versioned_simple.rs` - Simple versioned migration test
   - `tests/versioned_complex.rs` - Complex versioned migration test

3. **Validate Current Behavior**:
   - Run tests to document current migration tool behavior
   - Confirm tests fail as expected (version support missing)

4. **Iterative Implementation**:
   - Implement version support in migration tool
   - Use tests to validate each step
   - Achieve full test passing

This plan provides a comprehensive roadmap for implementing robust versioned object migration with thorough testing validation.