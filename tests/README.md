# Integration Test Suite Overview

The integration suite is organised around "one feature per test" plus a single end-to-end scenario that exercises the full pipeline. Use this document to pick the right test when working on a migration change.

## Feature-Focused Coverage
- `tests/single_file.rs::test_basic_single_object_copy` – baseline success path for a single object.
- `tests/single_file.rs::test_preserves_content_type` – verifies `Content-Type` header propagation.
- `tests/single_file.rs::test_preserves_cache_control` – covers `Cache-Control` directives.
- `tests/single_file.rs::test_preserves_content_disposition` – checks `Content-Disposition` filenames.
- `tests/single_file.rs::test_preserves_content_encoding` – keeps `Content-Encoding` values.
- `tests/single_file.rs::test_preserves_content_language` – ensures `Content-Language` locale survives.
- `tests/single_file.rs::test_preserves_expires_header` – verifies `Expires` timestamp preservation.
- `tests/single_file.rs::test_preserves_user_metadata` – validates custom user metadata copying.
- `tests/single_file.rs::test_preserves_acl` – ensures public-read ACLs propagate alongside data.
- `tests/single_file.rs::test_multipart_upload_behavior` – validates multipart uploads and verification tolerance.
- `tests/multiple_files.rs::test_pagination_handles_large_list` – stresses list pagination with 2 000 objects and a `max-keys` cap of 100.
- `tests/dry_run_validation.rs::test_dry_run_no_side_effects` – dry-run against empty destination keeps it empty.
- `tests/dry_run_validation.rs::test_dry_run_when_already_synced` – dry-run + execute when buckets already match stays a no-op.
- `tests/bucket_state.rs::test_partial_sync_scenario` – copies only missing keys when destination is partially seeded.
- `tests/bucket_state.rs::test_conflicting_files_scenario` – overwrites conflicting content/metadata in destination.
- `tests/error_recovery.rs::test_invalid_credentials_handling` – surfaces authentication failures cleanly.
- `tests/error_recovery.rs::test_nonexistent_source_bucket` – handles missing source bucket failure.
- `tests/error_recovery.rs::test_bucket_creation_on_missing_destination` – auto-creates destination bucket when absent.
- `tests/error_recovery.rs::test_empty_bucket_handling` – verifies empty source is a supported noop.
- `tests/multiple_files.rs::test_unicode_and_special_character_files` – guards complex object keys.
- `tests/http_server.rs::test_http_server_responds` – smoke tests the status server binary.

## End-to-End Regression
- `tests/multiple_files.rs::test_end_to_end_migration_scenario` – combines metadata-rich (with public ACL), multipart, zero-byte and unicode objects to check cross-feature interactions in one run.

## Notes
- All helpers live under the `test_common` crate (`tests/test-common/src/config.rs` contains the env wiring); run `TestConfig::validate_env()` locally before executing the suite.
- Large pagination runs always enforce small `max-keys` to surface continuation handling; consider running selectively when targeting pagination behavior.
