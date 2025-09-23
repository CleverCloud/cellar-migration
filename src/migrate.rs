use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
    error,
    sync::{Arc, Mutex},
};

use aws_sdk_s3::{
    error::SdkError,
    operation::{create_bucket::CreateBucketError, list_objects_v2::ListObjectsV2Error},
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use bytesize::ByteSize;
use futures::StreamExt;

use std::time::Duration;
use tokio::task::JoinError;
use tracing::{event, instrument, Level};

use crate::{
    provider::{
        get_provider, Provider, ProviderConf, ProviderObject, ProviderVersionKind, Providers,
    },
    radosgw::{
        uploader::{ThreadMigrationResult, Uploader},
        RadosGW,
    },
};

#[allow(dead_code)]
#[derive(Debug)]
pub struct BucketMigrationStats {
    pub bucket: String,
    pub synchronization_time: Duration,
    pub synchronization_size: usize,
    pub delete_size: usize,
    pub total_files_sync: usize,
    pub total_files_delete: usize,
}

#[derive(Debug)]
pub struct BucketMigrationError {
    pub errors: Vec<String>,
    pub stats: BucketMigrationStats,
}

impl error::Error for BucketMigrationError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl std::fmt::Display for BucketMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct BucketMigrationConfiguration {
    pub source_bucket: String,
    pub source_access_key: String,
    pub source_secret_key: String,
    pub source_endpoint: Option<String>,
    pub source_region: Option<String>,
    pub source_provider: Providers,
    pub destination_bucket: String,
    pub destination_access_key: String,
    pub destination_secret_key: String,
    pub destination_endpoint: String,
    pub delete_destination_files: bool,
    #[allow(dead_code)]
    pub max_keys: usize,
    pub chunk_size: usize,
    pub sync_threads: usize,
    pub dry_run: bool,
}

pub enum BucketObjectsMigrationResult {
    DryRun(Vec<ProviderObject>, Vec<ProviderObject>),
    Executed(Vec<Result<ThreadMigrationResult, JoinError>>),
}

#[instrument(skip_all, level = "debug")]
async fn migrate_objects(
    conf: BucketMigrationConfiguration,
    src_objects: &[ProviderObject],
    dst_objects: &[ProviderObject],
    source_clients: Arc<Mutex<VecDeque<Box<dyn Provider>>>>,
    radosgw_clients: Arc<Mutex<VecDeque<RadosGW>>>,
) -> BucketObjectsMigrationResult {
    let dest_index: BTreeMap<(String, Option<String>), &ProviderObject> = dst_objects
        .iter()
        .map(|object| (version_lookup_key(object), object))
        .collect();

    let mut seen_keys: BTreeSet<(String, Option<String>)> = BTreeSet::new();

    let objects_to_migrate: Vec<ProviderObject> = src_objects
        .iter()
        .filter_map(|object| {
            let key = version_lookup_key(object);
            seen_keys.insert(key.clone());

            match dest_index.get(&key) {
                Some(existing) => {
                    if object != *existing {
                        Some(object.clone())
                    } else {
                        None
                    }
                }
                None => Some(object.clone()),
            }
        })
        .collect();

    let objects_to_delete: Vec<ProviderObject> = if conf.delete_destination_files {
        dst_objects
            .iter()
            .filter_map(|object| {
                let key = version_lookup_key(object);
                if !seen_keys.contains(&key) {
                    Some(object.clone())
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let objects_to_sync = objects_to_migrate.len() + objects_to_delete.len();

    if !conf.dry_run {
        if objects_to_sync > 0 {
            let mut uploader = Uploader::new(
                source_clients,
                radosgw_clients,
                objects_to_migrate,
                objects_to_delete,
                conf.sync_threads,
                conf.chunk_size,
            );
            let results = uploader.sync().await;
            BucketObjectsMigrationResult::Executed(results)
        } else {
            BucketObjectsMigrationResult::Executed(Vec::new())
        }
    } else {
        BucketObjectsMigrationResult::DryRun(objects_to_migrate, objects_to_delete)
    }
}

async fn collect_bucket_objects(
    provider: &dyn Provider,
    use_versions: bool,
    capture_version_metadata: bool,
) -> anyhow::Result<Vec<ProviderObject>> {
    let mut stream = if use_versions {
        provider.list_object_versions(None, None)
    } else {
        provider.list_objects(None, None)
    };

    let mut objects: Vec<ProviderObject> = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        for object in chunk
            .into_iter()
            .filter(|object| !object.is_delete_marker())
        {
            objects.push(object);
        }
    }

    objects.sort_by(|left, right| {
        let key_order = left.key().cmp(right.key());
        if key_order != Ordering::Equal {
            return key_order;
        }

        let modified_order = left.get_last_modified().cmp(right.get_last_modified());
        if modified_order != Ordering::Equal {
            return modified_order;
        }

        match (left.version_id(), right.version_id()) {
            (Some(left_id), Some(right_id)) => left_id.cmp(right_id),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    });

    if use_versions && capture_version_metadata {
        #[allow(clippy::needless_range_loop)]
        for idx in 0..objects.len() {
            if let Some(dest_version) = objects[idx].version_id() {
                let metadata = provider.get_object_version_metadata(&objects[idx]).await?;
                let updated = objects[idx]
                    .clone()
                    .with_version(ProviderVersionKind::Versioned {
                        id: dest_version.to_string(),
                        metadata: Some(Box::new(metadata.clone())),
                    });
                objects[idx] = updated;
            }
        }
    }

    Ok(objects)
}

fn version_lookup_key(object: &ProviderObject) -> (String, Option<String>) {
    let key = object.key().to_string();
    let version_id = object
        .version_metadata()
        .and_then(|metadata| metadata.user_metadata.get("version-id").cloned())
        .or_else(|| object.version_id().map(|id| id.to_string()));

    (key, version_id)
}

#[instrument(skip_all, level = "debug")]
pub async fn migrate_bucket(
    conf: BucketMigrationConfiguration,
) -> anyhow::Result<BucketMigrationStats> {
    let sync_start = std::time::Instant::now();
    let mut source_clients: VecDeque<Box<dyn Provider>> = VecDeque::new();
    let mut destination_clients: VecDeque<RadosGW> = VecDeque::new();

    let async_conf = conf.clone();

    let source_listing_provider = get_provider(
        &conf.source_provider,
        ProviderConf::new(
            conf.source_endpoint.clone(),
            conf.source_region.clone(),
            conf.source_access_key.clone(),
            conf.source_secret_key.clone(),
            Some(conf.source_bucket.clone()),
        ),
    );
    let dest_listing_provider = get_provider(
        &Providers::Cellar,
        ProviderConf::new(
            Some(conf.destination_endpoint.clone()),
            None,
            conf.destination_access_key.clone(),
            conf.destination_secret_key.clone(),
            Some(conf.destination_bucket.clone()),
        ),
    );

    let source_is_versioned = source_listing_provider
        .is_bucket_versioned()
        .await
        .unwrap_or(false);

    let (mut destination_is_versioned, destination_error) =
        match dest_listing_provider.is_bucket_versioned().await {
            Ok(status) => (status, None),
            Err(error) => (false, Some(error)),
        };

    if source_is_versioned {
        if let Some(error) = destination_error {
            if conf.dry_run {
                event!(
                    Level::WARN,
                    "Failed to inspect destination bucket versioning: {:?}",
                    error
                );
            } else {
                return Err(error);
            }
        } else if !destination_is_versioned {
            if conf.dry_run {
                event!(
                    Level::WARN,
                    "Destination bucket {} is not versioned; dry-run would enable versioning before transfer",
                    conf.destination_bucket
                );
            } else {
                event!(
                    Level::INFO,
                    "Enabling versioning on destination bucket {}",
                    conf.destination_bucket
                );
                dest_listing_provider.enable_bucket_versioning().await?;
                destination_is_versioned = true;
            }
        }
    }

    let dest_use_versions = source_is_versioned || destination_is_versioned;

    event!(
        Level::INFO,
        "Source bucket {} versioning: {}",
        conf.source_bucket,
        if source_is_versioned {
            "enabled"
        } else {
            "disabled"
        }
    );
    event!(
        Level::INFO,
        "Destination bucket {} versioning: {}",
        conf.destination_bucket,
        if destination_is_versioned {
            "enabled"
        } else {
            "disabled"
        }
    );

    let source_objects =
        collect_bucket_objects(source_listing_provider.as_ref(), source_is_versioned, false)
            .await?;
    let dest_objects = match collect_bucket_objects(
        dest_listing_provider.as_ref(),
        dest_use_versions,
        source_is_versioned,
    )
    .await
    {
        Ok(objects) => objects,
        Err(error) => match error.downcast::<SdkError<ListObjectsV2Error, HttpResponse>>() {
            Ok(downcast_error) => match downcast_error.into_service_error() {
                ListObjectsV2Error::NoSuchBucket(bucket) => {
                    if conf.dry_run {
                        Vec::new()
                    } else {
                        bucket_already_created(&conf.destination_bucket);
                        anyhow::bail!(
                            "Destination bucket '{}' does not exist on the target cluster",
                            bucket
                        );
                    }
                }
                service_error => anyhow::bail!(service_error),
            },
            Err(error) => return Err(error),
        },
    };

    drop(source_listing_provider);
    drop(dest_listing_provider);

    event!(
        Level::INFO,
        "Collected {} source entries and {} destination entries",
        source_objects.len(),
        dest_objects.len()
    );

    let base_source_provider = get_provider(
        &conf.source_provider,
        ProviderConf::new(
            async_conf.clone().source_endpoint,
            async_conf.clone().source_region,
            async_conf.clone().source_access_key,
            async_conf.clone().source_secret_key,
            Some(async_conf.clone().source_bucket.clone()),
        ),
    );

    let base_radosgw_client = RadosGW::new(
        Some(async_conf.clone().destination_endpoint),
        None,
        async_conf.clone().destination_access_key,
        async_conf.clone().destination_secret_key,
        Some(async_conf.clone().destination_bucket),
    );

    for _ in 0..async_conf.sync_threads {
        source_clients.push_back(base_source_provider.clone());
        destination_clients.push_back(base_radosgw_client.clone());
    }

    let source_clients = Arc::new(Mutex::new(source_clients));
    let destination_clients = Arc::new(Mutex::new(destination_clients));

    let migration_result = migrate_objects(
        async_conf.clone(),
        &source_objects,
        &dest_objects,
        source_clients.clone(),
        destination_clients.clone(),
    )
    .await;

    let mut sync_errors: Vec<anyhow::Error> = Vec::new();
    let mut delete_errors: Vec<anyhow::Error> = Vec::new();
    let mut total_synced_size: usize = 0;
    let mut total_deleted_size: usize = 0;
    let mut total_files_sync: usize = 0;
    let mut total_files_delete: usize = 0;

    match migration_result {
        BucketObjectsMigrationResult::DryRun(to_migrate, to_delete) => {
            total_files_sync = to_migrate.len();
            total_synced_size = to_migrate
                .iter()
                .map(|object| object.get_size() as usize)
                .sum();

            to_migrate.iter().for_each(|object| {
                event!(
                    Level::INFO,
                    "Object to sync : {}/{} - {}",
                    async_conf.source_bucket,
                    object.get_key(),
                    ByteSize(object.get_size())
                );
            });

            event!(
                Level::INFO,
                "Current sync status: {} objects to sync for a total size of {}",
                total_files_sync,
                ByteSize(total_synced_size as u64)
            );

            if async_conf.delete_destination_files {
                total_files_delete = to_delete.len();
                total_deleted_size = to_delete
                    .iter()
                    .map(|object| object.get_size() as usize)
                    .sum();

                to_delete.iter().for_each(|object| {
                    event!(
                        Level::INFO,
                        "To delete on destination bucket: {}/{} - {}",
                        async_conf.destination_bucket,
                        object.get_key(),
                        ByteSize(object.get_size())
                    )
                });

                event!(
                    Level::INFO,
                    "Current delete status: {} objects to delete for a total size of {}",
                    total_files_delete,
                    ByteSize(total_deleted_size as u64)
                );
            }
        }
        BucketObjectsMigrationResult::Executed(mut results) => {
            while let Some(result) = results.pop() {
                let mut result = result.unwrap();
                total_files_sync += result.sync_results.len();
                total_files_delete += result.delete_results.len();

                event!(Level::TRACE, "Synced results: {:#?}", result.sync_results);
                event!(
                    Level::TRACE,
                    "Deleted results: {:#?}",
                    result.delete_results
                );

                while let Some(res) = result.sync_results.pop() {
                    match res {
                        Ok(size) => total_synced_size += size,
                        Err(err) => {
                            event!(Level::WARN, "Failed to sync a file: {:?}", err);
                            sync_errors.push(anyhow::anyhow!(err));
                        }
                    };
                }

                event!(
                    Level::INFO,
                    "Current sync status: {} synced objects for a total size of {}",
                    total_files_sync,
                    ByteSize(total_synced_size as u64)
                );

                if conf.delete_destination_files {
                    while let Some(res) = result.delete_results.pop() {
                        match res {
                            Ok(size) => total_deleted_size += size,
                            Err(err) => {
                                event!(Level::WARN, "Failed to delete a file: {:?}", err);
                                delete_errors.push(anyhow::anyhow!(err));
                            }
                        };
                    }

                    event!(
                        Level::INFO,
                        "Current delete status: {} deleted objects for a total size of {}",
                        total_files_delete,
                        ByteSize(total_deleted_size as u64)
                    );
                }
            }
        }
    };

    if !conf.dry_run {
        if total_files_sync > 0 {
            let sync_errors = sync_errors
                .iter()
                .map(|error| {
                    format!(
                        "{} | Error synchronizing file: {:?}",
                        conf.source_bucket, error
                    )
                })
                .collect::<Vec<String>>();

            let delete_errors = delete_errors
                .iter()
                .map(|error| {
                    format!(
                        "{} | Error deleting file on destination bucket: {:?}",
                        conf.source_bucket, error
                    )
                })
                .collect::<Vec<String>>();

            let results_errors = [&sync_errors[..], &delete_errors[..]].concat();

            if !results_errors.is_empty() {
                let stats = BucketMigrationStats {
                    bucket: conf.source_bucket.clone(),
                    synchronization_time: sync_start.elapsed(),
                    synchronization_size: total_synced_size,
                    delete_size: total_deleted_size,
                    total_files_sync,
                    total_files_delete,
                };

                Err(anyhow::Error::new(BucketMigrationError {
                    errors: results_errors,
                    stats,
                }))
            } else {
                Ok(BucketMigrationStats {
                    bucket: conf.source_bucket.clone(),
                    synchronization_time: sync_start.elapsed(),
                    synchronization_size: total_synced_size,
                    delete_size: total_deleted_size,
                    total_files_sync,
                    total_files_delete,
                })
            }
        } else {
            event!(
                Level::WARN,
                "{} | No files to synchronize",
                conf.source_bucket
            );
            Ok(BucketMigrationStats {
                bucket: conf.source_bucket.clone(),
                synchronization_time: sync_start.elapsed(),
                synchronization_size: 0,
                delete_size: total_deleted_size,
                total_files_sync,
                total_files_delete,
            })
        }
    } else {
        Ok(BucketMigrationStats {
            bucket: conf.source_bucket.clone(),
            synchronization_time: sync_start.elapsed(),
            synchronization_size: total_synced_size,
            delete_size: total_deleted_size,
            total_files_sync,
            total_files_delete,
        })
    }
}

#[instrument(skip(destination_access_key, destination_secret_key), level = "debug")]
pub async fn create_destination_buckets(
    destination_endpoint: String,
    destination_access_key: String,
    destination_secret_key: String,
    destination_bucket: Option<String>,
    destination_bucket_prefix: String,
    buckets: &[String],
    dry_run: bool,
) -> anyhow::Result<()> {
    let client = RadosGW::new(
        Some(destination_endpoint.clone()),
        None,
        destination_access_key.clone(),
        destination_secret_key.clone(),
        None,
    );
    let missing_buckets = {
        let radosgw_buckets = client.list_buckets().await?;

        buckets
            .iter()
            .filter(|source_bucket| {
                let source_bucket_name =
                    format!("{}{}", destination_bucket_prefix, **source_bucket);

                !radosgw_buckets.iter().any(|radosgw_bucket| -> bool {
                    let radosgw_bucket_name = radosgw_bucket
                        .name
                        .as_ref()
                        .expect("RadosGW bucket should have a name");

                    source_bucket_name == *radosgw_bucket_name
                })
            })
            .collect::<Vec<&String>>()
    };

    for bucket in missing_buckets {
        let destination_bucket = if let Some(destination_bucket) = &destination_bucket {
            format!("{}{}", destination_bucket_prefix, destination_bucket)
        } else {
            format!("{}{}", destination_bucket_prefix, bucket)
        };

        if dry_run {
            // To know if the bucket already exists on another add-on, we can try to list its files. If it's not created, we will receive a NoSuchBucket error
            // If it is, we will receive another error
            let client_dry_run = get_provider(
                &Providers::Cellar,
                ProviderConf {
                    endpoint: Some(destination_endpoint.clone()),
                    region: None,
                    access_key: destination_access_key.clone(),
                    secret_key: destination_secret_key.clone(),
                    bucket: Some(destination_bucket.clone()),
                },
            );

            match client_dry_run.list_objects(Some(1), None).next().await {
                Some(Ok(_)) | None => {}
                Some(Err(error)) => {
                    match error.downcast::<SdkError<ListObjectsV2Error, HttpResponse>>() {
                        Ok(downcast_error) => match downcast_error.into_service_error() {
                            ListObjectsV2Error::NoSuchBucket(_) => {
                                event!(Level::INFO, "DRY-RUN | Bucket {} is missing on the destination add-on. In non dry-run mode, I would create it.", destination_bucket);
                            }
                            e => {
                                bucket_already_created(&destination_bucket);
                                return Err(anyhow::Error::from(e));
                            }
                        },
                        Err(downcast) => {
                            panic!("Failed to downcast error to a RusotoError: {:?}", downcast)
                        }
                    }
                }
            };
        } else {
            event!(
                Level::INFO,
                "Bucket {} | Bucket is missing on the destination add-on. I will try to create it",
                bucket
            );

            match client.create_bucket(destination_bucket.clone()).await {
                Ok(_) => {
                    event!(
                        Level::INFO,
                        "Bucket {} | Bucket created",
                        destination_bucket
                    )
                }
                Err(error) => match error.into_service_error() {
                    CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                        event!(
                            Level::INFO,
                            "Bucket {} | Bucket created",
                            destination_bucket
                        )
                    }
                    e => {
                        bucket_already_created(&destination_bucket);
                        return Err(anyhow::Error::from(e));
                    }
                },
            };
        }
    }

    Ok(())
}

fn bucket_already_created(bucket: &str) {
    event!(Level::ERROR, "Bucket {} | Bucket can't be created because it probably has been created in another Cellar add-on, maybe by another user.", bucket);
    event!(Level::ERROR, "Please refer to https://github.com/CleverCloud/cellar-migration/#my-bucket-already-exists-on-the-destination-cluster to find a workaround");
}
