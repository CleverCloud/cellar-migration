use std::{cmp::Ordering, error};

use bytesize::ByteSize;
use futures::StreamExt;

use rusoto_core::RusotoError;
use rusoto_s3::{CreateBucketError, ListObjectsV2Error};
use std::time::Duration;
use tokio::task::JoinError;
use tracing::{event, instrument, Level};

use crate::{
    provider::{get_provider, ProviderConf, ProviderObject, Providers},
    radosgw::{
        uploader::{ThreadMigrationResult, Uploader},
        RadosGW,
    },
};

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
) -> BucketObjectsMigrationResult {
    let source_provider_conf = ProviderConf::new(
        conf.source_endpoint,
        conf.source_region,
        conf.source_access_key,
        conf.source_secret_key,
        Some(conf.source_bucket.clone()),
    );
    let source_provider = get_provider(&conf.source_provider, source_provider_conf);

    let radosgw_client = RadosGW::new(
        Some(conf.destination_endpoint),
        None,
        conf.destination_access_key,
        conf.destination_secret_key,
        Some(conf.destination_bucket),
    );
    let objects_to_migrate: Vec<ProviderObject> = src_objects
        .iter()
        .filter_map(|object| {
            if let Some(found) = dst_objects.iter().find(|d| d.get_key() == object.get_key()) {
                if object != found {
                    Some(object.clone())
                } else {
                    None
                }
            } else {
                Some(object.clone())
            }
        })
        .collect();

    let objects_to_delete: Vec<ProviderObject> = if conf.delete_destination_files {
        dst_objects
            .iter()
            .filter_map(|object| {
                if !src_objects
                    .iter()
                    .any(|src| src.get_key() == object.get_key())
                {
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
                source_provider,
                radosgw_client,
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

#[instrument(skip_all, level = "debug")]
pub async fn migrate_bucket(
    conf: BucketMigrationConfiguration,
) -> anyhow::Result<BucketMigrationStats> {
    let sync_start = std::time::Instant::now();

    let async_conf = conf.clone();
    let source_provider_conf = ProviderConf::new(
        conf.source_endpoint,
        conf.source_region,
        conf.source_access_key,
        conf.source_secret_key,
        Some(conf.source_bucket.clone()),
    );

    let dest_provider_conf = ProviderConf::new(
        Some(conf.destination_endpoint),
        None,
        conf.destination_access_key,
        conf.destination_secret_key,
        Some(conf.destination_bucket.clone()),
    );

    let source_provider = get_provider(&conf.source_provider, source_provider_conf);
    let dest_provider = get_provider(&Providers::Cellar, dest_provider_conf);

    let mut source_objects_stream = source_provider.list_objects(None, None);
    let mut dest_listing = dest_provider.list_objects(None, None);

    // Instead of listing all the files from each side and diff, fetch from both sides some files.
    // From each fetch, check that the last source file is lesser than our last destination file
    // If it is, we can start syncing the diff between the two
    // If it is not, we keep fetching destination files until it is
    // If we run out of destination files, it means we need to sync
    async {
        let mut sync_errors: Vec<anyhow::Error> = Vec::new();
        let mut delete_errors: Vec<anyhow::Error> = Vec::new();
        let mut total_synced_size: usize = 0;
        let mut total_deleted_size: usize = 0;
        let mut total_files_sync: usize = 0;
        let mut total_files_delete: usize = 0;
        let mut no_more_dst_objects = false;
        let mut dst_objects: Vec<ProviderObject> = Vec::new();

        while let Some(src_next) = source_objects_stream.next().await {
            if let Err(err) = src_next {
                event!(Level::ERROR, "Failed to fetch source objects: {:?}", err);
                anyhow::bail!(err);
            }

            let src_objects = src_next.ok().unwrap();

            event!(
                Level::DEBUG,
                "Migrate: Got source source_objects(len={}). delete_errors={}, total_synced_size={}, total_deleted_size={}, total_files_sync={}, total_files_delete={}, no_more_dst_objects={}, dst_objects={}",
                src_objects.len(),
                delete_errors.len(),
                total_synced_size,
                total_deleted_size,
                total_files_sync,
                total_files_delete,
                no_more_dst_objects,
                dst_objects.len()
            );
            event!(Level::TRACE, "Migrate: delete_errors: {:#?}", delete_errors);
            event!(Level::TRACE, "Migrate: source objects: {:#?}", src_objects);
            event!(Level::TRACE, "Migrate: dst_objects: {:#?}", dst_objects);

            if let Some(last_src) = src_objects.last() {
                let mut fetch_dst_objects = false;
                'inner: loop {
                    if no_more_dst_objects {
                        break;
                    }

                    if let Some(last_dst) = dst_objects.last() {
                        let order = last_src.get_key().cmp(&last_dst.get_key());
                        event!(
                            Level::DEBUG,
                            "Last src object: {}, last dst object: {}. Ordering={:?}",
                            last_src.get_key(),
                            last_dst.get_key(),
                            order
                        );
                        match order {
                            // We have fetched more destination objects than source objects
                            // So let's sync
                            Ordering::Equal | Ordering::Less => {
                                break 'inner;
                            }
                            // We haven't yet fetch enough objects on the dest side, continue
                            Ordering::Greater => {
                                fetch_dst_objects = true;
                            },
                        }
                    } else if !no_more_dst_objects {
                        fetch_dst_objects = true;
                    }

                    if fetch_dst_objects {
                        match dest_listing.next().await {
                            Some(Ok(objects)) => {
                                fetch_dst_objects = false;
                                dst_objects.extend(objects)
                            },
                            Some(Err(error)) => {
                                event!(
                                    Level::ERROR,
                                    "Failed to fetch dest objects: {:?}",
                                    error
                                );
                                anyhow::bail!(error);
                            }
                            None => {
                                no_more_dst_objects = true;
                            }
                        };
                        continue;
                    }
                }

                event!(Level::DEBUG, "Source objects: {}", src_objects.len());
                event!(Level::DEBUG, "Destination objects: {}", dst_objects.len());

                let migration_result =
                    migrate_objects(async_conf.clone(), &src_objects, &dst_objects).await;

                match migration_result {
                    BucketObjectsMigrationResult::DryRun(to_migrate, to_delete) => {
                        total_files_sync += to_migrate.len();

                        to_migrate.iter().for_each(|object| {
                            total_synced_size += object.get_size() as usize;
                            event!(
                                Level::INFO,
                                "Object to sync : {}/{} - {}",
                                async_conf.source_bucket,
                                object.get_key(),
                                ByteSize(object.get_size())
                            );

                        });

                        event!(Level::INFO,
                            "Current status: {} objects to sync for a size of {}",
                            total_files_sync,
                            ByteSize(total_synced_size as u64)
                        );

                        if async_conf.delete_destination_files {
                            total_files_delete += to_delete.len();
                            to_delete.iter().for_each(|object| {
                                event!(
                                    Level::INFO,
                                    "To delete on destination bucket: {}/{} - {}",
                                    async_conf.destination_bucket,
                                    object.get_key(),
                                    ByteSize(object.get_size())
                                )
                            });
                        }
                    }
                    BucketObjectsMigrationResult::Executed(mut results) => {
                        while let Some(result) = results.pop() {
                            let mut result = result.unwrap();
                            total_files_sync += result.sync_results.len();
                            total_files_delete += result.delete_results.len();

                            event!(Level::TRACE, "Synced results: {:#?}", result.sync_results);
                            event!(Level::TRACE, "Deleted results: {:#?}", result.delete_results);

                            while let Some(res) = result.sync_results.pop() {
                                match res {
                                    Ok(size) => total_synced_size += size,
                                    Err(err) => {
                                        event!(Level::WARN, "Failed to sync a file: {:?}", err);
                                        sync_errors.push(anyhow::anyhow!(err));
                                    }
                                };
                            }

                            while let Some(res) = result.delete_results.pop() {
                                match res {
                                    Ok(size) => total_deleted_size += size,
                                    Err(err) => {
                                        event!(Level::WARN, "Failed to delete a file: {:?}", err);
                                        delete_errors.push(anyhow::anyhow!(err));
                                    }
                                };
                            }
                        }
                    }
                };

                // Cleanup old dst objets already migrated
                dst_objects.retain(|object| {
                    !matches!(object.get_key().cmp(&last_src.get_key()), Ordering::Equal | Ordering::Less)
                });
            } else {
                // We don't have any more src objects to sync
                unreachable!("We don't have any more src objects to sync");
            }
        }

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

                let results_errors = vec![&sync_errors[..], &delete_errors[..]].concat();

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
    .await
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
            .filter(|riakcs_bucket| {
                let riakcs_bucket_name =
                    format!("{}{}", destination_bucket_prefix, **riakcs_bucket);

                !radosgw_buckets.iter().any(|radosgw_bucket| -> bool {
                    let radosgw_bucket_name = radosgw_bucket
                        .name
                        .as_ref()
                        .expect("RadosGW bucket should have a name");

                    riakcs_bucket_name == *radosgw_bucket_name
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
                Some(Err(error)) => match error.downcast::<RusotoError<_>>() {
                    Ok(RusotoError::Service(ListObjectsV2Error::NoSuchBucket(_))) => {
                        event!(Level::INFO, "DRY-RUN | Bucket {} is missing on the destination add-on. In non dry-run mode, I would create it.", destination_bucket);
                    }
                    Ok(e) => {
                        bucket_already_created(&destination_bucket);
                        return Err(anyhow::Error::from(e));
                    }
                    Err(downcast) => {
                        panic!("Failed to downcast error to a RusotoError: {:?}", downcast)
                    }
                },
            };
        } else {
            event!(
                Level::INFO,
                "Bucket {} | Bucket is missing on the destination add-on. I will try to create it",
                bucket
            );

            match client.create_bucket(destination_bucket.clone()).await {
                Ok(_)
                | Err(RusotoError::Service(CreateBucketError::BucketAlreadyOwnedByYou(_))) => {
                    event!(
                        Level::INFO,
                        "Bucket {} | Bucket created",
                        destination_bucket
                    )
                }
                Err(e) => {
                    bucket_already_created(&destination_bucket);
                    return Err(anyhow::Error::from(e));
                }
            };
        }
    }

    Ok(())
}

fn bucket_already_created(bucket: &str) {
    event!(Level::ERROR, "Bucket {} | Bucket can't be created because it probably has been created in another Cellar add-on, maybe by another user.", bucket);
    event!(Level::ERROR, "Please refer to https://github.com/CleverCloud/cellar-c1-migration-tool/#my-bucket-already-exists-on-the-destination-cluster to find a workaround");
}
