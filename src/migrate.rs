use std::error;

use log::{debug, error, info, warn};
use rusoto_core::RusotoError;
use rusoto_s3::{CreateBucketError, ListObjectsV2Error};
use std::time::Duration;

use crate::{
    radosgw::{uploader::Uploader, RadosGW},
    riakcs::{dto::ObjectContents, RiakCS},
};

#[derive(Debug)]
pub struct BucketMigrationStats {
    pub bucket: String,
    pub synchronization_time: Duration,
    pub synchronization_size: usize,
    pub objects: Vec<ObjectContents>,
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

#[derive(Debug)]
pub struct BucketMigrationConfiguration {
    pub source_bucket: String,
    pub source_access_key: String,
    pub source_secret_key: String,
    pub source_endpoint: String,
    pub destination_bucket: String,
    pub destination_access_key: String,
    pub destination_secret_key: String,
    pub destination_endpoint: String,
    pub max_keys: usize,
    pub chunk_size: usize,
    pub sync_threads: usize,
    pub dry_run: bool,
}

pub async fn migrate_bucket(
    conf: BucketMigrationConfiguration,
) -> anyhow::Result<BucketMigrationStats> {
    let sync_start = std::time::Instant::now();

    let riak_client = RiakCS::new(
        conf.source_endpoint,
        conf.source_access_key,
        conf.source_secret_key,
        Some(conf.source_bucket.clone()),
    );

    let radosgw_client = RadosGW::new(
        conf.destination_endpoint,
        conf.destination_access_key,
        conf.destination_secret_key,
        Some(conf.destination_bucket),
    );

    debug!("riak client: {:#?}", riak_client);
    debug!("radosgw_client: {:#?}", radosgw_client);

    let mut riak_objects = riak_client.list_objects(conf.max_keys).await?;
    let radosgw_objects = match radosgw_client.list_objects(None).await {
        Ok(objects) => objects,
        Err(RusotoError::Service(ListObjectsV2Error::NoSuchBucket(_))) => {
            if conf.dry_run {
                Vec::new()
            } else {
                panic!("Unexpected error: Destination bucket doesn't exist but we tried to list its files");
            }
        }
        Err(e) => return Err(anyhow::Error::from(e)),
    };

    debug!("Riakcs objects: {}", riak_objects.len());
    debug!("Radosgw objects: {}", radosgw_objects.len());

    riak_objects.retain(|object| {
        if let Some(found) = radosgw_objects
            .iter()
            .find(|&robject| robject.key == Some(object.get_key()))
        {
            object != found
        } else {
            true
        }
    });

    if !conf.dry_run {
        if !riak_objects.is_empty() {
            let mut uploader = Uploader::new(
                riak_client,
                radosgw_client,
                riak_objects.clone(),
                conf.sync_threads,
                conf.chunk_size,
            );
            let results = uploader.sync().await;
            let results_errors: Vec<&Result<ObjectContents, anyhow::Error>> = results
                .iter()
                .flat_map(|join_result| {
                    join_result
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|&result| result.is_err())
                        .collect::<Vec<&Result<ObjectContents, anyhow::Error>>>()
                })
                .collect();

            if !results_errors.is_empty() {
                let stats = BucketMigrationStats {
                    bucket: conf.source_bucket.clone(),
                    synchronization_time: sync_start.elapsed(),
                    synchronization_size: results
                        .iter()
                        .flat_map(|join_result| {
                            join_result
                                .as_ref()
                                .unwrap()
                                .iter()
                                .filter(|result| result.is_ok())
                                .map(|result| result.as_ref().unwrap())
                                .collect::<Vec<&ObjectContents>>()
                        })
                        .fold(0, |acc, object| acc + object.get_size() as usize),
                    objects: riak_objects,
                };

                Err(anyhow::Error::new(BucketMigrationError {
                    errors: results_errors
                        .iter()
                        .map(|e| {
                            format!("{} | Error synchronizing file: {:?}", conf.source_bucket, e)
                        })
                        .collect(),
                    stats,
                }))
            } else {
                Ok(BucketMigrationStats {
                    bucket: conf.source_bucket.clone(),
                    synchronization_time: sync_start.elapsed(),
                    synchronization_size: riak_objects
                        .iter()
                        .fold(0, |acc, obj| acc + obj.get_size() as usize),
                    objects: riak_objects,
                })
            }
        } else {
            warn!("{} | No files to synchronize", conf.source_bucket);
            Ok(BucketMigrationStats {
                bucket: conf.source_bucket.clone(),
                synchronization_time: sync_start.elapsed(),
                synchronization_size: 0,
                objects: riak_objects,
            })
        }
    } else {
        Ok(BucketMigrationStats {
            bucket: conf.source_bucket.clone(),
            synchronization_time: sync_start.elapsed(),
            synchronization_size: 0,
            objects: riak_objects,
        })
    }
}

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
        destination_endpoint.clone(),
        destination_access_key.clone(),
        destination_secret_key.clone(),
        None,
    );
    let missing_buckets = {
        let radosgw_buckets = client.list_buckets().await?;

        buckets
            .iter()
            .filter(|riakcs_bucket| {
                !radosgw_buckets.iter().any(|radosgw_bucket| -> bool {
                    let radosgw_bucket_name = format!(
                        "{}{}",
                        destination_bucket_prefix,
                        radosgw_bucket
                            .name
                            .as_ref()
                            .expect("RadosGW bucket should have a name")
                    );
                    **riakcs_bucket == radosgw_bucket_name
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
            let client_dry_run = RadosGW::new(
                destination_endpoint.clone(),
                destination_access_key.clone(),
                destination_secret_key.clone(),
                Some(destination_bucket.clone()),
            );

            match client_dry_run.list_objects(Some(1)).await {
                Ok(_) | Err(RusotoError::Service(ListObjectsV2Error::NoSuchBucket(_))) => {
                    info!("DRY-RUN | Bucket {} is missing on the destination add-on. In non dry-run mode, I would create it.", destination_bucket);
                }
                Err(e) => {
                    bucket_already_created(&destination_bucket);
                    return Err(anyhow::Error::from(e));
                }
            }
        } else {
            info!(
                "Bucket {} | Bucket is missing on the destination add-on. I will try to create it",
                bucket
            );

            match client.create_bucket(destination_bucket.clone()).await {
                Ok(_)
                | Err(RusotoError::Service(CreateBucketError::BucketAlreadyOwnedByYou(_))) => {
                    info!("Bucket {} | Bucket created", destination_bucket)
                }
                Err(e) => {
                    bucket_already_created(&destination_bucket);
                    return Err(anyhow::Error::from(e));
                }
            }
        }
    }

    Ok(())
}

fn bucket_already_created(bucket: &str) {
    error!("Bucket {} | Bucket can't be created because it probably has been created in another Cellar add-on, maybe by another user.", bucket);
    error!("Please refer to https://github.com/CleverCloud/cellar-c1-migration-tool/#my-bucket-already-exists-on-the-destination-cluster to find a workaround");
}
