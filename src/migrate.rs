use std::error;

use bytesize::ByteSize;
use log::{debug, info, warn};
use std::time::Duration;

use crate::{
    radosgw::{uploader::Uploader, RadosGW},
    riakcs::{dto::ObjectContents, RiakCS},
};

#[derive(Debug)]
pub struct BucketMigrationStats {
    pub synchronization_time: Duration,
    pub synchronization_size: usize,
}

#[derive(Debug)]
pub struct BucketMigrationError {
    pub bucket: String,
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
        conf.source_bucket.clone(),
    );

    let radosgw_client = RadosGW::new(
        conf.destination_endpoint,
        conf.destination_access_key,
        conf.destination_secret_key,
        conf.destination_bucket,
    );

    let mut riak_objects = riak_client.list_objects(conf.max_keys).await?;
    let radosgw_objects = radosgw_client.list_objects().await?;

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

    info!(
        "Those objects need to be sync: {:#?}",
        riak_objects
            .iter()
            .map(|object| { format!("{} - {}", object.get_key(), ByteSize(object.get_size())) })
            .collect::<Vec<String>>()
    );

    debug!("Objects to sync: {:#?}", riak_objects);

    let total_sync_bytes = riak_objects
        .iter()
        .fold(0, |acc, object| acc + object.get_size() as u64);
    info!(
        "{} | Total files to sync: {} for a total of {}",
        conf.source_bucket,
        riak_objects.len(),
        ByteSize(total_sync_bytes)
    );

    if !conf.dry_run {
        if !riak_objects.is_empty() {
            let mut uploader = Uploader::new(
                riak_client,
                radosgw_client,
                riak_objects,
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
                };

                Err(anyhow::Error::new(BucketMigrationError {
                    bucket: conf.source_bucket.clone(),
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
                    synchronization_time: sync_start.elapsed(),
                    synchronization_size: total_sync_bytes as usize,
                })
            }
        } else {
            warn!("{} | No files to synchronize", conf.source_bucket);
            Ok(BucketMigrationStats {
                synchronization_time: sync_start.elapsed(),
                synchronization_size: 0,
            })
        }
    } else {
        Ok(BucketMigrationStats {
            synchronization_time: sync_start.elapsed(),
            synchronization_size: 0,
        })
    }
}
