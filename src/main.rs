mod radosgw;
mod riakcs;

use bytesize::ByteSize;
use clap::{App, AppSettings, Arg, ArgMatches};
use log::LevelFilter;
use log::{debug, error, info, warn};
use radosgw::RadosGW;
use riakcs::RiakCS;

use crate::radosgw::uploader::Uploader;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_default_env()
        .default_format()
        .filter_level(LevelFilter::Info)
        .init();

    let num_cpus = num_cpus::get();
    let clap = clap::app_from_crate!()
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(
            App::new("migrate")
            .about("Migrate a cellar-c1 bucket to a cellar-c2 cluster. By default, it will dry run unless --execute is passed")
            .arg(Arg::new("source-bucket").long("source-bucket").help("Source bucket from which files will be copied").required(true).takes_value(true))
            .arg(Arg::new("source-access-key").long("source-access-key").help("Source bucket Cellar access key").required(true).takes_value(true))
            .arg(Arg::new("source-secret-key").long("source-secret-key").help("Source bucket Cellar secret key").required(true).takes_value(true))
            .arg(Arg::new("destination-bucket").long("destination-bucket").help("Destination bucket to which the files will be copied").required(true).takes_value(true))
            .arg(Arg::new("destination-access-key").long("destination-access-key").help("Destination bucket Cellar access key").required(true).takes_value(true))
            .arg(Arg::new("destination-secret-key").long("destination-secret-key").help("Destination bucket Cellar secret key").required(true).takes_value(true))
            .arg(Arg::new("destination-endpoint").long("destination-endpoint").help("Destination endpoint of the Cellar cluster. Defaults to Paris Cellar cluster")
                .required(false).takes_value(true).default_value("cellar-c2.services.clever-cloud.com")
            )
            .arg(
                Arg::new("threads").long("threads").short('t').help("Number of threads used to synchronize this bucket")
                .required(false).takes_value(true).default_value(&num_cpus.to_string())
            )
            .arg(
                Arg::new("multipart-chunk-size-mb").long("multipart-chunk-size-mb")
                .help("Size of each chunk of multipart upload in Megabytes. Files bigger than this size are automatically uploaded using multipart upload")
                .required(false).takes_value(true).default_value("100")
            )
            .arg(
                Arg::new("execute").long("execute").short('e')
                .help("Execute the synchronization. THIS COMMAND WILL MAKE PRODUCTION CHANGES TO THE DESTINATION BUCKET.")
                .required(false).takes_value(false)
            )
            .arg(
                Arg::new("max-keys").long("max-keys").short('m')
                .help("Define the maximum number of object keys to list when listing the bucket. Lowering this might help listing huge buckets")
                .required(false).takes_value(true).default_value("1000")
            )
        )
        .get_matches();

    match clap.subcommand() {
        Some(("migrate", migrate_matches)) => migrate(migrate_matches).await,
        e => unreachable!("Failed to parse subcommand: {:#?}", e),
    }
}

async fn migrate(params: &ArgMatches) -> anyhow::Result<()> {
    let dry_run = params.occurrences_of("execute") == 0;

    if dry_run {
        warn!("Running in dry run mode. No changes will be made. If you want to synchronize for real, use --execute");
    }

    let sync_threads = params
        .value_of_t("threads")
        .expect("Threads should be a usize");
    let multipart_upload_chunk_size: usize = params
        .value_of_t::<usize>("multipart-chunk-size-mb")
        .expect("Multipart chunk size should be a usize")
        * 1024
        * 1024;
    let max_keys = params
        .value_of_t::<usize>("max-keys")
        .expect("max-keys should be a usize");
    let sync_start = std::time::Instant::now();
    let riak_client = RiakCS::new(
        "cellar.services.clever-cloud.com".to_string(),
        params.value_of("source-access-key").unwrap().to_string(),
        params.value_of("source-secret-key").unwrap().to_string(),
        params.value_of("source-bucket").unwrap().to_string(),
    );

    let radosgw_client = RadosGW::new(
        params.value_of("destination-endpoint").unwrap().to_string(),
        params
            .value_of("destination-access-key")
            .unwrap()
            .to_string(),
        params
            .value_of("destination-secret-key")
            .unwrap()
            .to_string(),
        params.value_of("destination-bucket").unwrap().to_string(),
    );

    let mut riak_objects = riak_client.list_objects(max_keys).await?;
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

    let total_sync_bytes = riak_objects
        .iter()
        .fold(0, |acc, object| acc + object.get_size() as u64);
    info!(
        "Total files to sync: {} for a total of {}",
        riak_objects.len(),
        ByteSize(total_sync_bytes)
    );

    if !dry_run {
        if !riak_objects.is_empty() {
            let mut uploader = Uploader::new(
                riak_client,
                radosgw_client,
                riak_objects,
                sync_threads,
                multipart_upload_chunk_size,
            );
            let results = uploader.sync().await;
            let results_errors: Vec<&Result<(), anyhow::Error>> = results
                .iter()
                .flat_map(|join_result| {
                    join_result
                        .as_ref()
                        .unwrap()
                        .iter()
                        .filter(|&result| result.is_err())
                        .collect::<Vec<&Result<(), anyhow::Error>>>()
                })
                .collect();

            if !results_errors.is_empty() {
                error!("Synchronize failed for {} files", results_errors.len());
                for result in &results_errors {
                    error!("Error synchronizing file: {:?}", result);
                }

                return Err(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to synchronize {} files", results_errors.len()).as_str(),
                )));
            }
        } else {
            warn!("No files to synchronize");
        }
        let elapsed = sync_start.elapsed();
        info!(
            "Sync took {:?} for {} ({}/s)",
            elapsed,
            ByteSize(total_sync_bytes),
            ByteSize((total_sync_bytes as f64 / elapsed.as_secs_f64()) as u64)
        );
    } else {
        info!("Dry run took {:?}", sync_start.elapsed());
    }

    Ok(())
}
