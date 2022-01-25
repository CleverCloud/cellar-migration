mod migrate;
mod radosgw;
mod riakcs;

use bytesize::ByteSize;
use clap::{App, AppSettings, Arg, ArgMatches};
use log::LevelFilter;
use log::{error, info, warn};
use migrate::BucketMigrationConfiguration;

use crate::migrate::BucketMigrationError;

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
        Some(("migrate", migrate_matches)) => migrate_command(migrate_matches).await,
        e => unreachable!("Failed to parse subcommand: {:#?}", e),
    }
}

async fn migrate_command(params: &ArgMatches) -> anyhow::Result<()> {
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

    let bucket_migration = BucketMigrationConfiguration {
        source_bucket: params.value_of("source-bucket").unwrap().to_string(),
        source_access_key: params.value_of("source-access-key").unwrap().to_string(),
        source_secret_key: params.value_of("source-secret-key").unwrap().to_string(),
        source_endpoint: "cellar.services.clever-cloud.com".to_string(),
        destination_bucket: params.value_of("destination-bucket").unwrap().to_string(),
        destination_access_key: params
            .value_of("destination-access-key")
            .unwrap()
            .to_string(),
        destination_secret_key: params
            .value_of("destination-secret-key")
            .unwrap()
            .to_string(),
        destination_endpoint: params.value_of("destination-endpoint").unwrap().to_string(),
        max_keys,
        chunk_size: multipart_upload_chunk_size,
        sync_threads,
        dry_run,
    };

    let migration_result = migrate::migrate_bucket(bucket_migration).await;
    let stats = match &migration_result {
        Ok(stats) => Some(stats),
        Err(error) => error
            .downcast_ref::<BucketMigrationError>()
            .map(|error| &error.stats),
    };

    if let Err(error) = &migration_result {
        if let Some(err) = error.downcast_ref::<BucketMigrationError>() {
            for f in &err.errors {
                error!("{}", f);
            }
        } else {
            error!("Error during synchronization: {:#?}", error);
        }
    }

    let elapsed = sync_start.elapsed();
    let synchronization_size = stats.as_ref().map(|s| s.synchronization_size).unwrap_or(0);
    info!(
        "Sync took {:?} for {} ({}/s)",
        elapsed,
        ByteSize(synchronization_size as u64),
        ByteSize((synchronization_size as f64 / elapsed.as_secs_f64()) as u64)
    );

    Ok(())
}
