mod migrate;
mod provider;
mod radosgw;
mod riakcs;

use bytesize::ByteSize;
use clap::{value_parser, ArgAction};
use clap::{Arg, ArgMatches, Command};
use migrate::BucketMigrationConfiguration;
use tracing::event;
use tracing::instrument;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;
use url::Url;

use crate::migrate::{BucketMigrationError, BucketMigrationStats};
use crate::provider::ProviderConf;
use crate::provider::{get_provider, Providers};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var(EnvFilter::DEFAULT_ENV)
                .map(|_| EnvFilter::from_default_env())
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_span_events(FmtSpan::CLOSE | FmtSpan::NEW)
        .with_test_writer()
        .try_init();

    let clap = clap::command!()
        .arg_required_else_help(true)
        .subcommand(
            Command::new("migrate")
            .about("Migrate a bucket to a Cellar cluster. By default, it will dry run unless --execute is passed")
            .arg(Arg::new("source-bucket").long("source-bucket").help("Source bucket from which files will be copied. If omitted, all buckets of the add-on will be synchronized"))
            .arg(Arg::new("source-access-key").long("source-access-key").help("Source bucket Cellar access key").required(true))
            .arg(Arg::new("source-secret-key").long("source-secret-key").help("Source bucket Cellar secret key").required(true))
            .arg(Arg::new("source-endpoint").long("source-endpoint").help("Source endpoint of the S3 Bucket").value_parser(value_parser!(Url)))
            .arg(Arg::new("source-provider").long("source-provider").help("Provider for source bucket (AWS, Ceph, RiakCS, ..)").required(true))
            .arg(Arg::new("source-region").long("source-region").help("Region of the source bucket (eu-west-1,..)"))
            .arg(Arg::new("destination-bucket").long("destination-bucket").help("Destination bucket to which the files will be copied. If omitted, the bucket will be created if it doesn't exist"))
            .arg(Arg::new("destination-bucket-prefix").long("destination-bucket-prefix").help("Prefix to apply to the destination bucket name"))
            .arg(Arg::new("destination-access-key").long("destination-access-key").help("Destination bucket Cellar access key").required(true))
            .arg(Arg::new("destination-secret-key").long("destination-secret-key").help("Destination bucket Cellar secret key").required(true))
            .arg(Arg::new("destination-endpoint").long("destination-endpoint").help("Destination endpoint of the Cellar cluster. Defaults to Paris Cellar cluster")
                .required(false).default_value("https://cellar-c2.services.clever-cloud.com").value_parser(value_parser!(Url))
            )
            .arg(
                Arg::new("threads").long("threads").short('t').help("Number of threads used to synchronize this bucket")
                .required(false).value_parser(value_parser!(usize))
            )
            .arg(
                Arg::new("multipart-chunk-size-mb").long("multipart-chunk-size-mb")
                .help("Size of each chunk of multipart upload in Megabytes. Files bigger than this size are automatically uploaded using multipart upload")
                .required(false).value_parser(value_parser!(usize)).default_value("100")
            )
            .arg(
                Arg::new("execute").long("execute").short('e')
                .help("Execute the synchronization. THIS COMMAND WILL MAKE PRODUCTION CHANGES TO THE DESTINATION BUCKET.")
                .action(ArgAction::SetTrue)
            )
            .arg(
                Arg::new("max-keys").long("max-keys").short('m')
                .help("Define the maximum number of object keys to list when listing the bucket. Lowering this might help listing huge buckets")
                .required(false).value_parser(value_parser!(usize)).default_value("1000")
            )
            /* .arg(
                Arg::new("delete").long("delete").short('d')
                .help("Delete extraneous files from destination bucket")
                .action(ArgAction::SetTrue)
            )*/
        )
        .get_matches();

    match clap.subcommand() {
        Some(("migrate", migrate_matches)) => migrate_command(migrate_matches).await,
        e => unreachable!("Failed to parse subcommand: {:#?}", e),
    }
}

#[instrument(skip_all, level = "debug")]
async fn migrate_command(params: &ArgMatches) -> anyhow::Result<()> {
    let dry_run = params.get_one::<bool>("execute") == Some(&false);

    if dry_run {
        event!(Level::WARN, "Running in dry run mode. No changes will be made. If you want to synchronize for real, use --execute");
    }

    unsafe { increase_limits() }?;

    let sync_threads: usize = *params
        .get_one::<usize>("threads")
        .unwrap_or(&num_cpus::get());
    let multipart_upload_chunk_size: usize = params
        .get_one::<usize>("multipart-chunk-size-mb")
        .expect("Multipart chunk size should be a usize")
        * 1024
        * 1024;
    let max_keys: usize = *params
        .get_one("max-keys")
        .expect("max-keys should be a usize");

    //let delete_destination_files = params.get_one::<bool>("delete") == Some(&true);
    let delete_destination_files = false;

    let source_bucket: Option<String> = params
        .get_one("source-bucket")
        .map(|s: &String| s.to_owned());
    let source_access_key: String = params
        .get_one::<String>("source-access-key")
        .unwrap()
        .to_string();
    let source_secret_key: String = params
        .get_one::<String>("source-secret-key")
        .unwrap()
        .to_string();
    let source_endpoint = params.get_one::<Url>("source-endpoint").map(Url::to_string);
    let source_region = params
        .get_one::<String>("source-region")
        .map(|s| s.to_owned());

    let source_provider = params
        .get_one::<String>("source-provider")
        .ok_or("Missing source provider".to_string())
        .and_then(|s| Providers::try_from(s.as_str()))
        .unwrap();

    let destination_bucket = params
        .get_one::<String>("destination-bucket")
        .map(|s| s.as_str().to_string());
    let destination_bucket_prefix = params
        .get_one::<String>("destination-bucket-prefix")
        .map(|b| format!("{}-", b))
        .unwrap_or_default();
    let destination_access_key = params
        .get_one::<String>("destination-access-key")
        .unwrap()
        .to_string();
    let destination_secret_key = params
        .get_one::<String>("destination-secret-key")
        .unwrap()
        .to_string();
    let destination_endpoint = params
        .get_one::<Url>("destination-endpoint")
        .map(Url::to_string)
        .unwrap()
        .to_string();

    if source_bucket.is_none() && destination_bucket.is_some() {
        event!(Level::ERROR, "You can't give a destination bucket without a source bucket. Please specify the --source-bucket option");
        std::process::exit(1);
    }

    match (&source_endpoint, &source_region) {
        (None, None) => {
            event!(
                Level::ERROR,
                "You have to define either --source-endpoint or --source-region"
            );
            std::process::exit(1);
        }
        (Some(_), None) => {
            if let Providers::AwsS3 = source_provider {
                event!(
                    Level::ERROR,
                    "For source-provider aws-s3, you need to specify --source-region as well"
                );
                std::process::exit(1);
            }
        }
        _ => {}
    };

    let sync_start = std::time::Instant::now();

    let source_provider_conf = ProviderConf::new(
        source_endpoint.clone(),
        source_region.clone(),
        source_access_key.clone(),
        source_secret_key.clone(),
        None,
    );

    let buckets_to_migrate = if let Some(bucket) = source_bucket.as_ref() {
        event!(Level::INFO, "Only bucket {} will be migrated", bucket);
        vec![bucket.clone()]
    } else {
        event!(
            Level::INFO,
            "All buckets of this Cellar add-ons will be migrated"
        );

        get_provider(&source_provider, source_provider_conf)
            .get_buckets()
            .await?
    };

    // First make sure the destination buckets exist / can be created
    // If not, exit now
    if let Err(error) = migrate::create_destination_buckets(
        destination_endpoint.clone(),
        destination_access_key.clone(),
        destination_secret_key.clone(),
        destination_bucket.clone(),
        destination_bucket_prefix.clone(),
        &buckets_to_migrate,
        dry_run,
    )
    .await
    {
        event!(
            Level::ERROR,
            "Error while creating destination buckets. Error = {:?}. Aborting now.",
            error
        );
        std::process::exit(1);
    }

    let mut migration_results = Vec::with_capacity(buckets_to_migrate.len());

    for bucket in &buckets_to_migrate {
        if dry_run {
            event!(
                Level::INFO,
                "DRY-RUN | Bucket {} | Starting listing of files that need to be synchronized",
                bucket
            );
        } else {
            event!(
                Level::INFO,
                "Bucket {} | Starting migration of bucket",
                bucket
            );
        }

        let destination_bucket = if source_bucket.is_some() {
            if buckets_to_migrate.len() == 1 {
                destination_bucket.as_ref().unwrap_or(bucket)
            } else {
                panic!(
                    "We can't have a source bucket specified but with multiple buckets to migrate"
                );
            }
        } else {
            bucket
        };

        event!(
            Level::DEBUG,
            "Bucket {} | Starting synchronization of bucket with destination bucket {}",
            bucket,
            destination_bucket
        );

        let bucket_migration = BucketMigrationConfiguration {
            source_bucket: bucket.clone(),
            source_access_key: source_access_key.clone(),
            source_secret_key: source_secret_key.clone(),
            source_endpoint: source_endpoint.clone(),
            source_region: source_region.clone(),
            source_provider: source_provider.clone(),
            destination_bucket: format!("{}{}", destination_bucket_prefix, destination_bucket),
            destination_access_key: destination_access_key.clone(),
            destination_secret_key: destination_secret_key.clone(),
            destination_endpoint: destination_endpoint.clone(),
            delete_destination_files,
            max_keys,
            chunk_size: multipart_upload_chunk_size,
            sync_threads,
            dry_run,
        };

        event!(
            Level::TRACE,
            "Bucket {} | Bucket Migration Configuration: {:#?}",
            bucket,
            bucket_migration
        );

        let migration_result = migrate::migrate_bucket(bucket_migration).await;

        event!(
            Level::TRACE,
            "Bucket {} | Migration result: {:#?}",
            bucket,
            migration_result
        );

        if !dry_run {
            event!(
                Level::INFO,
                "Bucket {} | Bucket has been synchronized",
                bucket
            );
        }

        migration_results.push(migration_result);
    }

    if dry_run {
        let all_stats = migration_results
            .iter()
            .filter_map(|result| match result {
                Ok(stats) => Some(stats),
                Err(error) => error
                    .downcast_ref::<BucketMigrationError>()
                    .map(|err| &err.stats),
            })
            .collect::<Vec<&BucketMigrationStats>>();

        let total_sync_bytes = all_stats
            .iter()
            .fold(0, |acc, stats| acc + stats.synchronization_size);

        let total_objects_sync: usize = all_stats
            .iter()
            .fold(0, |acc, stats| acc + stats.total_files_sync);

        event!(
            Level::INFO,
            "Total files to sync: {} for a total of {}",
            total_objects_sync,
            ByteSize(total_sync_bytes as u64)
        );

        if delete_destination_files {
            let total_objects_delete: usize = all_stats
                .iter()
                .fold(0, |acc, stats| acc + stats.total_files_delete);

            let total_delete_bytes: usize = all_stats
                .iter()
                .fold(0, |acc, stats| acc + stats.delete_size);

            event!(
                Level::INFO,
                "Total files to delete: {} for a total of {}",
                total_objects_delete,
                ByteSize(total_delete_bytes as u64)
            );
        }
    }

    let elapsed = sync_start.elapsed();

    for (index, migration_result) in migration_results.iter().enumerate() {
        let bucket = buckets_to_migrate
            .get(index)
            .expect("Bucket should be at index");

        if let Err(error) = migration_result {
            if let Some(err) = error.downcast_ref::<BucketMigrationError>() {
                for f in &err.errors {
                    event!(Level::ERROR, "Bucket {} | {}", bucket, f);
                }
            } else {
                event!(
                    Level::ERROR,
                    "Bucket {} | Error during synchronization: {:#?}",
                    bucket,
                    error
                );
            }
        }
    }

    let synchronization_size = migration_results.iter().fold(0, |acc, migration_result| {
        let stats = match migration_result {
            Ok(stats) => Some(stats),
            Err(error) => error
                .downcast_ref::<BucketMigrationError>()
                .map(|error| &error.stats),
        };

        if let Some(bucket_stats) = stats {
            acc + bucket_stats.synchronization_size
        } else {
            acc
        }
    });

    if dry_run {
        event!(Level::INFO, "Dry run files diff took {:?}", elapsed,);
    } else {
        event!(
            Level::INFO,
            "Sync took {:?} for {} ({}/s)",
            elapsed,
            ByteSize(synchronization_size as u64),
            ByteSize((synchronization_size as f64 / elapsed.as_secs_f64()) as u64)
        );
    }

    Ok(())
}

unsafe fn increase_limits() -> anyhow::Result<()> {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) < 0 {
        let error = libc::strerror(*libc::__errno_location());
        let len = libc::strlen(error);
        let error_msg = String::from_utf8(Vec::from_raw_parts(error as _, len, len))
            .expect("We should have an UTF-8 error");
        anyhow::bail!(format!(
            "Failed to get file descriptors limit: {}",
            error_msg
        ));
    }

    event!(
        Level::DEBUG,
        "Current file descriptors limit: {}, max limit: {}",
        limits.rlim_cur,
        limits.rlim_max
    );

    if limits.rlim_cur < limits.rlim_max {
        limits.rlim_cur = limits.rlim_max;
        event!(
            Level::DEBUG,
            "Increasing file descriptors limit from {} to {}",
            limits.rlim_cur,
            limits.rlim_max
        );

        if libc::setrlimit(libc::RLIMIT_NOFILE, &limits) < 0 {
            let error = libc::strerror(*libc::__errno_location());
            let len = libc::strlen(error);
            let error_msg = String::from_utf8(Vec::from_raw_parts(error as _, len, len))
                .expect("We should have an UTF-8 error");
            anyhow::bail!(format!(
                "Failed to increase file descriptors limit: {}",
                error_msg
            ));
        }
    }

    Ok(())
}
