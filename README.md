# Cellar migration tool

A CLI tool to migrate your object storage buckets on Clever Cloud. This tool currently supports AWS-S3 and Cellar (Clever Cloud own Object Storage service) but it should
work with any service implementing the S3 API.

This is an `rsync` like tool that will synchronize your buckets. You can start it in a loop and it will only synchronize objects that are different between the two buckets.
It is best to run it on a machine with a high network bandwidth.

## Installation

You can download pre-built binaries in the [Releases section](https://github.com/CleverCloud/cellar-migration/releases). Supported platforms are Linux, Mac OS and Windows.

If your platform isn't supported or you prefer to compile it yourself, you can install [Rust](https://www.rust-lang.org/) and run as your regular user:

```
cargo install --git https://github.com/CleverCloud/cellar-migration --tag v1.2.2
```

It should then install the binary in `$HOME/.cargo/bin`.

## Usage

To display the help:

```
./cellar-migration --help
./cellar-migration --bin migrate --help
```

To migrate a bucket, you'll want to use the `migrate` command. You'll have some required parameters to provide:
- `--source-access-key`
- `--source-secret-key`
- `--source-bucket`
- `--source-endpoint`
- `--source-provider`
- `--source-region`
- `--destination-access-key`
- `--destination-secret-key`
- `--destination-bucket`
- `--destination-endpoint`
- `--destination-bucket-prefix`

You also have an option to specify the number of synchronization threads to use (default to the number of cores available) and a `--execute` flag to actually synchronize. By default,
it will only run in a dry mode and list files that need to be synchronized.

You can also configure the multipart chunk size if needed, by default it is 100MB.

A `--delete` option exists to delete files on the remote bucket that are not on the source bucket. Be careful: if your bucket already had files before a first synchronization, then
those file will probably end up being deleted.

## Automatic migration

You can deploy it on a Clever Cloud VM to have an automatic replication. You can create a new Rust application with the following environment variables:
```
CC_CACHE_DEPENDENCIES="true"
CC_RUST_BIN="http-server"
CC_WORKER_COMMAND="cargo run --bin cellar-migration --release -- migrate --source-access-key <src_access_key> --source-secret-key <src_secret_key> --source-bucket <src_bucket> --source-endpoint <src_endpoint> --source-provider <src_provider> --source-region <src_region> --destination-access-key <dst_access_key> --destination-secret-key <dst_secret_key> --destination-bucket <dst_bucket> --destination-endpoint cellar-c2.services.clever-cloud.com --threads 16"
CC_WORKER_RESTART_DELAY="60"
PORT="8080"
```

Update the `CC_WORKER_COMAND` to match your needs. We recommand you to use at least a `L` instance to benefit from multiple CPU cores.

## My bucket already exists on the destination cluster

First make sure it is not in one of your other Cellar add-ons. If it is not, try to create it in your destination add-on. If the error persists, it means
the bucket name is already taken and that you will have to pick a different name for that bucket.

Then, to synchronize all your buckets, you will have to start a synchronization loop on your own. To do so, on Mac and Linux (should also work on Windows with WSL2), you can execute this script.

Replace the first `bucket1 bucket2 bucket3` with the buckets you want to synchronize and in the `if / elif` below, set the `destination_bucket` to the appropriate value
to override your unavailable bucket name.

Write this in a `synchronize.sh` file in your current directory (example below to migrate from S3 AWS to Cellar).

```bash
#!/usr/bin/env bash

main() {
  for bucket in bucket1 bucket2 bucket3; do
    local destination_bucket=""

    if [[ "${bucket}" == "bucket2" ]]; then
      destination_bucket="my-bucket2"
    elif [[ "${bucket}" == "bucket3" ]]; then
      destination_bucket="my-bucket3"
    else
      destination_bucket="${bucket}"
    fi

    cellar-migration migrate \
      --execute \
      --source-bucket "${bucket}" \
      --source-endpoint "s3.amazonaws.com" \
      --source-region "<eg: eu-west-1, etc>" \
      --source-provider "aws-s3" \
      --destination-bucket "${destination_bucket}" \
      --destination-endpoint "cellar-c2.services.clever-cloud.com" \
      --source-access-key "<source_key>" \
      --source-secret-key "<source_secret>" \
      --destination-access-key "<destination_key>" \
      --destination-secret-key "<destination_secret>"

  done
}

main "$@"
```

Once you wrote that into a file, `chmod +x ./synchronize.sh` and then execute it: `./synchronize.sh`