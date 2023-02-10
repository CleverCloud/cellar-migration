# AWS S3 to Cellar migration tool

## What's this?

A CLI tool to migrate your object storage buckets on Clever Cloud. This tool currently supports AWS-S3 and Cellar (Clever Cloud own Object Storage service) but it should
work with any service implementing the S3 API.

This is an `rsync` like tool that will synchronize your buckets. You can start it in a loop and it will only synchronize objects that are different between the two buckets.
It is best to run it on a machine with a high network bandwidth.

## Running this tool locally

### Installation

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

## 💡☁️ Running this tool on Clever Cloud

![Clever Cloud logo](/assets/logo.png)

For automatic migrations. Recommended for huge buckets.

### Requirements

- A [Clever Cloud](https://www.clever-cloud.com/fr/) account
- Git or a GitHub account

### Create a Cellar addon

1. From you Clever Cloud dashboard, click on **Create** > an addon, then choose **Cellar S3 Storage**.

![create addon](/assets/create-addon.png)
![choose cellar](/assets/cellar.png)

2. From your Cellar dashboard, create a bucket. Careful, you won't be able to change its name later.
3. Cellar dashboard also contains the environement variables you'll need for running the script : `Host`, `Key_ID` and `Key_secret`. Copy them and save it somewhere safe, or download the pre-filled s3cfg file. You'll need it later.

 ![create bucket from addon dashboard](/assets/bucket.png)

ℹ️ Note: This process has been tested with an existing bucket on Clever Cloud. The CLI, theorically, create a bucket when started and connected to a Cellar addon, but it's not stable enought to be guaranteed.

### Deploy this repository

**Note**: To deploy from GitHub, you'll need to connect your GitHub account to Clever Cloud.

1. Clone this repository with `git clone git@github.com:CleverCloud/cellar-migration.git` or fork it if you'll deploy from GitHub.
2. From your Clever Cloud console, click on **Create** > an application then choose your preferred deployment method (Git ot GitHub). If deploying from GitHub, select this freshly forked repository.

    ![step](/assets/deploy.png)

3. Select a **Rust** runtime
4. Edit the **scalability** options:

  - ⚠️ disable automatic scalability
  - select minimum size of `L` and maximum size of `3XL`

5. Add the following **environment variables**:

```
CC_CACHE_DEPENDENCIES="true"
CC_RUST_BIN="http-server"
CC_WORKER_COMMAND="cargo run --bin cellar-migration --release -- migrate --source-access-key <src_access_key> --source-secret-key <src_secret_key> --source-bucket <src_bucket> --source-endpoint <src_endpoint> --source-provider <src_provider> --source-region <src_region> --destination-access-key <dst_access_key> --destination-secret-key <dst_secret_key> --destination-bucket <dst_bucket> --destination-endpoint cellar-c2.services.clever-cloud.com --threads 16"
CC_WORKER_RESTART_DELAY="60"
PORT="8080"
```

Don't forget to modifiy the `CC_WORKER_COMMAND` with your own credentials, where:

- `source` = **S3** bucket containing the objects you need to sync
- `destination` = **Cellar** bucket that will receive the objects.

This step is when you take the Cellar credentials you saved and insert it in the command options. An example of what `CC_WORKER_COMMAND`  for migrating from AWS to Clever Cloud would look like this:

```shell
--source-access-key <s3_access_key> --source-secret-key <s3_secret_key> --source-bucket <s3_bucket_name> --source-endpoint s3.amazonaws.com --source-provider aws-s3 --source-region <src_region> --destination-access-key <cellar_key_id> --destination-secret-key <cellar_key_secret> --destination-bucket <cellar_bucket_name> --destination-endpoint cellar-c2.services.clever-cloud.com --threads 16"
```

Save changes and click on **Next**. If you're deploying from GitHub, the app will automatically start. If you are using Git, copy the commands from the console to push from your repository.

### Monitor with Grafana

After your Rust app is deployed, you can monitor its activity from the Clever Cloud integrated Grafana dashboard. Access it from **Home > Metrics in Grafana > Enable / Open Grafana**.

Look for your Rust runtime and access its metrics. Watch carefully the RAM consumption, as if it goes above 80%, it means your instance is too small to run the process. You can easily edit this configuration from the console : click on your app > **Scalability** and choose a bigger instance.

ℹ️ You can also configure alerting rules within Grafana from the **Alerting** option on your Grafana dashboard.

![alerting option in Grafana menu](/assets/alerting.png)

## Cluster replicating

### My bucket already exists on the destination cluster

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