# Cellar migration tool

A CLI tool to migrate from one cellar-c1 bucket to a cellar-c2 bucket on Clever Cloud. It is best to run it on a machine with a high network bandwidth.

This is an `rsync` like tool that will synchronize your buckets. You can start it in a loop and it will only synchronize objects that are different between the two buckets.

## Installation

You can download pre-built binaries in the [Releases section](https://github.com/CleverCloud/cellar-c1-migration-tool/releases). Supported platforms are Linux, Mac OS and Windows.

If your platform isn't supported or you prefer to compile it yourself, you can install [Rust](https://www.rust-lang.org/) and run as your regular user:

```
cargo install --git https://github.com/CleverCloud/cellar-c1-migration-tool --tag v1.2.0
```

It should then install the binary in `$HOME/.cargo/bin`.

## Usage

To display the help:

```
./cellar-migration --help
./cellar-migration migrate --help
```

To migrate a bucket, you'll want to use the `migrate` command. You'll have some required parameters to provide:
- `--source-access-key`
- `--source-secret-key`
- `--source-bucket`
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


## My bucket already exists on the destination cluster

First make sure it is not in one of your other Cellar add-ons. If it is not, try to create it in your destination add-on. If the error persists, it means
the bucket name is already taken and that you will have to pick a different name for that bucket.

Then, to synchronize all your buckets, you will have to start a synchronization loop on your own. To do so, on Mac and Linux (should also work on Windows with WSL2), you can execute this script.

Replace the first `bucket1 bucket2 bucket3` with the buckets you want to synchronize and in the `if / elif` below, set the `destination_bucket` to the appropriate value
to override your unavailable bucket name.

Write this in a `synchronize.sh` file in your current directory.

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
      --source-bucket "${bucket}" \
      --destination-bucket "${destination_bucket}" \
      --source-access-key "<source_key>" \
      --source-secret-key "<source_secret>" \
      --destination-access-key "<destination_key>" \
      --destination-secret-key "<destination_secret>"

  done
}

main "$@"
```

Once you wrote that into a file, `chmod +x ./synchronize.sh` and then execute it: `./synchronize.sh`