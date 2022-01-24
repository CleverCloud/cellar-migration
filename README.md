# Cellar migration tool

A CLI tool to migrate from one cellar-c1 bucket to a cellar-c2 bucket on Clever Cloud. It is best to run it on a machine with a high network bandwidth.

This is an `rsync` like tool that will synchronize your buckets. You can start it in a loop and it will only synchronize objects that are different between the two buckets.

## Installation

You can download pre-built binaries in the [Releases section](https://github.com/CleverCloud/cellar-c1-migration-tool/releases). Supported platforms are Linux, Mac OS and Windows.

If your platform isn't supported or you prefer to compile it yourself, you can install [Rust](https://www.rust-lang.org/) and run as your regular user:

```
cargo install --git https://github.com/CleverCloud/cellar-c1-migration-tool --tag v1.0.4
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

You also have an option to specify the number of synchronization threads to use (default to the number of cores available) and a `--execute` flag to actually synchronize. By default,
it will only run in a dry mode and list files that need to be synchronized.

You can also configure the multipart chunk size if needed, by default it is 100MB.