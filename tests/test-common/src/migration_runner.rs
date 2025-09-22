use crate::config::TestConfig;
use std::io::{self, Read, Write};
use std::process::{Command, ExitStatus, Stdio};

#[derive(Debug, Clone)]
pub struct MigrationOptions {
    pub chunk_size_mb: usize,
    pub thread_count: usize,
    pub max_keys: Option<usize>,
    pub execute: bool,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            chunk_size_mb: 100,
            thread_count: 4,
            max_keys: Some(1000),
            execute: true,
        }
    }
}

impl MigrationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn chunk_size_mb(mut self, size: usize) -> Self {
        self.chunk_size_mb = size;
        self
    }

    pub fn thread_count(mut self, count: usize) -> Self {
        self.thread_count = count;
        self
    }

    pub fn max_keys(mut self, max_keys: Option<usize>) -> Self {
        self.max_keys = max_keys;
        self
    }

    pub fn dry_run() -> Self {
        let mut options = Self::new();
        options.execute = false;
        options
    }
}

/// Run the migration CLI with the specified configuration and options
pub async fn run_migration_cli(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    options: MigrationOptions,
) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let mut cmd = Command::new("cargo");

    let mut args = vec![
        "run".to_string(),
        "--bin".to_string(),
        "cellar-migration".to_string(),
        "--".to_string(),
        "migrate".to_string(),
        "--source-access-key".to_string(),
        config.src_access_key.clone(),
        "--source-secret-key".to_string(),
        config.src_secret_key.clone(),
        "--source-bucket".to_string(),
        src_bucket.to_string(),
        "--source-endpoint".to_string(),
        config.src_endpoint.clone(),
        "--source-provider".to_string(),
        "cellar".to_string(),
        "--destination-access-key".to_string(),
        config.dst_access_key.clone(),
        "--destination-secret-key".to_string(),
        config.dst_secret_key.clone(),
        "--destination-bucket".to_string(),
        dst_bucket.to_string(),
        "--destination-endpoint".to_string(),
        config.dst_endpoint.clone(),
        "--multipart-chunk-size-mb".to_string(),
        options.chunk_size_mb.to_string(),
        "--threads".to_string(),
        options.thread_count.to_string(),
    ];

    if let Some(max_keys) = options.max_keys {
        args.push("--max-keys".to_string());
        args.push(max_keys.to_string());
    }

    if options.execute {
        args.push("--execute".to_string());
    }

    cmd.args(&args);
    cmd.env("RUST_LOG", "cellar_migration=info");

    // Capture stdout and stderr from child process
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let nocapture_enabled = is_nocapture_enabled();

    if nocapture_enabled {
        println!("launching cli: cargo {}", args.join(" "));

        let mut child = cmd.spawn()?;
        let mut child_stdout = child.stdout.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stdout")
        })?;
        let mut child_stderr = child.stderr.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stderr")
        })?;

        let stdout_thread = std::thread::spawn(move || -> io::Result<()> {
            let mut buffer = [0u8; 4096];
            let stdout = io::stdout();
            let mut handle = stdout.lock();

            loop {
                let read = child_stdout.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                handle.write_all(&buffer[..read])?;
                handle.flush()?;
            }

            Ok(())
        });

        let stderr_thread = std::thread::spawn(move || -> io::Result<()> {
            let mut buffer = [0u8; 4096];
            let stderr = io::stderr();
            let mut handle = stderr.lock();

            loop {
                let read = child_stderr.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                handle.write_all(&buffer[..read])?;
                handle.flush()?;
            }

            Ok(())
        });

        let status = child.wait()?;

        stdout_thread
            .join()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "stdout thread panicked"))??;
        stderr_thread
            .join()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "stderr thread panicked"))??;

        Ok(status)
    } else {
        let output = cmd.output()?;
        Ok(output.status)
    }
}

/// Convenience function for basic migration with default options
pub async fn run_basic_migration(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let options = MigrationOptions::new()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count);

    run_migration_cli(config, src_bucket, dst_bucket, options).await
}

fn is_nocapture_enabled() -> bool {
    if let Ok(value) = std::env::var("RUST_TEST_NOCAPTURE") {
        let normalized = value.trim();
        if !(normalized.is_empty() || normalized == "0" || normalized.eq_ignore_ascii_case("false"))
        {
            return true;
        }
    }

    std::env::args().any(|arg| {
        let normalized = arg.trim();
        normalized == "--nocapture" || normalized.starts_with("--nocapture=")
    })
}

/// Convenience function for dry-run migration
pub async fn run_dry_run_migration(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let options = MigrationOptions::dry_run()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count);

    run_migration_cli(config, src_bucket, dst_bucket, options).await
}

/// Convenience function for migration with max_keys parameter
pub async fn run_migration_with_max_keys(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
    max_keys: usize,
) -> Result<ExitStatus, Box<dyn std::error::Error>> {
    let options = MigrationOptions::new()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count)
        .max_keys(Some(max_keys));

    run_migration_cli(config, src_bucket, dst_bucket, options).await
}
