use crate::config::TestConfig;
use std::io::{self, Read, Write};
use std::process::{Command, ExitStatus, Stdio};

/// Result from running the migration CLI
#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub exit_status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
    pub files_to_sync: Option<usize>,
    pub files_to_delete: Option<usize>,
}

impl MigrationResult {
    pub fn success(&self) -> bool {
        self.exit_status.success()
    }

    pub fn code(&self) -> Option<i32> {
        self.exit_status.code()
    }

    /// Parse the migration output to extract stats
    fn parse_output(stdout: &str, stderr: &str) -> (Option<usize>, Option<usize>) {
        let combined = format!("{}\n{}", stdout, stderr);

        let files_to_sync = Self::extract_sync_count(&combined);
        let files_to_delete = Self::extract_delete_count(&combined);

        (files_to_sync, files_to_delete)
    }

    fn extract_sync_count(output: &str) -> Option<usize> {
        // Look for patterns like:
        // "2025-10-22T15:23:19.010946Z  INFO cellar_migration: Current sync status: 5 objects to sync for a total size of..."
        // "2025-10-22T15:23:19.010946Z  INFO cellar_migration: Current sync status: 0 synced objects for a total size of..."
        // "| No files to synchronize" (when 0 files need syncing)
        for line in output.lines() {
            if line.contains("sync status:") {
                if let Some(count_str) = line.split_whitespace().nth(7) {
                    if let Ok(count) = count_str.parse::<usize>() {
                        return Some(count);
                    }
                }
            } else if line.contains("No files to synchronize") {
                // When there are 0 files to sync, this message is shown instead
                return Some(0);
            }
        }
        None
    }

    fn extract_delete_count(output: &str) -> Option<usize> {
        // Look for patterns like:
        // "Current delete status: 2 objects to delete for a total size of..."
        // "Current delete status: 0 deleted objects for a total size of..."
        for line in output.lines() {
            if line.contains("delete status:") {
                if let Some(count_str) = line.split_whitespace().nth(3) {
                    if let Ok(count) = count_str.parse::<usize>() {
                        return Some(count);
                    }
                }
            }
        }
        None
    }
}

/// Get the path to a built binary, avoiding recompilation
pub fn get_binary_path(binary_name: &str) -> Result<String, std::env::VarError> {
    let env_var = format!("CARGO_BIN_EXE_{}", binary_name.replace('-', "_"));
    std::env::var(&env_var).or_else(|_| -> Result<String, std::env::VarError> {
        // Fallback: look in target/debug
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
        Ok(format!("{}/target/debug/{}", manifest_dir, binary_name))
    })
}

#[derive(Debug, Clone)]
pub struct MigrationOptions {
    pub chunk_size_mb: usize,
    pub thread_count: usize,
    pub max_keys: Option<usize>,
    pub execute: bool,
    pub preserve_version_ids: bool,
    pub preserve_last_modified_timestamps: bool,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            chunk_size_mb: 100,
            thread_count: 4,
            max_keys: Some(1000),
            execute: true,
            preserve_version_ids: false,
            preserve_last_modified_timestamps: false,
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

    pub fn preserve_version_ids(mut self, value: bool) -> Self {
        self.preserve_version_ids = value;
        self
    }

    pub fn preserve_last_modified_timestamps(mut self, value: bool) -> Self {
        self.preserve_last_modified_timestamps = value;
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
) -> Result<MigrationResult, Box<dyn std::error::Error>> {
    // Use the already-built binary instead of cargo run to avoid recompilation
    let binary_path = get_binary_path("cellar-migration")
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    let mut cmd = Command::new(&binary_path);

    let mut args = vec![
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

    if options.preserve_version_ids {
        args.push("--preserve-version-ids".to_string());
    }

    if options.preserve_last_modified_timestamps {
        args.push("--preserve-last-modified-timestamps".to_string());
    }

    cmd.args(&args);
    cmd.env("RUST_LOG", "cellar_migration=info");

    // Capture stdout and stderr from child process
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let nocapture_enabled = is_nocapture_enabled();

    if nocapture_enabled {
        println!("launching cli: {} {}", binary_path, args.join(" "));

        let mut child = cmd.spawn()?;
        let mut child_stdout = child.stdout.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stdout")
        })?;
        let mut child_stderr = child.stderr.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stderr")
        })?;

        let stdout_captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let stderr_captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let stdout_captured_clone = stdout_captured.clone();
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
                stdout_captured_clone
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buffer[..read]);
            }

            Ok(())
        });

        let stderr_captured_clone = stderr_captured.clone();
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
                stderr_captured_clone
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buffer[..read]);
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

        let stdout_bytes = stdout_captured.lock().unwrap();
        let stderr_bytes = stderr_captured.lock().unwrap();
        let stdout = String::from_utf8_lossy(&stdout_bytes).to_string();
        let stderr = String::from_utf8_lossy(&stderr_bytes).to_string();

        let (files_to_sync, files_to_delete) = MigrationResult::parse_output(&stdout, &stderr);

        Ok(MigrationResult {
            exit_status: status,
            stdout,
            stderr,
            files_to_sync,
            files_to_delete,
        })
    } else {
        let output = cmd.output()?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let (files_to_sync, files_to_delete) = MigrationResult::parse_output(&stdout, &stderr);

        Ok(MigrationResult {
            exit_status: output.status,
            stdout,
            stderr,
            files_to_sync,
            files_to_delete,
        })
    }
}

/// Convenience function for basic migration with default options
/// Runs migration TWICE to test idempotency and returns both results
pub async fn run_basic_migration(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
) -> Result<(MigrationResult, MigrationResult), Box<dyn std::error::Error>> {
    run_basic_migration_with_flags(
        config,
        src_bucket,
        dst_bucket,
        chunk_size_mb,
        thread_count,
        false,
        false,
    )
    .await
}

/// Convenience function for migration with preservation flags
pub async fn run_basic_migration_with_flags(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
    preserve_version_ids: bool,
    preserve_last_modified_timestamps: bool,
) -> Result<(MigrationResult, MigrationResult), Box<dyn std::error::Error>> {
    let options = MigrationOptions::new()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count)
        .preserve_version_ids(preserve_version_ids)
        .preserve_last_modified_timestamps(preserve_last_modified_timestamps);

    // First migration run
    let first_run = run_migration_cli(config, src_bucket, dst_bucket, options.clone()).await?;

    // Second migration run for idempotency check
    let second_run = run_migration_cli(config, src_bucket, dst_bucket, options).await?;

    Ok((first_run, second_run))
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
) -> Result<MigrationResult, Box<dyn std::error::Error>> {
    let options = MigrationOptions::dry_run()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count);

    run_migration_cli(config, src_bucket, dst_bucket, options).await
}

/// Convenience function for migration with max_keys parameter
/// Runs migration TWICE to test idempotency and returns both results
pub async fn run_migration_with_max_keys(
    config: &TestConfig,
    src_bucket: &str,
    dst_bucket: &str,
    chunk_size_mb: usize,
    thread_count: usize,
    max_keys: usize,
) -> Result<(MigrationResult, MigrationResult), Box<dyn std::error::Error>> {
    let options = MigrationOptions::new()
        .chunk_size_mb(chunk_size_mb)
        .thread_count(thread_count)
        .max_keys(Some(max_keys));

    // First migration run
    let first_run = run_migration_cli(config, src_bucket, dst_bucket, options.clone()).await?;

    // Second migration run for idempotency check
    let second_run = run_migration_cli(config, src_bucket, dst_bucket, options).await?;

    Ok((first_run, second_run))
}
