//! File generation utilities for creating test files with predictable content

use chrono::{DateTime, Utc};
use md5::Context;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Represents a test file with its metadata
#[derive(Debug, Clone)]
pub struct TestFile {
    pub path: PathBuf,
    pub size: usize,
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<DateTime<Utc>>,
    pub acl_public: bool,
    pub metadata: HashMap<String, String>,
    pub expected_md5: Option<String>,
}

impl TestFile {
    pub fn new(name: &str, size: usize) -> Self {
        Self {
            path: PathBuf::from(name),
            size,
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            expires: None,
            acl_public: false,
            metadata: HashMap::new(),
            expected_md5: None,
        }
    }

    pub fn with_content_type(mut self, content_type: &str) -> Self {
        self.content_type = Some(content_type.to_string());
        self
    }

    pub fn with_cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = Some(cache_control.to_string());
        self
    }

    pub fn with_content_disposition(mut self, content_disposition: &str) -> Self {
        self.content_disposition = Some(content_disposition.to_string());
        self
    }

    pub fn with_content_encoding(mut self, content_encoding: &str) -> Self {
        self.content_encoding = Some(content_encoding.to_string());
        self
    }

    pub fn with_content_language(mut self, content_language: &str) -> Self {
        self.content_language = Some(content_language.to_string());
        self
    }

    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }

    pub fn with_public_acl(mut self) -> Self {
        self.acl_public = true;
        self
    }

    pub fn with_user_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    pub fn set_expected_md5<S: Into<String>>(&mut self, expected_md5: S) {
        self.expected_md5 = Some(expected_md5.into());
    }

    pub fn key(&self) -> String {
        self.path.to_string_lossy().to_string()
    }
}

/// File generator for creating test files with predictable content
pub struct FileGenerator {
    temp_dir: PathBuf,
}

impl FileGenerator {
    /// Create a new file generator with a temporary directory in target/tmp/
    pub fn new() -> Result<Self, std::io::Error> {
        let temp_dir = PathBuf::from("target/tmp");
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir)?;
        }
        Ok(Self { temp_dir })
    }

    /// Create a new file generator with a test-specific temporary directory
    pub fn new_for_test(test_name: &str) -> Result<Self, std::io::Error> {
        let temp_dir = PathBuf::from("target/tmp").join(test_name);
        if !temp_dir.exists() {
            fs::create_dir_all(&temp_dir)?;
        }
        Ok(Self { temp_dir })
    }

    /// Generate a file with predictable content pattern
    pub fn generate_file(&self, test_file: &TestFile) -> Result<PathBuf, std::io::Error> {
        let start_time = std::time::Instant::now();
        let file_path = self.temp_dir.join(&test_file.path);

        // Log file generation start
        if test_file.size > 1_000_000 {
            // Log for files > 1MB
            println!(
                "Generating file: {} (size: {} bytes, content_type: {:?})",
                test_file.key(),
                test_file.size,
                test_file.content_type.as_deref().unwrap_or("none")
            );
        }

        // Create parent directories if they don't exist
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = fs::File::create(&file_path)?;

        if test_file.size == 0 {
            // Empty file
            println!("Generated empty file: {}", test_file.key());
            return Ok(file_path);
        }

        // Generate predictable content based on file name and size
        let pattern = format!("Test file: {}\n", test_file.key());
        let pattern_bytes = pattern.as_bytes();
        let mut bytes_written = 0;

        while bytes_written < test_file.size {
            let remaining = test_file.size - bytes_written;
            let to_write = if remaining >= pattern_bytes.len() {
                pattern_bytes
            } else {
                &pattern_bytes[..remaining]
            };

            file.write_all(to_write)?;
            bytes_written += to_write.len();
        }

        file.flush()?;

        // Log completion for larger files
        if test_file.size > 1_000_000 {
            println!(
                "Generated file {} in {:.2}s",
                test_file.key(),
                start_time.elapsed().as_secs_f64()
            );
        }

        Ok(file_path)
    }

    /// Generate a file with custom content
    pub fn generate_file_with_content(
        &self,
        test_file: &TestFile,
        content: &[u8],
    ) -> Result<PathBuf, std::io::Error> {
        let file_path = self.temp_dir.join(&test_file.path);

        // Create parent directories if they don't exist
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(&file_path, content)?;

        if test_file.size > 1_000_000 {
            println!(
                "Generated custom content file: {} (size: {} bytes)",
                test_file.key(),
                content.len()
            );
        }

        Ok(file_path)
    }

    /// Compute the MD5 checksum of a generated file and return it as a lowercase hex string
    pub fn compute_md5(&self, file_path: &Path) -> Result<String, std::io::Error> {
        let mut file = fs::File::open(file_path)?;
        let mut context = Context::new();
        let mut buffer = [0u8; 8192];

        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            context.consume(&buffer[..read]);
        }

        let digest = context.compute();
        Ok(format!("{:x}", digest))
    }

    /// Generate multiple test files with various sizes and metadata
    pub fn generate_test_files(&self, count: usize) -> Result<Vec<TestFile>, std::io::Error> {
        println!(
            "Generating {} test files with various sizes and metadata",
            count
        );

        let sizes = [1024, 100_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000]; // 1KB to 50MB
        let content_types = [
            "text/plain",
            "application/json",
            "image/png",
            "application/octet-stream",
        ];
        let cache_controls = [
            "max-age=3600",
            "no-cache",
            "private",
            "public, max-age=86400",
        ];

        let mut test_files = Vec::new();
        let mut total_size = 0u64;

        for i in 0..count {
            let size = sizes[i % sizes.len()];
            total_size += size as u64;
            let content_type = content_types[i % content_types.len()];
            let cache_control = cache_controls[i % cache_controls.len()];

            let mut test_file = TestFile::new(&format!("test-file-{:04}.txt", i), size)
                .with_content_type(content_type)
                .with_cache_control(cache_control);

            // Add some variety in metadata
            if i % 3 == 0 {
                test_file =
                    test_file.with_content_disposition("attachment; filename=\"downloaded.txt\"");
            }
            if i % 4 == 0 {
                test_file = test_file.with_content_encoding("gzip");
            }
            if i % 5 == 0 {
                test_file = test_file.with_content_language("en-US");
            }
            test_files.push(test_file);
        }

        println!(
            "Generated {} test file definitions (total size: {:.2} MB)",
            count,
            total_size as f64 / 1_000_000.0
        );

        Ok(test_files)
    }

    /// Generate files with unicode names for edge case testing
    pub fn generate_unicode_files(&self) -> Result<Vec<TestFile>, std::io::Error> {
        println!("Generating files with Unicode names for edge case testing");

        let unicode_names = [
            "测试.txt",
            "café.json",
            "файл.bin",
            "🎉emoji.txt",
            "ñoño-español.data",
        ];

        let mut test_files = Vec::new();
        for (i, name) in unicode_names.iter().enumerate() {
            let test_file = TestFile::new(name, 1024 * (i + 1)).with_content_type("text/plain");
            println!("  Unicode file: {} (size: {} bytes)", name, 1024 * (i + 1));
            test_files.push(test_file);
        }

        println!("Generated {} Unicode test files", test_files.len());
        Ok(test_files)
    }

    /// Generate files with special characters in names
    pub fn generate_special_char_files(&self) -> Result<Vec<TestFile>, std::io::Error> {
        println!("Generating files with special characters in names");

        let special_names = [
            "file with spaces.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
            "file[with]brackets.txt",
        ];

        let mut test_files = Vec::new();
        for (i, name) in special_names.iter().enumerate() {
            let test_file =
                TestFile::new(name, 2048 * (i + 1)).with_content_type("application/octet-stream");
            println!(
                "  Special char file: {} (size: {} bytes)",
                name,
                2048 * (i + 1)
            );
            test_files.push(test_file);
        }

        println!(
            "Generated {} special character test files",
            test_files.len()
        );
        Ok(test_files)
    }

    /// Generate a large number of small files for pagination testing
    pub fn generate_pagination_files(&self, count: usize) -> Result<Vec<TestFile>, std::io::Error> {
        println!(
            "Generating {} small files for pagination testing (1KB each)",
            count
        );

        let mut test_files = Vec::new();

        for i in 0..count {
            let test_file = TestFile::new(
                &format!("pagination/file-{:06}.txt", i),
                1024, // 1KB files for pagination testing
            )
            .with_content_type("text/plain")
            .with_cache_control("max-age=3600");

            test_files.push(test_file);

            // Progress indicator for large counts
            if count > 1000 && (i + 1) % 1000 == 0 {
                println!("  Generated {} of {} pagination files", i + 1, count);
            }
        }

        println!(
            "Generated {} pagination test files (total size: {:.2} MB)",
            count,
            (count * 1024) as f64 / 1_000_000.0
        );

        Ok(test_files)
    }

    /// Clean up generated files
    pub fn cleanup(&self) -> Result<(), std::io::Error> {
        if self.temp_dir.exists() {
            fs::remove_dir_all(&self.temp_dir)?;
        }
        Ok(())
    }
}
