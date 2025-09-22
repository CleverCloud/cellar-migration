//! Test configuration utilities for managing test environment variables

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// Test configuration for S3 providers
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub src_access_key: String,
    pub src_secret_key: String,
    pub src_region: String,
    pub src_endpoint: String,
    pub src_path_style: bool,
    pub dst_access_key: String,
    pub dst_secret_key: String,
    pub dst_endpoint: String,
    pub dst_path_style: bool,
    pub skip_cleanup: bool,
    pub test_run_timestamp: u128,
}

impl TestConfig {
    /// Load test configuration from environment variables
    pub fn from_env() -> Result<Self, String> {
        let test_run_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("Failed to get system time: {}", e))?
            .as_millis();

        Ok(TestConfig {
            src_access_key: env::var("TEST_SRC_ACCESS_KEY")
                .map_err(|_| "TEST_SRC_ACCESS_KEY not set".to_string())?,
            src_secret_key: env::var("TEST_SRC_SECRET_KEY")
                .map_err(|_| "TEST_SRC_SECRET_KEY not set".to_string())?,
            src_region: env::var("TEST_SRC_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            src_endpoint: env::var("TEST_SRC_ENDPOINT")
                .ok()
                .unwrap_or_else(|| "https://cellar-c2.services.clever-cloud.com".to_string()),
            src_path_style: env::var("TEST_SRC_PATH_STYLE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            dst_access_key: env::var("TEST_DST_ACCESS_KEY")
                .map_err(|_| "TEST_DST_ACCESS_KEY not set".to_string())?,
            dst_secret_key: env::var("TEST_DST_SECRET_KEY")
                .map_err(|_| "TEST_DST_SECRET_KEY not set".to_string())?,
            dst_endpoint: env::var("TEST_DST_ENDPOINT")
                .unwrap_or_else(|_| "https://cellar-c2.services.clever-cloud.com".to_string()),
            dst_path_style: env::var("TEST_DST_PATH_STYLE")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            skip_cleanup: env::var("TEST_SKIP_CLEANUP")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            test_run_timestamp,
        })
    }

    /// Generate a unique bucket name for a test
    pub fn generate_bucket_name(&self, test_name: &str, suffix: &str) -> String {
        format!("test-{}-{}-{}", test_name, suffix, self.test_run_timestamp)
    }

    /// Check if all required environment variables are set
    pub fn validate_env() -> Result<(), String> {
        let required_vars = vec![
            "TEST_SRC_ACCESS_KEY",
            "TEST_SRC_SECRET_KEY",
            "TEST_DST_ACCESS_KEY",
            "TEST_DST_SECRET_KEY",
        ];

        for var in required_vars {
            if env::var(var).is_err() {
                return Err(format!("Required environment variable {} is not set", var));
            }
        }

        Ok(())
    }
}
