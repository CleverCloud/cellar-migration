use async_trait::async_trait;

use rusoto_credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials};

#[derive(Debug, Clone)]
pub struct AWSCredentialsProvider {
    access_key: String,
    private_key: String,
}

impl AWSCredentialsProvider {
    pub fn new(access_key: String, private_key: String) -> AWSCredentialsProvider {
        AWSCredentialsProvider {
            access_key,
            private_key,
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for AWSCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        Ok(AwsCredentials::new(
            self.access_key.clone(),
            self.private_key.clone(),
            None,
            None,
        ))
    }
}
