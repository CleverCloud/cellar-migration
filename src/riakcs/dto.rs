use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use hyper::{body::Incoming, Response};

use serde_derive::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct ObjectContents {
    #[serde(rename(deserialize = "Key"))]
    key: String,
    #[serde(rename(deserialize = "LastModified"))]
    last_modified: String,
    #[serde(rename(deserialize = "ETag"))]
    etag: String,
    #[serde(rename(deserialize = "Size"))]
    size: u64,
}

impl ObjectContents {
    pub fn get_key(&self) -> String {
        self.key.clone()
    }

    #[allow(dead_code)]
    pub fn get_last_modified(&self) -> DateTime<Utc> {
        DateTime::from_str(&self.last_modified).expect("Should be a valid LastModified")
    }

    pub fn get_etag(&self) -> String {
        self.etag.clone()
    }

    pub fn get_size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ListObjectResponse {
    #[serde(rename(deserialize = "Name"))]
    name: String,
    #[serde(rename(deserialize = "Contents"))]
    contents: Option<Vec<ObjectContents>>,
    #[serde(rename(deserialize = "IsTruncated"))]
    truncated: bool,
}

impl ListObjectResponse {
    #[allow(dead_code)]
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_objects(&self) -> Vec<ObjectContents> {
        self.contents.clone().unwrap_or_default()
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

#[derive(Debug, Clone)]
pub struct ObjectMetadataResponse {
    pub acl_public: bool,
    pub metadata: ObjectMetadata,
}

impl ObjectMetadataResponse {
    pub fn new(metadata: ObjectMetadata, acl_public: bool) -> ObjectMetadataResponse {
        ObjectMetadataResponse {
            acl_public,
            metadata,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub last_modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_md5: Option<String>,
    pub expires: Option<DateTime<Utc>>,
}

impl ObjectMetadata {
    fn extract_header(response: &Response<Incoming>, header: &str) -> Option<String> {
        response.headers().get(header).map(|v| {
            v.to_str()
                .unwrap_or_else(|_| panic!("{} header should be a valid string", header))
                .to_string()
        })
    }
}

impl From<Response<Incoming>> for ObjectMetadata {
    fn from(response: Response<Incoming>) -> Self {
        ObjectMetadata {
            last_modified: response.headers().get("last-modified").map(|lm| {
                let date = DateTime::parse_from_rfc2822(
                    lm.to_str()
                        .expect("Last Modified header should be a valid string"),
                )
                .expect("Last Modified header should be a valid UTC date");
                Utc.from_utc_datetime(&date.naive_utc())
            }),
            etag: Self::extract_header(&response, "etag").map(|etag| etag.replace('\"', "")),
            content_type: Self::extract_header(&response, "content-type"),
            content_length: Self::extract_header(&response, "content-length")
                .map(|ct| {
                    ct.parse::<usize>()
                        .expect("Content-Length header should be a valid usize")
                })
                .expect("Content-Length header should be present"),
            cache_control: Self::extract_header(&response, "cache-control"),
            content_disposition: Self::extract_header(&response, "content-disposition"),
            content_encoding: Self::extract_header(&response, "content-encoding"),
            content_language: Self::extract_header(&response, "content-language"),
            content_md5: Self::extract_header(&response, "content-md5"),
            expires: Self::extract_header(&response, "expires").map(|lm| {
                let date = DateTime::parse_from_rfc2822(&lm)
                    .expect("Last Modified header should be a valid UTC date");
                Utc.from_utc_datetime(&date.naive_utc())
            }),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ListBucket {
    #[serde(rename(deserialize = "Name"))]
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct ListBuckets {
    #[serde(rename(deserialize = "Bucket"))]
    pub bucket: Option<Vec<ListBucket>>,
}

#[derive(Debug, Deserialize)]
pub struct ListBucketsResult {
    #[serde(rename(deserialize = "Buckets"))]
    pub buckets: ListBuckets,
}

impl ListBucketsResult {
    pub fn get_buckets(&self) -> Vec<ListBucket> {
        self.buckets.bucket.clone().unwrap_or_default()
    }
}
