use std::str::FromStr;

use chrono::{DateTime, FixedOffset, Utc};
use hyper::{Body, Response};

use serde_derive::Deserialize;
use tracing::{event, instrument, Level};

use crate::radosgw::uploader::RADOSGW_MIN_PART_SIZE;

const ONE_MEGABYTE: f64 = (1024 * 1024) as f64;

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

impl PartialEq<rusoto_s3::Object> for ObjectContents {
    #[instrument(skip_all, level = "trace")]
    fn eq(&self, other: &rusoto_s3::Object) -> bool {
        event!(Level::TRACE, "Self: {:#?}\nOther: {:#?}", self, other);

        if other.key == Some(self.get_key()) && other.size == Some(self.get_size() as i64) {
            if other.e_tag == Some(self.get_etag()) {
                true
            } else if self.get_etag().contains('-') {
                let part_size = get_part_size_from_etag(&self.get_etag(), self.get_size() as usize);
                if part_size < RADOSGW_MIN_PART_SIZE {
                    event!(Level::WARN, "Object {} has been uploaded using multipart upload with a part size less than 5MB. Falling back to last modification date to compare objects.", self.get_key());
                    let other_date: Option<DateTime<Utc>> = other
                        .last_modified
                        .as_ref()
                        .and_then(|date| DateTime::from_str(date).ok());
                    if let Some(other_date) = other_date {
                        self.get_last_modified() < other_date
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
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
    pub last_modified: Option<DateTime<FixedOffset>>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
    pub content_length: usize,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_md5: Option<String>,
    pub expires: Option<String>,
}

impl ObjectMetadata {
    fn extract_header(response: &Response<Body>, header: &str) -> Option<String> {
        response.headers().get(header).map(|v| {
            v.to_str()
                .unwrap_or_else(|_| panic!("{} header should be a valid string", header))
                .to_string()
        })
    }

    pub fn etag_has_parts(&self) -> bool {
        self.etag.as_ref().and_then(|etag| etag.find('-')).is_some()
    }
}

impl From<Response<Body>> for ObjectMetadata {
    fn from(response: Response<Body>) -> Self {
        ObjectMetadata {
            last_modified: response.headers().get("last-modified").map(|lm| {
                DateTime::parse_from_rfc2822(
                    lm.to_str()
                        .expect("Last Modified header should be a valid string"),
                )
                .expect("Last Modified header should be a valid UTC date")
            }),
            etag: Self::extract_header(&response, "etag").map(|etag| etag.replace("\"", "")),
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
            expires: Self::extract_header(&response, "expires"),
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
        self.buckets.bucket.clone().unwrap_or_else(Vec::new)
    }
}

pub fn get_part_size_from_etag(etag: &str, object_size: usize) -> usize {
    // See https://teppen.io/2018/06/23/aws_s3_etags/ for the calcul explanation
    let etag_parts = etag
        .replace("\"", "")
        .split('-')
        .into_iter()
        .last()
        .and_then(|etag_nbr| etag_nbr.parse::<usize>().ok())
        .unwrap_or_else(|| panic!("ETag should contain a part that's a usize, got {}", etag));

    let raw_part_size = object_size as f64 / etag_parts as f64;

    let part_modulus = raw_part_size % ONE_MEGABYTE;
    ((raw_part_size + ONE_MEGABYTE) - part_modulus) as usize
}
