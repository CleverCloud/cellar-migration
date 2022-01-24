use std::str::FromStr;

use chrono::{DateTime, FixedOffset, Utc};
use hyper::{Body, Response};
use log::trace;
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
    contents: Vec<ObjectContents>,
    #[serde(rename(deserialize = "IsTruncated"))]
    truncated: bool,
}

impl ListObjectResponse {
    #[allow(dead_code)]
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_objects(&self) -> Vec<ObjectContents> {
        self.contents.clone()
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

impl PartialEq<rusoto_s3::Object> for ObjectContents {
    fn eq(&self, other: &rusoto_s3::Object) -> bool {
        trace!("Self: {:#?}\nOther: {:#?}", self, other);

        other.key == Some(self.get_key())
            && other.e_tag == Some(self.get_etag())
            && other.size == Some(self.get_size() as i64)
    }
}

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

    pub fn get_number_of_parts(&self) -> usize {
        self.etag
            .as_ref()
            .and_then(|etag| {
                etag.split('-')
                    .into_iter()
                    .last()
                    .and_then(|etag_nbr| etag_nbr.parse::<usize>().ok())
            })
            .unwrap_or(0)
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
