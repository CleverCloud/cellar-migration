pub mod dto;

use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use dto::{ListObjectResponse, ObjectContents};
use futures::Stream;
use http_body_util::{BodyExt, Empty};
use hyper::{body::Incoming, Method, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use ring::hmac;
use serde::Deserialize;
use serde_xml_rs::de::Deserializer;
use tracing::{event, instrument, Level};
use xml::reader::ParserConfig;

use crate::{
    provider::{
        Provider, ProviderObject, ProviderObjectMetadata, ProviderResponse, ProviderResponseStream,
        ProviderResponseStreamChunk,
    },
    radosgw::uploader::RiakResponseStream,
    riakcs::dto::ListBucketsResult,
};

use self::dto::{ListBucket, ObjectMetadata, ObjectMetadataResponse};

type RequestBody = Empty<Bytes>;
type ResponseBody = Incoming;

#[derive(Debug)]
#[allow(dead_code)]
pub struct RiakCSError {
    uri: String,
    code: u16,
    body: Option<String>,
}

impl RiakCSError {
    pub fn new(uri: String, code: u16, body: Option<String>) -> RiakCSError {
        RiakCSError { uri, code, body }
    }
}

impl std::error::Error for RiakCSError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl std::fmt::Display for RiakCSError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

#[derive(Debug, Clone)]
pub struct RiakCS {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: Option<String>,
}

impl RiakCS {
    pub fn new(
        endpoint: String,
        access_key: String,
        secret_key: String,
        bucket: Option<String>,
    ) -> RiakCS {
        RiakCS {
            endpoint,
            access_key,
            secret_key,
            bucket,
        }
    }

    #[instrument(skip(self), level = "debug")]
    fn sign_string(&self, to_sign: String) -> String {
        let key = hmac::Key::new(
            hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
            self.secret_key.as_bytes(),
        );
        event!(Level::TRACE, "to sign: {:#?}", to_sign);
        let computed_hash = hmac::sign(&key, to_sign.as_bytes());
        event!(Level::TRACE, "{:x?}", computed_hash.as_ref());
        base64::engine::general_purpose::STANDARD_NO_PAD.encode(computed_hash.as_ref())
    }

    fn sign_request(&self, req: &mut Request<RequestBody>) {
        let mut to_sign: Vec<String> = Vec::new();
        let now = Utc::now().to_rfc2822();
        req.headers_mut().append("x-amz-date", now.parse().unwrap());

        to_sign.push(req.method().as_str().to_string());
        to_sign.push(
            req.headers()
                .get("content-md5")
                .map(|v| v.to_str().unwrap())
                .unwrap_or("")
                .to_string(),
        );
        to_sign.push(
            req.headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap())
                .unwrap_or("")
                .to_string(),
        );
        to_sign.push(
            req.headers()
                .get("date")
                .map(|v| v.to_str().unwrap())
                .unwrap_or("")
                .to_string(),
        );

        for (header_name, header_value) in req.headers() {
            if header_name.as_str().starts_with("x-amz-") {
                to_sign.push(format!(
                    "{}:{}",
                    header_name.as_str(),
                    header_value.to_str().unwrap()
                ));
            }
        }

        to_sign.push(req.uri().path().to_string());

        let encoded_sha1 = self.sign_string(to_sign.join("\n"));

        req.headers_mut().append(
            "Authorization",
            format!("AWS {}:{}", self.access_key, encoded_sha1)
                .parse()
                .unwrap(),
        );
    }

    fn sign_url(&self, object: &ProviderObject, expiry: DateTime<Utc>) -> String {
        let to_sign = format!(
            "GET\n\n\n{}\n/{}/{}",
            expiry.timestamp(),
            self.bucket.as_ref().unwrap_or(&String::new()),
            urlencoding::encode(&object.get_key())
        );

        self.sign_string(to_sign)
    }

    fn get_uri(&self) -> String {
        format!(
            "https://{}/{}",
            self.endpoint,
            self.bucket.as_ref().unwrap_or(&String::new())
        )
    }

    #[instrument(skip(self, req), level = "debug")]
    async fn send_request_deser<'de, T>(&self, req: Request<RequestBody>) -> Result<T>
    where
        T: Deserialize<'de>,
    {
        let uri = req.uri().to_string();
        let response = self.send_request(req).await?;
        let status = response.status();
        let body_bytes = response.into_body().collect().await?.to_bytes();
        let data_str = String::from_utf8_lossy(&body_bytes);
        event!(Level::TRACE, "{}", data_str);

        let reader = ParserConfig::default()
            .trim_whitespace(false)
            .create_reader(data_str.as_bytes());
        if status.is_success() {
            let deser = T::deserialize(&mut Deserializer::new(reader))?;
            Ok(deser)
        } else {
            Err(anyhow::Error::from(RiakCSError::new(
                uri,
                status.as_u16(),
                Some(data_str.to_string()),
            )))
        }
    }

    #[instrument(skip(self, req), level = "debug")]
    async fn send_request(&self, req: Request<RequestBody>) -> Result<Response<ResponseBody>> {
        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new()).build::<_, RequestBody>(https);

        event!(
            Level::TRACE,
            "Sending {} request to {:?}",
            req.method().as_str(),
            req.uri()
        );
        let response = client.request(req).await?;

        event!(Level::TRACE, "{:#?}", response);
        Ok(response)
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn list_objects(
        &self,
        max_keys: Option<usize>,
        mut marker: Option<String>,
    ) -> Result<Vec<ObjectContents>> {
        let mut results = Vec::new();
        loop {
            let uri = format!(
                "{}?max-keys={}{}",
                self.get_uri(),
                std::cmp::max(max_keys.unwrap_or(1000), 1000),
                marker
                    .take()
                    .map(|m| format!("&marker={}", urlencoding::encode(&m)))
                    .unwrap_or_default()
            );

            event!(Level::TRACE, "Build request with uri: {}", uri);
            let mut req = Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(RequestBody::new())?;

            self.sign_request(&mut req);
            event!(Level::TRACE, "{:#?}", req);

            let response: ListObjectResponse = self.send_request_deser(req).await?;

            let mut objects = response.get_objects();
            let last_object = objects.last().map(|o| o.get_key());
            results.append(&mut objects);

            if response.truncated() {
                marker = last_object;
            } else {
                break;
            }
        }

        Ok(results)
    }

    #[instrument(skip(self), level = "debug")]
    fn get_download_url(&self, object: &ProviderObject) -> String {
        let uri = self.get_uri();
        let expires = Utc::now() + Duration::hours(1);
        let signature = self.sign_url(object, expires);
        event!(
            Level::TRACE,
            "Expires: {:?}, now={:?}, signature={}",
            expires,
            Utc::now(),
            signature
        );

        format!(
            "{}/{}?AWSAccessKeyId={}&Expires={}&Signature={}",
            uri,
            urlencoding::encode(&object.get_key()),
            self.access_key,
            expires.timestamp(),
            urlencoding::encode(&signature)
        )
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn get_object(&self, object: &ProviderObject) -> Result<Response<ResponseBody>> {
        let url = self.get_download_url(object);

        let req = Request::builder()
            .method(Method::GET)
            .uri(url)
            .body(RequestBody::new())?;

        self.send_request(req).await
    }

    #[allow(dead_code)]
    pub async fn get_object_acl(&self, object: &ObjectContents) -> Result<()> {
        let uri = format!("{}/{}?acl", self.get_uri(), object.get_key());
        let mut req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(RequestBody::new())?;

        self.sign_request(&mut req);

        let response = self.send_request(req).await?;
        let data = response.into_body().collect().await?.to_bytes();
        let data_str = String::from_utf8_lossy(&data);
        event!(Level::TRACE, "{}", data_str);

        Ok(())
    }

    #[instrument(skip(self), level = "debug")]
    async fn _get_object_metadata(
        &self,
        object: &ProviderObject,
        with_signature: bool,
    ) -> Result<ObjectMetadataResponse> {
        let uri = format!(
            "{}/{}",
            self.get_uri(),
            urlencoding::encode(&object.get_key())
        );
        let mut use_signature = with_signature;

        // Loop or else it will complain about "recursion in an `async fn` requires boxing"
        // and it didn't want to handle this.
        // So the loop will only loop once maximum
        // See rustc --explain E0733
        loop {
            let mut req = Request::builder()
                .method(Method::HEAD)
                .uri(uri.clone())
                .body(RequestBody::new())?;

            if use_signature {
                self.sign_request(&mut req);
            }

            let response = self.send_request(req).await?;
            if response.status().is_success() {
                return Ok(ObjectMetadataResponse::new(
                    ObjectMetadata::from(response),
                    !use_signature,
                ));
            } else if !use_signature && response.status().as_u16() == 403 {
                use_signature = true;
                continue;
            } else {
                return Err(anyhow::Error::new(RiakCSError::new(
                    uri.clone(),
                    response.status().as_u16(),
                    None,
                )));
            }
        }
    }

    #[instrument(skip(self), level = "debug")]
    pub async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> Result<ObjectMetadataResponse> {
        self._get_object_metadata(object, false).await
    }

    pub async fn list_buckets(&self) -> Result<Vec<ListBucket>> {
        let uri = self.get_uri();
        let mut req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(RequestBody::new())?;

        self.sign_request(&mut req);
        let response: ListBucketsResult = self.send_request_deser(req).await?;

        Ok(response.get_buckets().to_vec())
    }
}

#[async_trait]
impl Provider for RiakCS {
    async fn get_buckets(&self) -> anyhow::Result<Vec<String>> {
        let riak_buckets = self.list_buckets().await?;
        let buckets = riak_buckets
            .iter()
            .map(|bucket| bucket.name.clone())
            .collect();
        Ok(buckets)
    }

    fn list_objects(
        &self,
        max_keys: Option<usize>,
        start_after: Option<String>,
    ) -> Pin<Box<dyn Stream<Item = anyhow::Result<Vec<ProviderObject>>> + '_>> {
        Box::pin(futures::stream::unfold(
            start_after,
            move |start_after| async move {
                let objects: anyhow::Result<Vec<ProviderObject>> = self
                    .list_objects(max_keys, start_after.clone())
                    .await
                    .map(|res| res.iter().map(|object| object.into()).collect());

                match objects {
                    Ok(objects) => {
                        if objects.is_empty() {
                            None
                        } else {
                            let last_object = objects.last().unwrap().get_key();
                            Some((Ok(objects), Some(last_object)))
                        }
                    }
                    Err(error) => Some((Err(anyhow::anyhow!(error)), start_after)),
                }
            },
        ))
    }

    async fn get_object_metadata(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<ProviderObjectMetadata> {
        self.get_object_metadata(object)
            .await
            .map(|metadata| metadata.into())
    }

    async fn get_object(
        &self,
        object: &ProviderObject,
    ) -> anyhow::Result<Box<dyn ProviderResponse>> {
        self.get_object(object).await.map(|res| {
            let x: Box<dyn ProviderResponse> = Box::new(RiakCSResponse::new(res));
            x
        })
    }
}

#[derive(Debug)]
struct RiakCSResponse {
    response: Option<Response<ResponseBody>>,
    status: StatusCode,
}

impl RiakCSResponse {
    pub fn new(response: Response<ResponseBody>) -> RiakCSResponse {
        let status = response.status();
        RiakCSResponse {
            response: Some(response),
            status,
        }
    }
}

impl ProviderResponse for RiakCSResponse {
    fn status(&self) -> u16 {
        self.status.as_u16()
    }

    fn body(&mut self) -> ProviderResponseStream {
        Box::pin(RiakResponseStream::new(
            self.response
                .take()
                .expect("We should have a response")
                .into_body(),
        ))
    }

    fn body_chunked(&mut self, chunk_size: usize) -> ProviderResponseStream {
        Box::pin(ProviderResponseStreamChunk::new(
            Box::pin(RiakResponseStream::new(
                self.response
                    .take()
                    .expect("We should have a response")
                    .into_body(),
            )),
            chunk_size,
        ))
    }
}
