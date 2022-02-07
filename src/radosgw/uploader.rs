use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use hyper::body::HttpBody;
use rusoto_core::ByteStream;
use tokio::task::JoinError;
use tracing::event;
use tracing::Level;

use crate::riakcs::{
    dto::{get_part_size_from_etag, ObjectContents, ObjectMetadataResponse},
    RiakCS,
};

use super::RadosGW;

pub const RADOSGW_MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB

pub struct ThreadMigrationResult {
    pub sync_results: Vec<anyhow::Result<ObjectContents>>,
    pub delete_results: Vec<anyhow::Result<rusoto_s3::Object>>,
}

#[derive(Debug, Clone)]
pub struct Uploader {
    riak_client: RiakCS,
    radosgw_client: RadosGW,
    objects: Arc<Mutex<VecDeque<ObjectContents>>>,
    objects_to_delete: Arc<Mutex<VecDeque<rusoto_s3::Object>>>,
    threads: usize,
    multipart_chunk_size: usize,
}

impl Uploader {
    pub fn new(
        riak_client: RiakCS,
        radosgw_client: RadosGW,
        objects: Vec<ObjectContents>,
        objects_to_delete: Vec<rusoto_s3::Object>,
        threads: usize,
        multipart_chunk_size: usize,
    ) -> Uploader {
        let sync_len = objects.len() + objects_to_delete.len();
        if sync_len < threads {
            event!(
                Level::WARN,
                "There are more threads than files to synchronize. I'll only start {} threads",
                sync_len
            );
        }

        Uploader {
            riak_client,
            radosgw_client,
            objects: Arc::new(Mutex::new(VecDeque::from(objects))),
            objects_to_delete: Arc::new(Mutex::new(VecDeque::from(objects_to_delete))),
            threads: std::cmp::min(threads, sync_len),
            multipart_chunk_size,
        }
    }

    pub async fn sync(&mut self) -> Vec<Result<ThreadMigrationResult, JoinError>> {
        event!(Level::INFO, "Starting {} sync threads", self.threads);
        let mut handles = Vec::new();
        let total_files = self.objects.clone().lock().unwrap().len();
        let total_files_to_delete = self.objects_to_delete.clone().lock().unwrap().len();

        for thread_id in 0..self.threads {
            let riak_client = self.riak_client.clone();
            let radosgw_client = self.radosgw_client.clone();
            let files = self.objects.clone();
            let files_to_delete = self.objects_to_delete.clone();
            let multipart_chunk_size = self.multipart_chunk_size;
            let handle = tokio::spawn(async move {
                let mut results = Vec::new();
                let mut delete_results = Vec::new();
                loop {
                    let (object, remaining) = {
                        let mut files = files.lock().unwrap();
                        let object = files.pop_front();
                        let remaining = files.len();
                        (object, remaining)
                    };

                    if let Some(object) = object {
                        event!(
                            Level::INFO,
                            "Thread {} | ({}/{}) Starting to sync object {}",
                            thread_id,
                            total_files - remaining,
                            total_files,
                            object.get_key()
                        );

                        let result = Uploader::sync_object(
                            &riak_client,
                            &radosgw_client,
                            &object,
                            thread_id,
                            multipart_chunk_size,
                        )
                        .await
                        .map(|_| object);

                        results.push(result);
                    } else {
                        let (object_to_delete, remaining) = {
                            let mut files = files_to_delete.lock().unwrap();
                            let object = files.pop_front();
                            let remaining = files.len();
                            (object, remaining)
                        };

                        if let Some(object_to_delete) = object_to_delete {
                            event!(
                                Level::INFO,
                                "Thread {} | ({}/{}) Deleting object {} on destination bucket",
                                thread_id,
                                total_files_to_delete - remaining,
                                total_files_to_delete,
                                object_to_delete.key.as_ref().unwrap()
                            );

                            let result = Uploader::delete_destination_object(
                                &radosgw_client,
                                object_to_delete,
                                thread_id,
                            )
                            .await;

                            delete_results.push(result);
                        } else {
                            event!(
                                Level::INFO,
                                "Thread {} | No more objects to synchronize, quitting..",
                                thread_id
                            );
                            break;
                        }
                    }
                }

                ThreadMigrationResult {
                    sync_results: results,
                    delete_results,
                }
            });

            handles.push(handle);
        }

        futures::future::join_all(handles).await
    }

    pub async fn sync_object(
        riak_client: &RiakCS,
        radosgw_client: &RadosGW,
        object: &ObjectContents,
        thread_id: usize,
        multipart_chunk_size: usize,
    ) -> anyhow::Result<()> {
        let object_metadata = riak_client.get_object_metadata(object).await?;
        let mut response = riak_client.get_object(object).await?;
        if response.status().is_success() {
            let start = std::time::Instant::now();
            let object_size = object.get_size() as usize;
            let force_multipart_upload = if object_metadata.metadata.etag_has_parts() {
                let part_size = get_part_size_from_etag(
                    object_metadata.metadata.etag.as_ref().unwrap(),
                    object_size,
                );

                if part_size >= RADOSGW_MIN_PART_SIZE {
                    Some(part_size)
                } else {
                    event!(Level::WARN, "Object {} has been initially uploaded using multipart upload with parts less than 5MB. A different part size will be used if needed.", object.get_key());
                    None
                }
            } else {
                None
            };

            if let Some(part_size) = force_multipart_upload {
                let body =
                    RiakResponseStreamChunk::new(RiakResponseStream::new(response), part_size);
                Uploader::sync_object_multipart(
                    radosgw_client,
                    object,
                    &object_metadata,
                    body,
                    part_size,
                    thread_id,
                )
                .await?;
            } else if object_size < multipart_chunk_size {
                let body = ByteStream::new(RiakResponseStream::new(response));
                Uploader::sync_object_singlepart(
                    radosgw_client,
                    object,
                    &object_metadata,
                    body,
                    thread_id,
                )
                .await?;
            } else {
                let body = RiakResponseStreamChunk::new(
                    RiakResponseStream::new(response),
                    multipart_chunk_size,
                );
                Uploader::sync_object_multipart(
                    radosgw_client,
                    object,
                    &object_metadata,
                    body,
                    multipart_chunk_size,
                    thread_id,
                )
                .await?;
            }
            event!(
                Level::INFO,
                "Thread {} | Object {} has been put in {:?}",
                thread_id,
                object.get_key(),
                start.elapsed()
            );
            Ok(())
        } else if let Some(body) = response.body_mut().data().await {
            match body {
                Ok(bytes) => Err(anyhow::Error::from(DownloadError {
                    code: response.status().as_u16(),
                    message: Some(String::from_utf8_lossy(&bytes).to_string()),
                    object: object.clone(),
                })),
                Err(error) => Err(anyhow::Error::from(DownloadError {
                    code: response.status().as_u16(),
                    message: Some(format!("{:#?}", error)),
                    object: object.clone(),
                })),
            }
        } else {
            Err(anyhow::Error::from(DownloadError {
                code: response.status().as_u16(),
                message: None,
                object: object.clone(),
            }))
        }
    }

    pub async fn sync_object_singlepart(
        radosgw_client: &RadosGW,
        object: &ObjectContents,
        object_metadata: &ObjectMetadataResponse,
        body: ByteStream,
        thread_id: usize,
    ) -> anyhow::Result<()> {
        let response = radosgw_client
            .put_object(
                object.get_key(),
                object_metadata,
                object.get_size() as i64,
                body,
            )
            .await;

        match response {
            Ok(put_object_output) => {
                event!(
                    Level::TRACE,
                    "Thread {} | {:#?}",
                    thread_id,
                    put_object_output
                );
                Ok(())
            }
            Err(error) => Err(anyhow::Error::from(error)),
        }
    }

    pub async fn sync_object_multipart(
        radosgw_client: &RadosGW,
        object: &ObjectContents,
        object_metadata: &ObjectMetadataResponse,
        body: RiakResponseStreamChunk,
        multipart_chunk_size: usize,
        thread_id: usize,
    ) -> anyhow::Result<()> {
        let total_parts = (object.get_size() as f64 / multipart_chunk_size as f64).ceil() as usize;
        event!(Level::DEBUG, "Thread {} | Initiating multipart upload for object {}. object_size={}, part_size={}, total_parts={}", thread_id, object.get_key(), object.get_size(), multipart_chunk_size, total_parts);
        let multipart_upload = radosgw_client
            .create_multipart_upload(object.get_key(), object_metadata)
            .await?;
        let multipart_upload_id = multipart_upload
            .upload_id
            .expect("Multipart upload should have an upload id");
        let body_wrapper = Arc::new(Mutex::new(body));
        let mut completed_parts = Vec::with_capacity(total_parts);

        for part_number in 0..total_parts {
            let total_uploaded = part_number * multipart_chunk_size;
            let radosgw_part_number = part_number + 1;
            let remaining = object.get_size() as usize - total_uploaded;
            let part_size = std::cmp::min(remaining, multipart_chunk_size);
            event!(
                Level::DEBUG,
                "Thread {} | Object {}, total_uploaded={}, remaining={}, part_size={}",
                thread_id,
                object.get_key(),
                total_uploaded,
                remaining,
                part_size
            );

            let upload_part_response = radosgw_client
                .put_object_part(
                    object.get_key(),
                    part_size as i64,
                    ByteStream::new(RiakResponseStreamChunkWrapper::new(body_wrapper.clone())),
                    multipart_upload_id.clone(),
                    radosgw_part_number as i64,
                )
                .await;

            event!(
                Level::DEBUG,
                "Thread {} | Upload part response: {:#?}",
                thread_id,
                upload_part_response
            );

            match upload_part_response {
                Ok(response) => {
                    completed_parts.push((radosgw_part_number, response));
                }
                Err(error) => {
                    event!(
                        Level::DEBUG,
                        "Thread {} | Multipart upload aborted for {}",
                        thread_id,
                        object.get_key()
                    );
                    radosgw_client
                        .abort_multipart_upload(object.get_key(), multipart_upload_id)
                        .await?;
                    return Err(anyhow::Error::from(error));
                }
            }
        }

        match radosgw_client
            .complete_multipart_upload(
                object.get_key(),
                multipart_upload_id.clone(),
                completed_parts,
            )
            .await
        {
            Ok(_) => {}
            Err(error) => {
                event!(
                    Level::DEBUG,
                    "Thread {} | Multipart upload failed to complete for {}, reason={:#?}",
                    thread_id,
                    object.get_key(),
                    error
                );
                radosgw_client
                    .abort_multipart_upload(object.get_key(), multipart_upload_id)
                    .await?;
                return Err(anyhow::Error::from(error));
            }
        }

        event!(
            Level::DEBUG,
            "Thread {} | Multipart upload for object {} has finished.",
            thread_id,
            object.get_key()
        );

        Ok(())
    }

    pub async fn delete_destination_object(
        radosgw_client: &RadosGW,
        object: rusoto_s3::Object,
        thread_id: usize,
    ) -> anyhow::Result<rusoto_s3::Object> {
        event!(
            Level::DEBUG,
            "Thread {} | Delete object {}",
            thread_id,
            object.key.as_ref().unwrap()
        );

        radosgw_client
            .delete_object(object)
            .await
            .map_err(|err| anyhow::anyhow!(err))
    }
}

#[derive(Debug, Clone)]
pub struct DownloadError {
    pub code: u16,
    pub message: Option<String>,
    pub object: ObjectContents,
}

impl std::error::Error for DownloadError {
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

impl std::fmt::Display for DownloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", &self)
    }
}

pub struct RiakResponseStream {
    response: hyper::Response<hyper::Body>,
}

impl RiakResponseStream {
    pub fn new(response: hyper::Response<hyper::Body>) -> RiakResponseStream {
        RiakResponseStream { response }
    }
}

impl Stream for RiakResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.response).poll_data(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(body))) => Poll::Ready(Some(Ok(body))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                error.to_string(),
            )))),
        }
    }
}

#[derive(Debug)]
pub enum RiakResponseStreamChunkState {
    Active,
    Ended,
    Error(std::io::Error),
}

/// This structure exists because when we give a part to upload to rusoto
/// it will read until the end of the stream to end the part instead of just reading what the part's size
/// This structure simulates that, encapsulates the RiakResponseStream and keep an internal state on when to end the stream
/// because a part has been fully read.
pub struct RiakResponseStreamChunk {
    response: RiakResponseStream,
    chunk_size: usize,
    chunks: VecDeque<Bytes>,
    returned_bytes: usize,
    state: RiakResponseStreamChunkState,
}

impl RiakResponseStreamChunk {
    pub fn new(response: RiakResponseStream, chunk_size: usize) -> RiakResponseStreamChunk {
        RiakResponseStreamChunk {
            response,
            chunk_size,
            chunks: VecDeque::new(),
            returned_bytes: 0,
            state: RiakResponseStreamChunkState::Active,
        }
    }
}

impl Stream for RiakResponseStreamChunk {
    type Item = Result<Bytes, std::io::Error>;

    #[allow(clippy::branches_sharing_code)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        event!(Level::TRACE, "RiakResponseStreamChunk: poll_next, chunk_size={}, chunks_len={}, returned_bytes={}, state={:?}", self.chunk_size, self.chunks.len(), self.returned_bytes, self.state);
        if let RiakResponseStreamChunkState::Error(err) = &self.state {
            return Poll::Ready(Some(Err(std::io::Error::new(err.kind(), err.to_string()))));
        }

        match Pin::new(&mut self.response).poll_next(cx) {
            Poll::Ready(None) => {
                event!(
                    Level::TRACE,
                    "RiakResponseStreamChunk poll: got EOF from riak"
                );
                self.state = RiakResponseStreamChunkState::Ended;
            }
            Poll::Ready(Some(Ok(bytes))) => {
                event!(
                    Level::TRACE,
                    "RiakResponseStreamChunk: Got new chunk of length: {}",
                    bytes.len()
                );
                self.chunks.push_back(bytes);
            }
            Poll::Ready(Some(Err(error))) => {
                event!(
                    Level::TRACE,
                    "RiakResponseStreamChunk: Got error from stream, {:?}",
                    error
                );
                self.state = RiakResponseStreamChunkState::Error(error);
            }
            Poll::Pending => {}
        };

        if self.returned_bytes == self.chunk_size {
            event!(Level::TRACE, "RiakResponseStreamChunk: our stream has returned all needed bytes for now. Reset returned_bytes to 0.");
            self.returned_bytes = 0;
        }

        if !self.chunks.is_empty() {
            event!(
                Level::TRACE,
                "RiakResponseStreamChunk: we have some chunks ({}) to return. returned_bytes={}",
                self.chunks.len(),
                self.returned_bytes
            );
            let mut chunk = self.chunks.pop_front().unwrap();
            let diff = self.chunk_size - self.returned_bytes;
            event!(
                Level::TRACE,
                "RiakResponseStreamChunk: current chunk len={}, diff={}",
                chunk.len(),
                diff
            );
            if diff > chunk.len() {
                self.returned_bytes += chunk.len();
                event!(Level::TRACE, "RiakResponseStreamChunk: chunk is smaller than needed diff, returning it. returned_bytes={}", self.returned_bytes);
                Poll::Ready(Some(Ok(chunk)))
            } else {
                let new_chunk = chunk.split_off(diff);
                self.chunks.push_front(new_chunk);
                self.returned_bytes += chunk.len();
                event!(Level::TRACE, "RiakResponseStreamChunk: chunk is bigger than needed diff, only returning a portion. returned_bytes={}", self.returned_bytes);
                Poll::Ready(Some(Ok(chunk)))
            }
        } else if let RiakResponseStreamChunkState::Ended = self.state {
            self.returned_bytes = 0;
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// This struct exists so we can share a single RiakResponseStreamChunk
/// that will be fed to multiple ByteStream instances, without losing the
/// ownership on the inner Stream.
pub struct RiakResponseStreamChunkWrapper {
    inner: Arc<Mutex<RiakResponseStreamChunk>>,
}

impl RiakResponseStreamChunkWrapper {
    pub fn new(inner: Arc<Mutex<RiakResponseStreamChunk>>) -> RiakResponseStreamChunkWrapper {
        RiakResponseStreamChunkWrapper { inner }
    }
}

impl Stream for RiakResponseStreamChunkWrapper {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.clone().lock().unwrap().poll_next_unpin(cx)
    }
}
