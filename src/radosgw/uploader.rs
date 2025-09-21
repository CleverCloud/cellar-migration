use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::Stream;
use http_body::Body;
use hyper::body::Incoming;
use tokio::task::JoinError;
use tracing::event;
use tracing::Level;

use crate::provider::{
    Provider, ProviderObject, ProviderObjectMetadata, ProviderResponseHttp04X,
    ProviderResponseStream, ProviderResponseStreamChunkWrapper,
};

use super::RadosGW;

pub type ObjectMigrationSize = usize;

pub struct ThreadMigrationResult {
    pub sync_results: Vec<anyhow::Result<ObjectMigrationSize>>,
    pub delete_results: Vec<anyhow::Result<ObjectMigrationSize>>,
}

#[derive(Debug, Clone)]
pub struct Uploader {
    source_provider_clients: Arc<Mutex<VecDeque<Box<dyn Provider>>>>,
    radosgw_clients: Arc<Mutex<VecDeque<RadosGW>>>,
    objects: Arc<Mutex<VecDeque<ProviderObject>>>,
    objects_to_delete: Arc<Mutex<VecDeque<ProviderObject>>>,
    threads: usize,
    multipart_chunk_size: usize,
}

impl Uploader {
    pub fn new(
        source_provider_clients: Arc<Mutex<VecDeque<Box<dyn Provider>>>>,
        radosgw_clients: Arc<Mutex<VecDeque<RadosGW>>>,
        objects: Vec<ProviderObject>,
        objects_to_delete: Vec<ProviderObject>,
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
            source_provider_clients,
            radosgw_clients,
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
            let source_clients = self.source_provider_clients.clone();
            let radosgw_clients = self.radosgw_clients.clone();
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

                    let radosgw_client = radosgw_clients
                        .lock()
                        .expect("Tried to lock radosgw_clients mutex but failed")
                        .pop_front()
                        .expect("We should have a RadosGW client available");

                    if let Some(object) = object {
                        event!(
                            Level::INFO,
                            "Thread {} | ({}/{}) Starting to sync object {}",
                            thread_id,
                            total_files - remaining,
                            total_files,
                            object.get_key()
                        );

                        let source_client = source_clients
                            .lock()
                            .expect("Tried to lock source_clients mutex but failed")
                            .pop_front()
                            .expect("We should have a source client available");

                        let result = Uploader::sync_object(
                            &*source_client,
                            &radosgw_client,
                            &object,
                            thread_id,
                            multipart_chunk_size,
                        )
                        .await
                        .map(|_| object.get_size() as usize);

                        results.push(result);
                        source_clients
                            .lock()
                            .expect("Tried to lock source_clients mutex but failed")
                            .push_back(source_client);
                        radosgw_clients
                            .lock()
                            .expect("Tried to lock radosgw_clients mutex but failed")
                            .push_back(radosgw_client);
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
                                object_to_delete.get_key()
                            );

                            let result = Uploader::delete_destination_object(
                                &radosgw_client,
                                object_to_delete,
                                thread_id,
                            )
                            .await
                            .map(|object| object.get_size() as usize);

                            radosgw_clients
                                .lock()
                                .expect("Tried to lock radosgw_clients mutex but failed")
                                .push_back(radosgw_client);
                            delete_results.push(result);
                        } else {
                            event!(
                                Level::INFO,
                                "Thread {} | No more objects to synchronize, quitting..",
                                thread_id
                            );
                            radosgw_clients
                                .lock()
                                .expect("Tried to lock radosgw_clients mutex but failed")
                                .push_back(radosgw_client);

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
        source_provider_client: &dyn Provider,
        radosgw_client: &RadosGW,
        object: &ProviderObject,
        thread_id: usize,
        multipart_chunk_size: usize,
    ) -> anyhow::Result<()> {
        let object_metadata = source_provider_client.get_object_metadata(object).await?;
        let mut response = source_provider_client.get_object(object).await?;
        if response.success() {
            let start = std::time::Instant::now();
            let object_size = object.get_size() as usize;

            if object_size < multipart_chunk_size {
                // Create ByteStream from the response body with known size to avoid UnsizedRequestBody errors
                let body_http04x = ProviderResponseHttp04X::with_exact_size(
                    response.body(),
                    object_size,
                );
                let body = ByteStream::from_body_1_x(body_http04x);

                Uploader::sync_object_singlepart(
                    radosgw_client,
                    object,
                    &object_metadata,
                    body,
                    thread_id,
                )
                .await?;
            } else {
                let body = response.body_chunked(multipart_chunk_size);
                Uploader::sync_object_multipart(
                    radosgw_client,
                    object,
                    &object_metadata,
                    Box::pin(body),
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
        } else if let Some(body) = response.consume_body().await {
            match body {
                Ok(bytes) => Err(anyhow::Error::from(DownloadError {
                    code: response.status(),
                    message: Some(String::from_utf8_lossy(&bytes).to_string()),
                    object: object.clone(),
                })),
                Err(error) => Err(anyhow::Error::from(DownloadError {
                    code: response.status(),
                    message: Some(format!("{:#?}", error)),
                    object: object.clone(),
                })),
            }
        } else {
            Err(anyhow::Error::from(DownloadError {
                code: response.status(),
                message: None,
                object: object.clone(),
            }))
        }
    }

    pub async fn sync_object_singlepart(
        radosgw_client: &RadosGW,
        object: &ProviderObject,
        object_metadata: &ProviderObjectMetadata,
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
            Err(error) => Err(anyhow::anyhow!(format!(
                "Failed to put object {}: {:?}",
                object.get_key(),
                error
            ))),
        }
    }

    pub async fn sync_object_multipart(
        radosgw_client: &RadosGW,
        object: &ProviderObject,
        object_metadata: &ProviderObjectMetadata,
        body: ProviderResponseStream,
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

            let chunk = ProviderResponseStreamChunkWrapper::new(body_wrapper.clone());
            let part_body = ByteStream::from_body_1_x(ProviderResponseHttp04X::with_exact_size(
                Box::pin(chunk),
                part_size,
            ));
            let upload_part_response = radosgw_client
                .put_object_part(
                    object.get_key(),
                    part_size as i64,
                    part_body,
                    multipart_upload_id.clone(),
                    radosgw_part_number as _,
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

                    return Err(anyhow::anyhow!(format!(
                        "Failed to put object {}: {:?}",
                        object.get_key(),
                        error
                    )));
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
        object: ProviderObject,
        thread_id: usize,
    ) -> anyhow::Result<ProviderObject> {
        event!(
            Level::DEBUG,
            "Thread {} | Delete object {}",
            thread_id,
            object.get_key()
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
    pub object: ProviderObject,
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
    body: Incoming,
}

impl RiakResponseStream {
    pub fn new(body: Incoming) -> RiakResponseStream {
        RiakResponseStream { body }
    }
}

impl Stream for RiakResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.body).poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(body) => return Poll::Ready(Some(Ok(body))),
                    Err(_) => continue,
                },
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        error,
                    ))))
                }
            }
        }
    }
}
