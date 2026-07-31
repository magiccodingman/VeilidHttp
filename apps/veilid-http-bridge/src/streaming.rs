//! Stream-capable HTTP transaction handling over Veilid AppMessages.

use crate::completion::{CompletionClaim, CompletionLookup, CompletionStore};
use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::{StreamExt as _, stream};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot},
    task::JoinHandle,
};
use veilid_http_core::RetryPolicy;
use veilid_http_engine::{InboundBody, OutboundBody};
use veilid_http_http::{
    HeaderField, ResponseHead, attach_route_headers, normalize_request, strip_hop_by_hop,
    upstream_url,
};
use veilid_http_stream::{
    CompressionMode, DecodedFrame, RequestAccepted, RequestOpen, ResponseOpen, StreamDirection,
    StreamError, decode, encode_ack, encode_error, encode_request_accepted, encode_response_open,
};
use veilid_http_transport::{RouteTarget, VeilidTransport};
use veilid_http_wire::{Frame, FrameBundle, VEILID_MESSAGE_LIMIT};

/// Runtime limits and one-upstream policy used by streamed transactions.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Exactly one HTTP upstream base URL.
    pub upstream_url: String,
    /// Trusted downstream route metadata header.
    pub route_header: String,
    /// Complete VHTTP frame target.
    pub frame_bytes: usize,
    /// Local maximum in-flight frames in each direction.
    pub window_frames: u32,
    /// Encoded bytes allowed between source and send window.
    pub max_pending_bytes: usize,
    /// Out-of-order compressed bytes retained by a receiver.
    pub max_out_of_order_bytes: usize,
    /// Maximum logical request bytes; zero means `u64::MAX`.
    pub max_request_bytes: u64,
    /// Maximum logical response bytes; zero means `u64::MAX`.
    pub max_response_bytes: u64,
    /// Overall transaction lifetime.
    pub overall_timeout: Duration,
}

impl StreamingConfig {
    fn request_limit(&self) -> u64 {
        if self.max_request_bytes == 0 {
            u64::MAX
        } else {
            self.max_request_bytes
        }
    }

    fn response_limit(&self) -> u64 {
        if self.max_response_bytes == 0 {
            u64::MAX
        } else {
            self.max_response_bytes
        }
    }
}

struct ServerTransaction {
    return_target: RouteTarget,
    accepted_reply: Bytes,
    request_receiver: Mutex<Option<InboundBody>>,
    upstream_body: Mutex<Option<mpsc::Sender<Bytes>>>,
    response_sender: Mutex<Option<OutboundBody>>,
    response_notify: Notify,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ServerTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerTransaction")
            .field("return_target", &self.return_target)
            .finish_non_exhaustive()
    }
}

/// Shared stream runtime used by the bridge event loop.
#[derive(Clone)]
pub struct StreamingBridge {
    transport: Arc<dyn VeilidTransport>,
    client: reqwest::Client,
    completion: Arc<CompletionStore>,
    config: StreamingConfig,
    transactions: Arc<Mutex<HashMap<[u8; 16], Arc<ServerTransaction>>>>,
    clock: Instant,
}

impl std::fmt::Debug for StreamingBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamingBridge")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl StreamingBridge {
    /// Create a stream runtime around one Veilid adapter and one HTTP upstream client.
    #[must_use]
    pub fn new(
        transport: Arc<dyn VeilidTransport>,
        client: reqwest::Client,
        completion: Arc<CompletionStore>,
        config: StreamingConfig,
    ) -> Self {
        Self {
            transport,
            client,
            completion,
            config,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            clock: Instant::now(),
        }
    }

    /// Whether the encoded AppCall is a stream-capable `RequestOpen`.
    #[must_use]
    pub fn is_stream_open(encoded: Bytes) -> bool {
        matches!(decode(encoded), Ok(DecodedFrame::RequestOpen { .. }))
    }

    /// Accept a stream-capable request opening and reply through the AppCall.
    ///
    /// The AppCall reply is deliberately `RequestAccepted`; all response status/body data
    /// then travels to the client's private return route.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed metadata, route import, HTTP policy, transaction
    /// setup, or AppCall reply failure.
    pub async fn handle_open(
        &self,
        call_id: &str,
        site_fingerprint: String,
        encoded: Bytes,
    ) -> Result<()> {
        let (transaction_id, open) = match decode(encoded)? {
            DecodedFrame::RequestOpen {
                transaction_id,
                value,
                initial_payload,
            } => {
                if !initial_payload.is_empty() {
                    bail!(
                        "stream RequestOpen initial payload is reserved until sequenced initial-data support is enabled"
                    );
                }
                (transaction_id, value)
            }
            _ => bail!("expected stream-capable RequestOpen"),
        };

        if let Some(existing) = self.transactions.lock().await.get(&transaction_id).cloned() {
            self.transport
                .app_call_reply(call_id, existing.accepted_reply.clone())
                .await
                .context("replay RequestAccepted for duplicate RequestOpen")?;
            return Ok(());
        }

        match self.completion.claim(transaction_id).await {
            CompletionClaim::Execute => {}
            CompletionClaim::WaitCapacity(_) => {
                let reply = encode_error(
                    transaction_id,
                    &StreamError {
                        code: "server-busy".to_owned(),
                        message: "the bridge has reached its active transaction limit".to_owned(),
                        retryable: true,
                    },
                )?;
                self.transport
                    .app_call_reply(call_id, reply)
                    .await
                    .context("reply to capacity-limited RequestOpen")?;
                return Ok(());
            }
            CompletionClaim::WaitDuplicate(_)
            | CompletionClaim::Replay(_)
            | CompletionClaim::Tombstone => {
                let reply = encode_error(
                    transaction_id,
                    &StreamError {
                        code: "transaction-already-seen".to_owned(),
                        message: "transaction is already executing or completed; it will not be forwarded again".to_owned(),
                        retryable: false,
                    },
                )?;
                self.transport
                    .app_call_reply(call_id, reply)
                    .await
                    .context("reply to duplicate completed RequestOpen")?;
                return Ok(());
            }
        }

        let result = self
            .handle_open_claimed(call_id, site_fingerprint, transaction_id, open)
            .await;
        if result.is_err() {
            self.completion.abandon(transaction_id).await;
        }
        result
    }

    async fn handle_open_claimed(
        &self,
        call_id: &str,
        site_fingerprint: String,
        transaction_id: [u8; 16],
        open: RequestOpen,
    ) -> Result<()> {
        let open = RequestOpen {
            head: normalize_request(open.head)?,
            ..open
        };
        if open.return_route_blob.is_empty() {
            bail!("stream RequestOpen did not supply a private return RouteBlob");
        }
        if open.response_receive_window == 0 || open.response_receive_window > 64 {
            bail!("invalid client response receive window");
        }
        let return_target = self
            .transport
            .import_route(Bytes::from(open.return_route_blob.clone()))
            .await
            .context("import client private return route")?;
        let accepted_reply = encode_request_accepted(
            transaction_id,
            RequestAccepted {
                request_receive_window: self.config.window_frames,
            },
        )?;

        let (body_sender, body_receiver) = mpsc::channel::<Bytes>(
            usize::try_from(self.config.window_frames)
                .unwrap_or(32)
                .max(1),
        );
        let request_receiver = if open.request_body {
            Some(InboundBody::new(
                transaction_id,
                StreamDirection::Request,
                open.request_compression,
                self.config.window_frames,
                self.config.max_out_of_order_bytes,
                self.config.request_limit(),
            )?)
        } else {
            None
        };
        let transaction = Arc::new(ServerTransaction {
            return_target: return_target.clone(),
            accepted_reply: accepted_reply.clone(),
            request_receiver: Mutex::new(request_receiver),
            upstream_body: Mutex::new(open.request_body.then_some(body_sender)),
            response_sender: Mutex::new(None),
            response_notify: Notify::new(),
            task: Mutex::new(None),
        });
        self.transactions
            .lock()
            .await
            .insert(transaction_id, Arc::clone(&transaction));

        let (start_sender, start_receiver) = oneshot::channel::<()>();
        let runtime = self.clone();
        let task_transaction = Arc::clone(&transaction);
        let task = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                return;
            }
            let result = runtime
                .run_upstream(
                    transaction_id,
                    site_fingerprint,
                    open,
                    body_receiver,
                    Arc::clone(&task_transaction),
                )
                .await;
            runtime.transactions.lock().await.remove(&transaction_id);
            if let Err(error) = &result {
                tracing::error!(%error, transaction = %hex_transaction(transaction_id), "streamed HTTP transaction failed");
                let _ = runtime
                    .send_error(
                        &task_transaction.return_target,
                        transaction_id,
                        "stream-transaction-failed",
                        error.to_string(),
                        false,
                    )
                    .await;
            }
            if let Err(error) = runtime.completion.record(transaction_id, None).await {
                tracing::error!(%error, transaction = %hex_transaction(transaction_id), "failed to persist streamed completion tombstone");
            }
        });
        *transaction.task.lock().await = Some(task);

        if let Err(error) = self.transport.app_call_reply(call_id, accepted_reply).await {
            if let Some(task) = transaction.task.lock().await.take() {
                task.abort();
            }
            self.transactions.lock().await.remove(&transaction_id);
            return Err(error).context("reply RequestAccepted");
        }
        let _ = start_sender.send(());
        Ok(())
    }

    /// Process one AppMessage or VHTTP transport bundle for active stream transactions.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed frames, missing transactions, body/ACK state
    /// failures, or transport dispatch failure.
    pub async fn handle_message(&self, payload: Bytes) -> Result<()> {
        let mut pending_acks: HashMap<RouteTarget, Vec<Bytes>> = HashMap::new();
        for encoded in split_frames(payload)? {
            match decode(encoded.clone())? {
                DecodedFrame::Data {
                    transaction_id,
                    direction: StreamDirection::Request,
                    sequence,
                    ..
                }
                | DecodedFrame::End {
                    transaction_id,
                    sequence,
                    value:
                        veilid_http_stream::StreamEnd {
                            direction: StreamDirection::Request,
                            ..
                        },
                } => {
                    let Some(transaction) = self.transaction(transaction_id).await? else {
                        continue;
                    };
                    let output = transaction
                        .request_receiver
                        .lock()
                        .await
                        .as_mut()
                        .ok_or_else(|| {
                            anyhow::anyhow!("transaction does not accept a request body")
                        })?
                        .receive(encoded)?;
                    for chunk in output.logical_chunks {
                        let sender = transaction.upstream_body.lock().await.clone();
                        let Some(sender) = sender else {
                            bail!("upstream request body is already closed");
                        };
                        sender
                            .send(chunk)
                            .await
                            .context("apply upstream request-body backpressure")?;
                    }
                    if output.completed {
                        transaction.upstream_body.lock().await.take();
                    }
                    let should_ack =
                        output.completed || output.ack.selective != 0 || sequence % 4 == 3;
                    if should_ack {
                        pending_acks
                            .entry(transaction.return_target.clone())
                            .or_default()
                            .push(encode_ack(transaction_id, output.ack)?);
                    }
                }
                DecodedFrame::Ack {
                    transaction_id,
                    value,
                } if value.direction == StreamDirection::Response => {
                    let Some(transaction) = self.transaction(transaction_id).await? else {
                        continue;
                    };
                    let mut sender = transaction.response_sender.lock().await;
                    let outbound = sender
                        .as_mut()
                        .ok_or_else(|| anyhow::anyhow!("response stream is not open"))?;
                    outbound.acknowledge(value)?;
                    drop(sender);
                    transaction.response_notify.notify_waiters();
                }
                DecodedFrame::Cancel { transaction_id, .. } => {
                    self.cancel(transaction_id).await;
                }
                DecodedFrame::Error {
                    transaction_id,
                    value,
                } => {
                    tracing::warn!(
                        transaction = %hex_transaction(transaction_id),
                        code = %value.code,
                        message = %value.message,
                        "client reported stream error"
                    );
                    self.cancel(transaction_id).await;
                }
                DecodedFrame::Other(Frame { frame_type, .. }) => {
                    tracing::debug!(?frame_type, "ignored non-stream AppMessage frame");
                }
                other => {
                    tracing::debug!(?other, "ignored stream frame in the wrong bridge direction");
                }
            }
        }
        for (target, frames) in pending_acks {
            self.send_encoded_frames(&target, frames)
                .await
                .context("send batched request-stream ACKs")?;
        }
        Ok(())
    }

    /// Abort every active transaction, used when the server receiving route dies.
    pub async fn cancel_all(&self) {
        let transactions = self.transactions.lock().await.drain().collect::<Vec<_>>();
        for (id, transaction) in transactions {
            if let Some(task) = transaction.task.lock().await.take() {
                task.abort();
            }
            if let Err(error) = self.completion.record(id, None).await {
                tracing::error!(%error, transaction = %hex_transaction(id), "failed to persist cancelled transaction tombstone");
            }
        }
    }

    async fn transaction(&self, id: [u8; 16]) -> Result<Option<Arc<ServerTransaction>>> {
        if let Some(transaction) = self.transactions.lock().await.get(&id).cloned() {
            return Ok(Some(transaction));
        }
        if self.completion.lookup(id).await.is_some() {
            return Ok(None);
        }
        bail!("unknown or expired VHTTP transaction")
    }

    async fn cancel(&self, id: [u8; 16]) {
        if let Some(transaction) = self.transactions.lock().await.remove(&id) {
            transaction.upstream_body.lock().await.take();
            if let Some(task) = transaction.task.lock().await.take() {
                task.abort();
            }
            if let Err(error) = self.completion.record(id, None).await {
                tracing::error!(%error, transaction = %hex_transaction(id), "failed to persist cancelled transaction tombstone");
            }
        } else if matches!(
            self.completion.lookup(id).await,
            Some(
                CompletionLookup::Response(_)
                    | CompletionLookup::Tombstone
                    | CompletionLookup::InFlight
            )
        ) {
            tracing::debug!(transaction = %hex_transaction(id), "ignored cancellation for completed transaction");
        }
    }

    async fn run_upstream(
        &self,
        transaction_id: [u8; 16],
        site_fingerprint: String,
        open: RequestOpen,
        body_receiver: mpsc::Receiver<Bytes>,
        transaction: Arc<ServerTransaction>,
    ) -> Result<()> {
        let target = upstream_url(&self.config.upstream_url, &open.head.path_and_query)?;
        let request_method = open.head.method.clone();
        let method = reqwest::Method::from_bytes(request_method.as_bytes())
            .context("parse streamed HTTP method")?;
        let headers = attach_route_headers(
            open.head.headers,
            &site_fingerprint,
            &self.config.route_header,
        );
        let mut request = self.client.request(method, target);
        for header in headers {
            if header.name.eq_ignore_ascii_case("host")
                || header.name.eq_ignore_ascii_case("content-length")
            {
                continue;
            }
            let name = reqwest::header::HeaderName::from_bytes(header.name.as_bytes())?;
            let value = reqwest::header::HeaderValue::from_bytes(header.value.as_bytes())?;
            request = request.header(name, value);
        }
        if open.request_body {
            let body_stream = stream::unfold(body_receiver, |mut receiver| async move {
                receiver
                    .recv()
                    .await
                    .map(|chunk| (Ok::<Bytes, std::io::Error>(chunk), receiver))
            });
            request = request.body(reqwest::Body::wrap_stream(body_stream));
        }

        let response = tokio::time::timeout(self.config.overall_timeout, request.send())
            .await
            .context("streamed upstream request exceeded overall timeout")?
            .context("send streamed upstream request")?;
        let status = response.status().as_u16();
        let response_headers = strip_hop_by_hop(
            response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value.to_str().ok().map(|value| HeaderField {
                        name: name.as_str().to_owned(),
                        value: value.to_owned(),
                    })
                })
                .collect::<Vec<_>>(),
        );
        let response_has_body = !request_method.eq_ignore_ascii_case("HEAD")
            && !(100..200).contains(&status)
            && !matches!(status, 204 | 205 | 304);
        let response_open = ResponseOpen {
            head: ResponseHead {
                status,
                headers: response_headers,
            },
            response_compression: CompressionMode::Zstd,
            response_body: response_has_body,
            request_receive_window: self.config.window_frames,
        };
        self.transport
            .app_message(
                &transaction.return_target,
                encode_response_open(transaction_id, &response_open, Bytes::new())?,
            )
            .await
            .context("send streamed ResponseOpen")?;
        if !response_has_body {
            return Ok(());
        }

        let mut outbound = OutboundBody::new(
            transaction_id,
            StreamDirection::Response,
            CompressionMode::Zstd,
            3,
            self.config.frame_bytes,
            256,
            usize::try_from(self.config.window_frames).unwrap_or(32),
            self.config.max_pending_bytes,
        )?;
        outbound.set_peer_window(open.response_receive_window)?;
        *transaction.response_sender.lock().await = Some(outbound);

        let deadline = tokio::time::sleep(self.config.overall_timeout);
        tokio::pin!(deadline);
        let mut response_stream = response.bytes_stream();
        loop {
            self.dispatch_ready(&transaction).await?;
            let backpressured = transaction
                .response_sender
                .lock()
                .await
                .as_ref()
                .is_some_and(OutboundBody::is_backpressured);
            if backpressured {
                tokio::select! {
                    () = transaction.response_notify.notified() => {}
                    () = tokio::time::sleep(Duration::from_millis(50)) => {}
                    () = &mut deadline => bail!("streamed response exceeded overall timeout"),
                }
                continue;
            }

            tokio::select! {
                item = response_stream.next() => {
                    match item {
                        Some(Ok(chunk)) => self.push_response_chunk(&transaction, &chunk).await?,
                        Some(Err(error)) => return Err(error).context("read streamed upstream response"),
                        None => break,
                    }
                }
                () = transaction.response_notify.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
                () = &mut deadline => bail!("streamed response exceeded overall timeout"),
            }
        }

        transaction
            .response_sender
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("response sender disappeared"))?
            .finish_input()?;
        loop {
            self.dispatch_ready(&transaction).await?;
            if transaction
                .response_sender
                .lock()
                .await
                .as_ref()
                .is_some_and(OutboundBody::is_complete)
            {
                break;
            }
            tokio::select! {
                () = transaction.response_notify.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
                () = &mut deadline => bail!("response acknowledgements exceeded overall timeout"),
            }
        }
        Ok(())
    }

    async fn push_response_chunk(
        &self,
        transaction: &ServerTransaction,
        chunk: &[u8],
    ) -> Result<()> {
        let limit = transaction
            .response_sender
            .lock()
            .await
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("response sender is not initialized"))?
            .max_input_chunk();
        let count = chunk.chunks(limit).count();
        for (index, part) in chunk.chunks(limit).enumerate() {
            tokio::time::timeout(
                self.config.overall_timeout,
                self.wait_for_response_capacity(transaction),
            )
            .await
            .context("response sender remained backpressured past the overall timeout")??;
            transaction
                .response_sender
                .lock()
                .await
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("response sender disappeared"))?
                .push(part, index + 1 == count)?;
            self.dispatch_ready(transaction).await?;
        }
        Ok(())
    }

    async fn wait_for_response_capacity(&self, transaction: &ServerTransaction) -> Result<()> {
        loop {
            self.dispatch_ready(transaction).await?;
            let backpressured = transaction
                .response_sender
                .lock()
                .await
                .as_ref()
                .is_some_and(OutboundBody::is_backpressured);
            if !backpressured {
                return Ok(());
            }
            tokio::select! {
                () = transaction.response_notify.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
    }

    async fn dispatch_ready(&self, transaction: &ServerTransaction) -> Result<()> {
        let now = u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX);
        let frames = transaction
            .response_sender
            .lock()
            .await
            .as_mut()
            .map(|sender| {
                sender.take_sendable(
                    now,
                    RetryPolicy {
                        initial_ms: 250,
                        maximum_ms: 10_000,
                    },
                )
            })
            .unwrap_or_default()
            .into_iter()
            .map(|frame| frame.encoded)
            .collect();
        self.send_encoded_frames(&transaction.return_target, frames)
            .await
            .context("send or retry response stream frames")
    }

    async fn send_encoded_frames(&self, target: &RouteTarget, frames: Vec<Bytes>) -> Result<()> {
        let mut bundle = Vec::new();
        let mut encoded_bytes = 8_usize;
        for frame in frames {
            let projected = encoded_bytes.saturating_add(4).saturating_add(frame.len());
            if !bundle.is_empty() && projected > VEILID_MESSAGE_LIMIT {
                self.send_bundle(target, std::mem::take(&mut bundle))
                    .await?;
                encoded_bytes = 8;
            }
            encoded_bytes = encoded_bytes.saturating_add(4).saturating_add(frame.len());
            bundle.push(frame);
        }
        if !bundle.is_empty() {
            self.send_bundle(target, bundle).await?;
        }
        Ok(())
    }

    async fn send_bundle(&self, target: &RouteTarget, frames: Vec<Bytes>) -> Result<()> {
        let payload = if frames.len() == 1 {
            frames.into_iter().next().expect("one frame")
        } else {
            FrameBundle { frames }.encode()?
        };
        self.transport
            .app_message(target, payload)
            .await
            .context("send bundled VHTTP AppMessage")
    }

    async fn send_error(
        &self,
        target: &RouteTarget,
        transaction_id: [u8; 16],
        code: &str,
        message: String,
        retryable: bool,
    ) -> Result<()> {
        self.transport
            .app_message(
                target,
                encode_error(
                    transaction_id,
                    &StreamError {
                        code: code.to_owned(),
                        message,
                        retryable,
                    },
                )?,
            )
            .await
            .context("send VHTTP stream error")
    }
}

fn split_frames(payload: Bytes) -> Result<Vec<Bytes>> {
    if let Ok(bundle) = FrameBundle::decode(payload.clone()) {
        return Ok(bundle.frames);
    }
    Frame::decode(payload.clone()).context("decode VHTTP AppMessage frame")?;
    Ok(vec![payload])
}

fn hex_transaction(id: [u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}
