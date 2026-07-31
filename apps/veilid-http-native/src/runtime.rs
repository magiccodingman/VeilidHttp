//! Native VHTTP client runtime over embedded `veilid-core`.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use veilid_http_core::RetryPolicy;
use veilid_http_engine::{InboundBody, OutboundBody};
use veilid_http_http::{RequestHead, ResponseHead};
use veilid_http_stream::{
    Ack, CompressionMode, DecodedFrame, RequestOpen, ResponseOpen, StreamDirection, decode,
    encode_ack, encode_cancel, encode_request_open,
};
use veilid_http_transport::{RouteTarget, TransportEvent, VeilidTransport};
use veilid_http_veilid_native::NativeVeilidTransport;
use veilid_http_wire::{Frame, FrameBundle, VEILID_MESSAGE_LIMIT};

const REQUEST_INPUT_CHANNEL_FRAMES: usize = 4;

/// Complete response convenience type built on top of the streaming runtime.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct BufferedResponse {
    /// HTTP response status and headers.
    pub head: ResponseHead,
    /// Complete logical response body.
    pub body: Bytes,
}

/// One event produced for a locally consuming HTTP response.
#[derive(Debug)]
pub enum ClientResponseEvent {
    /// Upstream status and end-to-end response headers are available.
    Head(ResponseHead),
    /// One decompressed response-body chunk.
    Data(Bytes),
    /// The response stream completed and passed length/digest verification.
    End,
    /// The request failed before normal completion.
    Error(String),
}

/// Handle returned immediately for one streaming HTTP transaction.
#[derive(Debug)]
pub struct ClientRequest {
    /// Bounded request-body input. Dropping the sender marks the request body complete.
    pub request_body: Option<mpsc::Sender<Bytes>>,
    /// Credits returned after one IPC request-body frame has entered the VHTTP sender.
    pub request_credits: Option<mpsc::Receiver<u32>>,
    /// Initial request-body frames the local IPC producer may submit.
    pub initial_request_credits: u32,
    /// Bounded response event stream. Consuming it controls Veilid response ACK progress.
    pub responses: mpsc::Receiver<ClientResponseEvent>,
    cancel: watch::Sender<bool>,
}

impl ClientRequest {
    /// Request cancellation of the remote transaction.
    #[allow(dead_code)]
    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    /// Clone a cancellation handle for an external connection registry.
    #[must_use]
    pub fn cancellation_handle(&self) -> watch::Sender<bool> {
        self.cancel.clone()
    }
}

#[derive(Debug, Clone)]
struct ReturnRoute {
    target: RouteTarget,
    blob: Bytes,
    fingerprint: String,
}

#[derive(Debug)]
struct PendingResponseFrames {
    frames: VecDeque<Bytes>,
    bytes: usize,
    limit: usize,
}

impl PendingResponseFrames {
    fn new(limit: usize) -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
            limit,
        }
    }

    fn push(&mut self, frame: Bytes) -> Result<()> {
        let next = self.bytes.saturating_add(frame.len());
        if next > self.limit {
            bail!(
                "response frames received before ResponseOpen exceeded the {} byte bound",
                self.limit
            );
        }
        self.bytes = next;
        self.frames.push_back(frame);
        Ok(())
    }

    fn pop(&mut self) -> Option<Bytes> {
        let frame = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(frame.len());
        Some(frame)
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

/// Native client runtime with one rotating private return route.
pub struct ClientRuntime {
    transport: Arc<NativeVeilidTransport>,
    data_dir: PathBuf,
    return_route: RwLock<ReturnRoute>,
    transactions: Mutex<HashMap<[u8; 16], mpsc::UnboundedSender<Bytes>>>,
    clock: Instant,
    window_frames: usize,
    frame_bytes: usize,
    max_pending_bytes: usize,
    max_out_of_order_bytes: usize,
    max_request_bytes: u64,
    max_response_bytes: u64,
    overall_timeout: Duration,
}

impl std::fmt::Debug for ClientRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientRuntime")
            .field("data_dir", &self.data_dir)
            .field("window_frames", &self.window_frames)
            .field("frame_bytes", &self.frame_bytes)
            .finish_non_exhaustive()
    }
}

impl ClientRuntime {
    /// Allocate the client return route, persist its publishable blob, and begin update dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error when route allocation or persistence fails.
    pub async fn start(
        transport: Arc<NativeVeilidTransport>,
        data_dir: PathBuf,
    ) -> Result<Arc<Self>> {
        let route = allocate_return_route(&transport, &data_dir).await?;
        let runtime = Arc::new(Self {
            transport,
            data_dir,
            return_route: RwLock::new(route),
            transactions: Mutex::new(HashMap::new()),
            clock: Instant::now(),
            window_frames: 32,
            frame_bytes: 30 * 1024,
            max_pending_bytes: 8 * 1024 * 1024,
            max_out_of_order_bytes: 4 * 1024 * 1024,
            max_request_bytes: 0,
            max_response_bytes: 0,
            overall_timeout: Duration::from_secs(60 * 60),
        });
        let dispatcher = Arc::clone(&runtime);
        tokio::spawn(async move {
            if let Err(error) = dispatcher.dispatch_updates().await {
                tracing::error!(%error, "native Veilid update dispatcher stopped");
            }
        });
        Ok(runtime)
    }

    /// Import a server RouteBlob into the embedded node.
    ///
    /// # Errors
    ///
    /// Returns an error when Veilid rejects the blob.
    pub async fn import_route(&self, blob: Bytes) -> Result<RouteTarget> {
        self.transport
            .import_route(blob)
            .await
            .context("import private server RouteBlob")
    }

    /// Start a fully streaming HTTP transaction.
    ///
    /// Request input and response output are bounded channels. The runtime does not ACK
    /// response bytes until the response channel accepts the corresponding logical chunk,
    /// allowing IPC and Chromium backpressure to propagate through Veilid to the upstream.
    #[must_use]
    pub fn start_request(
        self: &Arc<Self>,
        server_target: RouteTarget,
        head: RequestHead,
        has_body: bool,
    ) -> ClientRequest {
        let (
            request_sender,
            request_receiver,
            request_credit_sender,
            request_credit_receiver,
            initial_request_credits,
        ) = if has_body {
            let (sender, receiver) = mpsc::channel(REQUEST_INPUT_CHANNEL_FRAMES);
            let (credit_sender, credit_receiver) = mpsc::channel(REQUEST_INPUT_CHANNEL_FRAMES * 2);
            (
                Some(sender),
                Some(receiver),
                Some(credit_sender),
                Some(credit_receiver),
                u32::try_from(REQUEST_INPUT_CHANNEL_FRAMES).unwrap_or(4),
            )
        } else {
            (None, None, None, None, 0)
        };
        let (response_sender, response_receiver) = mpsc::channel(8);
        let (cancel_sender, cancel_receiver) = watch::channel(false);
        let runtime = Arc::clone(self);
        let terminal_sender = response_sender.clone();
        tokio::spawn(async move {
            let result = runtime
                .run_stream(
                    server_target,
                    head,
                    has_body,
                    request_receiver,
                    request_credit_sender,
                    response_sender,
                    cancel_receiver,
                )
                .await;
            match result {
                Ok(()) => {
                    let _ = terminal_sender.send(ClientResponseEvent::End).await;
                }
                Err(error) => {
                    let _ = terminal_sender
                        .send(ClientResponseEvent::Error(error.to_string()))
                        .await;
                }
            }
        });
        ClientRequest {
            request_body: request_sender,
            request_credits: request_credit_receiver,
            initial_request_credits,
            responses: response_receiver,
            cancel: cancel_sender,
        }
    }

    /// Execute a request and collect the response for tests and small control callers.
    ///
    /// # Errors
    ///
    /// Returns any error surfaced by the streaming runtime.
    #[allow(dead_code)]
    pub async fn request_buffered(
        self: &Arc<Self>,
        server_target: &RouteTarget,
        head: RequestHead,
        body: Bytes,
    ) -> Result<BufferedResponse> {
        if self.max_request_bytes != 0
            && u64::try_from(body.len()).unwrap_or(u64::MAX) > self.max_request_bytes
        {
            bail!("request body exceeds configured client limit");
        }
        let request = self.start_request(server_target.clone(), head, !body.is_empty());
        let ClientRequest {
            request_body,
            request_credits: _,
            initial_request_credits: _,
            mut responses,
            cancel: _,
        } = request;
        if let Some(sender) = request_body {
            if !body.is_empty() {
                sender
                    .send(body)
                    .await
                    .map_err(|_| anyhow::anyhow!("streaming request input closed"))?;
            }
            drop(sender);
        }

        let mut response_head = None;
        let mut response_body = Vec::new();
        while let Some(event) = responses.recv().await {
            match event {
                ClientResponseEvent::Head(head) => response_head = Some(head),
                ClientResponseEvent::Data(chunk) => response_body.extend_from_slice(&chunk),
                ClientResponseEvent::End => {
                    return Ok(BufferedResponse {
                        head: response_head.ok_or_else(|| {
                            anyhow::anyhow!("transaction completed without ResponseOpen")
                        })?,
                        body: Bytes::from(response_body),
                    });
                }
                ClientResponseEvent::Error(error) => bail!(error),
            }
        }
        bail!("native response event channel closed before completion")
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_stream(
        &self,
        server_target: RouteTarget,
        head: RequestHead,
        has_body: bool,
        request_input: Option<mpsc::Receiver<Bytes>>,
        request_credits: Option<mpsc::Sender<u32>>,
        response_events: mpsc::Sender<ClientResponseEvent>,
        cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        let transaction_id = rand::random::<[u8; 16]>();
        let (event_sender, mut events) = mpsc::unbounded_channel();
        self.transactions
            .lock()
            .await
            .insert(transaction_id, event_sender);
        let result = self
            .run_stream_inner(
                transaction_id,
                &server_target,
                head,
                has_body,
                request_input,
                request_credits,
                response_events,
                &mut events,
                cancel,
            )
            .await;
        self.transactions.lock().await.remove(&transaction_id);
        if let Err(error) = &result {
            if let Ok(cancel_frame) = encode_cancel(
                transaction_id,
                &veilid_http_stream::Cancel {
                    reason: error.to_string(),
                },
            ) {
                let _ = self
                    .transport
                    .app_message(&server_target, cancel_frame)
                    .await;
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_stream_inner(
        &self,
        transaction_id: [u8; 16],
        server_target: &RouteTarget,
        head: RequestHead,
        has_body: bool,
        mut request_input: Option<mpsc::Receiver<Bytes>>,
        request_credits: Option<mpsc::Sender<u32>>,
        response_events: mpsc::Sender<ClientResponseEvent>,
        events: &mut mpsc::UnboundedReceiver<Bytes>,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<()> {
        let return_route = self.return_route.read().await.clone();
        let opening = encode_request_open(
            transaction_id,
            &RequestOpen {
                head,
                return_route_blob: return_route.blob.to_vec(),
                request_compression: CompressionMode::Zstd,
                request_body: has_body,
                response_receive_window: u32::try_from(self.window_frames)
                    .context("response receive window does not fit u32")?,
            },
            Bytes::new(),
        )?;
        let reply = self
            .transport
            .app_call(server_target, opening)
            .await
            .context("open streamed VHTTP request")?;
        let request_receive_window = match decode(reply)? {
            DecodedFrame::RequestAccepted {
                transaction_id: reply_id,
                value,
            } => {
                if reply_id != transaction_id {
                    bail!("RequestAccepted transaction identifier mismatch");
                }
                if value.request_receive_window == 0 || value.request_receive_window > 64 {
                    bail!("server returned an invalid request receive window");
                }
                value.request_receive_window
            }
            DecodedFrame::Error { value, .. } => {
                bail!(
                    "server rejected RequestOpen: {}: {}",
                    value.code,
                    value.message
                );
            }
            other => bail!("unexpected streamed AppCall reply: {other:?}"),
        };

        let mut request_sender = if has_body {
            let mut sender = OutboundBody::new(
                transaction_id,
                StreamDirection::Request,
                CompressionMode::Zstd,
                3,
                self.frame_bytes,
                256,
                self.window_frames,
                self.max_pending_bytes,
            )?;
            sender.set_peer_window(request_receive_window)?;
            Some(sender)
        } else {
            None
        };
        let mut request_finished = !has_body;
        let mut request_bytes = 0_u64;
        let mut response_open: Option<ResponseOpen> = None;
        let mut response_receiver: Option<InboundBody> = None;
        let mut pending_response_frames = PendingResponseFrames::new(self.max_out_of_order_bytes);
        let mut last_response_ack: Option<Ack> = None;
        let mut response_finished = false;
        let deadline = tokio::time::sleep(self.overall_timeout);
        tokio::pin!(deadline);

        loop {
            if let Some(sender) = request_sender.as_mut() {
                self.dispatch_request_frames(server_target, sender).await?;
            }
            let request_complete = request_sender
                .as_ref()
                .is_none_or(OutboundBody::is_complete);
            if request_complete && response_finished {
                break;
            }

            let can_read_request = !request_finished
                && request_sender
                    .as_ref()
                    .is_some_and(|sender| !sender.is_backpressured());
            tokio::select! {
                frame = events.recv() => {
                    let frame = frame.ok_or_else(|| anyhow::anyhow!("native Veilid event stream closed"))?;
                    match decode(frame.clone())? {
                        DecodedFrame::Ack { transaction_id: id, value }
                            if id == transaction_id && value.direction == StreamDirection::Request =>
                        {
                            let sender = request_sender
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("received request ACK for bodyless request"))?;
                            sender.acknowledge(value)?;
                        }
                        DecodedFrame::ResponseOpen { transaction_id: id, value, initial_payload }
                            if id == transaction_id =>
                        {
                            if !initial_payload.is_empty() {
                                bail!("ResponseOpen initial payload is reserved until sequenced initial-data support is enabled");
                            }
                            if value.request_receive_window > 64 {
                                bail!("server returned an invalid updated request receive window");
                            }
                            if let Some(existing) = response_open.as_ref() {
                                if existing != &value {
                                    bail!("duplicate conflicting ResponseOpen");
                                }
                                self.send_response_ack(
                                    server_target,
                                    transaction_id,
                                    last_response_ack.unwrap_or_else(|| self.empty_response_ack()),
                                )
                                .await?;
                                continue;
                            }
                            if let Some(sender) = request_sender.as_mut() {
                                sender.set_peer_window(value.request_receive_window)?;
                            }
                            response_events
                                .send(ClientResponseEvent::Head(value.head.clone()))
                                .await
                                .map_err(|_| anyhow::anyhow!("local response consumer disconnected"))?;
                            if value.response_body {
                                response_receiver = Some(InboundBody::new(
                                    transaction_id,
                                    StreamDirection::Response,
                                    value.response_compression,
                                    u32::try_from(self.window_frames).unwrap_or(32),
                                    self.max_out_of_order_bytes,
                                    if self.max_response_bytes == 0 { u64::MAX } else { self.max_response_bytes },
                                )?);
                            } else {
                                if !pending_response_frames.is_empty() {
                                    bail!("response body frames arrived for a bodyless response");
                                }
                                response_finished = true;
                            }
                            response_open = Some(value);

                            while let Some(pending) = pending_response_frames.pop() {
                                let receiver = response_receiver
                                    .as_mut()
                                    .ok_or_else(|| anyhow::anyhow!("response body frame arrived for a bodyless response"))?;
                                let (completed, ack, _) = self
                                    .receive_response_frame(
                                        transaction_id,
                                        pending,
                                        receiver,
                                        &response_events,
                                    )
                                    .await?;
                                last_response_ack = Some(ack);
                                response_finished |= completed;
                            }
                            self.send_response_ack(
                                server_target,
                                transaction_id,
                                last_response_ack.unwrap_or_else(|| self.empty_response_ack()),
                            )
                            .await?;
                        }
                        DecodedFrame::Data { transaction_id: id, direction: StreamDirection::Response, .. }
                        | DecodedFrame::End { transaction_id: id, value: veilid_http_stream::StreamEnd { direction: StreamDirection::Response, .. }, .. }
                            if id == transaction_id =>
                        {
                            let Some(receiver) = response_receiver.as_mut() else {
                                pending_response_frames.push(frame)?;
                                continue;
                            };
                            let (completed, ack, sequence) = self
                                .receive_response_frame(
                                    transaction_id,
                                    frame,
                                    receiver,
                                    &response_events,
                                )
                                .await?;
                            last_response_ack = Some(ack);
                            if completed || ack.selective != 0 || sequence & 3 == 3 {
                                self.send_response_ack(server_target, transaction_id, ack).await?;
                            }
                            response_finished = completed;
                        }
                        DecodedFrame::Error { transaction_id: id, value } if id == transaction_id => {
                            bail!("server stream error {}: {}", value.code, value.message);
                        }
                        DecodedFrame::Cancel { transaction_id: id, value } if id == transaction_id => {
                            bail!("server cancelled transaction: {}", value.reason);
                        }
                        DecodedFrame::RequestAccepted { .. } => {}
                        other => tracing::debug!(?other, "ignored unrelated client stream frame"),
                    }
                }
                chunk = async {
                    match request_input.as_mut() {
                        Some(receiver) => receiver.recv().await,
                        None => None,
                    }
                }, if can_read_request => {
                    match chunk {
                        Some(chunk) => {
                            request_bytes = request_bytes
                                .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                                .ok_or_else(|| anyhow::anyhow!("request length overflow"))?;
                            if self.max_request_bytes != 0 && request_bytes > self.max_request_bytes {
                                bail!("request body exceeds configured client limit");
                            }
                            let sender = request_sender
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("request body channel exists without sender"))?;
                            let mut offset = 0;
                            while offset < chunk.len() {
                                let end = offset
                                    .saturating_add(sender.max_input_chunk())
                                    .min(chunk.len());
                                sender.push(&chunk[offset..end], false)?;
                                offset = end;
                            }
                            if let Some(credits) = request_credits.as_ref() {
                                let _ = credits.send(1).await;
                            }
                        }
                        None => {
                            let sender = request_sender
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("request body channel exists without sender"))?;
                            sender.finish_input()?;
                            request_finished = true;
                        }
                    }
                }
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        bail!("local HTTP request was cancelled");
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    if let Some(sender) = request_sender.as_mut() {
                        self.dispatch_request_frames(server_target, sender).await?;
                    }
                }
                () = &mut deadline => {
                    bail!("VHTTP transaction exceeded overall timeout");
                }
            }
        }
        if response_open.is_none() {
            bail!("transaction completed without ResponseOpen");
        }
        Ok(())
    }

    fn empty_response_ack(&self) -> Ack {
        Ack {
            direction: StreamDirection::Response,
            cumulative: None,
            selective: 0,
            receive_window: u32::try_from(self.window_frames).unwrap_or(32),
        }
    }

    async fn receive_response_frame(
        &self,
        transaction_id: [u8; 16],
        encoded: Bytes,
        receiver: &mut InboundBody,
        response_events: &mpsc::Sender<ClientResponseEvent>,
    ) -> Result<(bool, Ack, u32)> {
        let sequence = match decode(encoded.clone())? {
            DecodedFrame::Data {
                transaction_id: id,
                direction: StreamDirection::Response,
                sequence,
                ..
            }
            | DecodedFrame::End {
                transaction_id: id,
                sequence,
                value:
                    veilid_http_stream::StreamEnd {
                        direction: StreamDirection::Response,
                        ..
                    },
            } if id == transaction_id => sequence,
            _ => bail!("expected response data or end frame"),
        };
        let output = receiver.receive(encoded)?;
        for chunk in output.logical_chunks {
            response_events
                .send(ClientResponseEvent::Data(chunk))
                .await
                .map_err(|_| anyhow::anyhow!("local response consumer disconnected"))?;
        }
        Ok((output.completed, output.ack, sequence))
    }

    async fn send_response_ack(
        &self,
        server_target: &RouteTarget,
        transaction_id: [u8; 16],
        ack: Ack,
    ) -> Result<()> {
        self.send_encoded_frames(server_target, vec![encode_ack(transaction_id, ack)?])
            .await
            .context("send response-stream ACK")
    }

    async fn dispatch_request_frames(
        &self,
        target: &RouteTarget,
        sender: &mut OutboundBody,
    ) -> Result<()> {
        let now = u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX);
        let frames = sender
            .take_sendable(
                now,
                RetryPolicy {
                    initial_ms: 250,
                    maximum_ms: 10_000,
                },
            )
            .into_iter()
            .map(|frame| frame.encoded)
            .collect();
        self.send_encoded_frames(target, frames).await
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
            .context("send or retry VHTTP stream frames")
    }

    async fn dispatch_updates(self: Arc<Self>) -> Result<()> {
        loop {
            match self.transport.next_event().await? {
                TransportEvent::AppMessage { route, payload } => {
                    let current = self.return_route.read().await.clone();
                    if route.as_ref() != Some(&current.target) {
                        tracing::debug!(
                            ?route,
                            "ignored AppMessage for a non-current client return route"
                        );
                        continue;
                    }
                    for encoded in split_frames(payload)? {
                        let transaction_id = Frame::decode(encoded.clone())?.transaction_id;
                        if let Some(sender) =
                            self.transactions.lock().await.get(&transaction_id).cloned()
                        {
                            let _ = sender.send(encoded);
                        }
                    }
                }
                TransportEvent::AppCall { call_id, .. } => {
                    tracing::warn!(%call_id, "unexpected inbound AppCall on client return route");
                }
                TransportEvent::RouteChanged { route, dead: true } => {
                    let current = self.return_route.read().await.clone();
                    if route == current.target {
                        tracing::warn!(fingerprint = %current.fingerprint, "client return route died; rotating and failing active requests");
                        self.transactions.lock().await.clear();
                        let replacement =
                            allocate_return_route(&self.transport, &self.data_dir).await?;
                        *self.return_route.write().await = replacement;
                    }
                }
                TransportEvent::RouteChanged { .. } => {}
                TransportEvent::Shutdown => bail!("embedded Veilid node shut down"),
            }
        }
    }
}

async fn allocate_return_route(
    transport: &NativeVeilidTransport,
    data_dir: &Path,
) -> Result<ReturnRoute> {
    let (target, blob) = transport
        .allocate_route()
        .await
        .context("allocate client private return route")?;
    let route = ReturnRoute {
        target,
        fingerprint: veilid_http_route::fingerprint(&blob),
        blob,
    };
    persist_return_route(data_dir, &route)?;
    tracing::info!(
        event = "VHTTP_CLIENT_RETURN_ROUTE_READY",
        fingerprint = %route.fingerprint,
        route_id = %route.target.0,
        "client private return route is ready"
    );
    Ok(route)
}

fn persist_return_route(data_dir: &Path, route: &ReturnRoute) -> Result<()> {
    let directory = data_dir.join("return-route");
    fs::create_dir_all(&directory).with_context(|| format!("create {}", directory.display()))?;
    atomic_write(&directory.join("current.blob"), &route.blob)?;
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs();
    atomic_write(
        &directory.join("current.json"),
        &serde_json::to_vec_pretty(&serde_json::json!({
            "schema": "org.veilidhttp.client-return-route/v1",
            "routeId": route.target.0,
            "fingerprint": route.fingerprint,
            "createdAtUnixSeconds": created_at,
        }))?,
    )?;
    Ok(())
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, contents).with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

fn split_frames(payload: Bytes) -> Result<Vec<Bytes>> {
    if let Ok(bundle) = FrameBundle::decode(payload.clone()) {
        return Ok(bundle.frames);
    }
    Frame::decode(payload.clone()).context("decode VHTTP client AppMessage")?;
    Ok(vec![payload])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_open_response_buffer_is_strictly_bounded() {
        let mut pending = PendingResponseFrames::new(5);
        pending.push(Bytes::from_static(b"abc")).unwrap();
        assert!(pending.push(Bytes::from_static(b"def")).is_err());
        assert_eq!(pending.pop().as_deref(), Some(b"abc".as_slice()));
        assert!(pending.is_empty());
    }
}
