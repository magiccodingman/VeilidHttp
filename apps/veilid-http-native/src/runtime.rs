//! Native VHTTP client runtime over embedded `veilid-core`.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock, mpsc};
use veilid_http_core::RetryPolicy;
use veilid_http_engine::{InboundBody, OutboundBody};
use veilid_http_http::{RequestHead, ResponseHead};
use veilid_http_stream::{
    CompressionMode, DecodedFrame, RequestOpen, StreamDirection, decode, encode_ack,
    encode_cancel, encode_request_open,
};
use veilid_http_transport::{RouteTarget, TransportEvent, VeilidTransport};
use veilid_http_veilid_native::NativeVeilidTransport;
use veilid_http_wire::{Frame, FrameBundle};

/// Complete response used by the initial buffered Electron IPC compatibility path.
#[derive(Debug, Clone)]
pub struct BufferedResponse {
    /// HTTP response status and headers.
    pub head: ResponseHead,
    /// Complete logical response body.
    pub body: Bytes,
}

#[derive(Debug, Clone)]
struct ReturnRoute {
    target: RouteTarget,
    blob: Bytes,
    fingerprint: String,
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

    /// Execute one HTTP request through the full streamed VHTTP path.
    ///
    /// The current Electron compatibility caller provides a buffered body and receives a
    /// buffered response. Network transport between the native client and bridge is fully
    /// streaming, bounded, acknowledged, and retryable; IPC streaming replaces this final
    /// compatibility boundary separately.
    ///
    /// # Errors
    ///
    /// Returns an error for route, protocol, timeout, transport, compression, integrity,
    /// remote cancellation, or response-size failure.
    pub async fn request_buffered(
        &self,
        server_target: &RouteTarget,
        head: RequestHead,
        body: Bytes,
    ) -> Result<BufferedResponse> {
        if self.max_request_bytes != 0
            && u64::try_from(body.len()).unwrap_or(u64::MAX) > self.max_request_bytes
        {
            bail!("request body exceeds configured client limit");
        }
        let transaction_id = rand::random::<[u8; 16]>();
        let (event_sender, mut events) = mpsc::unbounded_channel();
        self.transactions.lock().await.insert(transaction_id, event_sender);
        let result = self
            .request_buffered_inner(
                transaction_id,
                server_target,
                head,
                body,
                &mut events,
            )
            .await;
        self.transactions.lock().await.remove(&transaction_id);
        result
    }

    async fn request_buffered_inner(
        &self,
        transaction_id: [u8; 16],
        server_target: &RouteTarget,
        head: RequestHead,
        body: Bytes,
        events: &mut mpsc::UnboundedReceiver<Bytes>,
    ) -> Result<BufferedResponse> {
        let return_route = self.return_route.read().await.clone();
        let request_body = !body.is_empty();
        let opening = encode_request_open(
            transaction_id,
            &RequestOpen {
                head,
                return_route_blob: return_route.blob.to_vec(),
                request_compression: CompressionMode::Zstd,
                request_body,
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
        match decode(reply)? {
            DecodedFrame::RequestAccepted { transaction_id: reply_id, value } => {
                if reply_id != transaction_id {
                    bail!("RequestAccepted transaction identifier mismatch");
                }
                if value.request_receive_window > 64 {
                    bail!("server returned an invalid request receive window");
                }
            }
            DecodedFrame::Error { value, .. } => {
                bail!("server rejected RequestOpen: {}: {}", value.code, value.message);
            }
            other => bail!("unexpected streamed AppCall reply: {other:?}"),
        }

        let mut request_sender = if request_body {
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
            sender.set_peer_window(u32::try_from(self.window_frames).unwrap_or(32))?;
            Some(sender)
        } else {
            None
        };
        let mut request_offset = 0;
        let mut request_finished = !request_body;
        let mut response_head: Option<ResponseHead> = None;
        let mut response_receiver: Option<InboundBody> = None;
        let mut response_body = Vec::new();
        let mut response_finished = false;
        let deadline = tokio::time::sleep(self.overall_timeout);
        tokio::pin!(deadline);

        loop {
            if let Some(sender) = request_sender.as_mut() {
                while request_offset < body.len() && !sender.is_backpressured() {
                    let limit = sender.max_input_chunk();
                    let end = request_offset.saturating_add(limit).min(body.len());
                    sender.push(&body[request_offset..end], end == body.len())?;
                    request_offset = end;
                }
                if request_offset == body.len() && !request_finished {
                    sender.finish_input()?;
                    request_finished = true;
                }
                self.dispatch_request_frames(server_target, sender).await?;
            }

            let request_complete = request_sender
                .as_ref()
                .is_none_or(OutboundBody::is_complete);
            if request_complete && response_finished {
                break;
            }

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
                            if response_head.is_some() {
                                bail!("duplicate conflicting ResponseOpen");
                            }
                            if !initial_payload.is_empty() {
                                bail!("ResponseOpen initial payload is reserved until sequenced initial-data support is enabled");
                            }
                            response_head = Some(value.head);
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
                                response_finished = true;
                            }
                        }
                        DecodedFrame::Data { transaction_id: id, direction: StreamDirection::Response, sequence, .. }
                        | DecodedFrame::End { transaction_id: id, sequence, value: veilid_http_stream::StreamEnd { direction: StreamDirection::Response, .. } }
                            if id == transaction_id =>
                        {
                            let receiver = response_receiver
                                .as_mut()
                                .ok_or_else(|| anyhow::anyhow!("response data arrived before ResponseOpen"))?;
                            let output = receiver.receive(frame)?;
                            for chunk in output.logical_chunks {
                                response_body.extend_from_slice(&chunk);
                            }
                            if output.completed || output.ack.selective != 0 || sequence % 4 == 3 {
                                self.transport
                                    .app_message(server_target, encode_ack(transaction_id, output.ack)?)
                                    .await
                                    .context("send response-stream ACK")?;
                            }
                            response_finished = output.completed;
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
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    if let Some(sender) = request_sender.as_mut() {
                        self.dispatch_request_frames(server_target, sender).await?;
                    }
                }
                () = &mut deadline => {
                    let _ = self.transport.app_message(
                        server_target,
                        encode_cancel(
                            transaction_id,
                            &veilid_http_stream::Cancel { reason: "client overall timeout".to_owned() },
                        )?,
                    ).await;
                    bail!("VHTTP transaction exceeded overall timeout");
                }
            }
        }

        Ok(BufferedResponse {
            head: response_head.ok_or_else(|| anyhow::anyhow!("transaction completed without ResponseOpen"))?,
            body: Bytes::from(response_body),
        })
    }

    async fn dispatch_request_frames(
        &self,
        target: &RouteTarget,
        sender: &mut OutboundBody,
    ) -> Result<()> {
        let now = u64::try_from(self.clock.elapsed().as_millis()).unwrap_or(u64::MAX);
        for frame in sender.take_sendable(
            now,
            RetryPolicy {
                initial_ms: 250,
                maximum_ms: 10_000,
            },
        ) {
            self.transport
                .app_message(target, frame.encoded)
                .await
                .context("send or retry request stream frame")?;
        }
        Ok(())
    }

    async fn dispatch_updates(self: Arc<Self>) -> Result<()> {
        loop {
            match self.transport.next_event().await? {
                TransportEvent::AppMessage { route, payload } => {
                    let current = self.return_route.read().await.clone();
                    if route.as_ref() != Some(&current.target) {
                        tracing::debug!(?route, "ignored AppMessage for a non-current client return route");
                        continue;
                    }
                    for encoded in split_frames(payload)? {
                        let transaction_id = Frame::decode(encoded.clone())?.transaction_id;
                        if let Some(sender) = self.transactions.lock().await.get(&transaction_id).cloned() {
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
                        let replacement = allocate_return_route(&self.transport, &self.data_dir).await?;
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
    fs::create_dir_all(&directory)
        .with_context(|| format!("create {}", directory.display()))?;
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
