//! Multiplexed client for the official `veilid-server` JSON remote API.

use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{Mutex, mpsc, oneshot},
};
use veilid_core::{OperationId, RouteId, VeilidAPIError, VeilidUpdate};
use veilid_http_transport::{RouteTarget, TransportError, TransportEvent, VeilidTransport};
use veilid_remote_api::{
    ApiResult, ApiResultWithString, ApiResultWithVecU8, RecvMessage, Request, RequestOp,
    ResponseOp, RoutingContextRequest, RoutingContextRequestOp, RoutingContextResponseOp,
};

const MAX_REMOTE_LINE_BYTES: usize = 40 * 1024 * 1024;

/// Address of the internal `veilid-server` client API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEndpoint {
    /// Loopback TCP client API endpoint.
    Tcp(SocketAddr),
    /// Unix-domain socket endpoint.
    #[cfg(unix)]
    Unix(PathBuf),
}

impl FromStr for RemoteEndpoint {
    type Err = TransportError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if let Some(address) = value.strip_prefix("tcp://") {
            return address.parse().map(Self::Tcp).map_err(|error| {
                TransportError::Fatal(format!("invalid Veilid TCP endpoint: {error}"))
            });
        }
        #[cfg(unix)]
        if let Some(path) = value.strip_prefix("unix://") {
            return Ok(Self::Unix(PathBuf::from(path)));
        }
        value.parse().map(Self::Tcp).map_err(|error| {
            TransportError::Fatal(format!("invalid Veilid client endpoint: {error}"))
        })
    }
}

/// Connected official remote-API transport.
pub struct RemoteVeilidTransport {
    requests: mpsc::Sender<Request>,
    pending: Arc<Mutex<HashMap<u32, oneshot::Sender<ResponseOp>>>>,
    events: Mutex<mpsc::UnboundedReceiver<TransportEvent>>,
    pending_calls: Arc<Mutex<HashMap<String, OperationId>>>,
    next_request_id: AtomicU32,
    request_timeout: Duration,
    routing_context_id: u32,
    server_version: String,
}

impl std::fmt::Debug for RemoteVeilidTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteVeilidTransport")
            .field("routing_context_id", &self.routing_context_id)
            .field("server_version", &self.server_version)
            .finish_non_exhaustive()
    }
}

impl RemoteVeilidTransport {
    /// Connect to `veilid-server`, attach it, and create one default-safe routing context.
    ///
    /// # Errors
    ///
    /// Returns a transport error when the socket cannot be opened, the remote JSON
    /// protocol fails, attachment fails, or no routing context can be allocated.
    pub async fn connect(
        endpoint: RemoteEndpoint,
        request_timeout: Duration,
    ) -> Result<Self, TransportError> {
        match endpoint {
            RemoteEndpoint::Tcp(address) => {
                let stream = TcpStream::connect(address)
                    .await
                    .map_err(|error| TransportError::Retryable(error.to_string()))?;
                Self::from_stream(stream, request_timeout).await
            }
            #[cfg(unix)]
            RemoteEndpoint::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(&path)
                    .await
                    .map_err(|error| {
                        TransportError::Retryable(format!("{}: {error}", path.display()))
                    })?;
                Self::from_stream(stream, request_timeout).await
            }
        }
    }

    /// Version reported by the connected `veilid-server`.
    #[must_use]
    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    async fn from_stream<S>(stream: S, request_timeout: Duration) -> Result<Self, TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, mut write_half) = tokio::io::split(stream);
        let (request_sender, mut request_receiver) = mpsc::channel::<Request>(256);
        let pending: Arc<Mutex<HashMap<u32, oneshot::Sender<ResponseOp>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let reader_pending = Arc::clone(&pending);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let pending_calls = Arc::new(Mutex::new(HashMap::new()));
        let reader_calls = Arc::clone(&pending_calls);

        tokio::spawn(async move {
            while let Some(request) = request_receiver.recv().await {
                let mut encoded = match serde_json::to_vec(&request) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        tracing::error!(%error, "failed to encode Veilid remote request");
                        break;
                    }
                };
                encoded.push(b'\n');
                if let Err(error) = write_half.write_all(&encoded).await {
                    tracing::error!(%error, "failed to write Veilid remote request");
                    break;
                }
                if let Err(error) = write_half.flush().await {
                    tracing::error!(%error, "failed to flush Veilid remote request");
                    break;
                }
            }
        });

        tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(%error, "failed to read Veilid remote message");
                        break;
                    }
                }
                if line.len() > MAX_REMOTE_LINE_BYTES {
                    tracing::error!(
                        bytes = line.len(),
                        "Veilid remote message exceeded safety bound"
                    );
                    break;
                }
                while matches!(line.last(), Some(b'\n' | b'\r')) {
                    line.pop();
                }
                let message = match serde_json::from_slice::<RecvMessage>(&line) {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::error!(%error, "failed to decode Veilid remote message");
                        break;
                    }
                };
                match message {
                    RecvMessage::Response(response) => {
                        if let Some(sender) = reader_pending.lock().await.remove(&response.id) {
                            let _ = sender.send(response.op);
                        }
                    }
                    RecvMessage::Update(update) => {
                        map_update(update, &event_sender, &reader_calls).await;
                    }
                }
            }
            reader_pending.lock().await.clear();
            let _ = event_sender.send(TransportEvent::Shutdown);
        });

        let mut client = Self {
            requests: request_sender,
            pending,
            events: Mutex::new(event_receiver),
            pending_calls,
            next_request_id: AtomicU32::new(1),
            request_timeout,
            routing_context_id: 0,
            server_version: String::new(),
        };

        client.server_version = match client.request(RequestOp::VeilidVersionString).await? {
            ResponseOp::VeilidVersionString { value } => value,
            _ => {
                return Err(TransportError::Fatal(
                    "unexpected version response".to_owned(),
                ));
            }
        };

        if let ResponseOp::Attach { result } = client.request(RequestOp::Attach).await? {
            if let Err(error) = unwrap_api_result(result) {
                if !error.to_string().to_ascii_lowercase().contains("already") {
                    return Err(error);
                }
            }
        } else {
            return Err(TransportError::Fatal(
                "unexpected attach response".to_owned(),
            ));
        }

        client.routing_context_id = match client.request(RequestOp::NewRoutingContext).await? {
            ResponseOp::NewRoutingContext { result } => unwrap_api_result(result)?,
            _ => {
                return Err(TransportError::Fatal(
                    "unexpected routing-context response".to_owned(),
                ));
            }
        };
        Ok(client)
    }

    async fn request(&self, op: RequestOp) -> Result<ResponseOp, TransportError> {
        let mut id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        if id == 0 {
            id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        }
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(id, sender);
        if self.requests.send(Request { id, op }).await.is_err() {
            self.pending.lock().await.remove(&id);
            return Err(TransportError::Shutdown);
        }
        match tokio::time::timeout(self.request_timeout, receiver).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(TransportError::Shutdown),
            Err(_) => {
                self.pending.lock().await.remove(&id);
                Err(TransportError::Timeout)
            }
        }
    }

    async fn routing_request(
        &self,
        rc_op: RoutingContextRequestOp,
    ) -> Result<RoutingContextResponseOp, TransportError> {
        match self
            .request(RequestOp::RoutingContext(RoutingContextRequest {
                rc_id: self.routing_context_id,
                rc_op,
            }))
            .await?
        {
            ResponseOp::RoutingContext(response) if response.rc_id == self.routing_context_id => {
                Ok(response.rc_op)
            }
            _ => Err(TransportError::Fatal(
                "unexpected routing-context operation response".to_owned(),
            )),
        }
    }
}

async fn map_update(
    update: VeilidUpdate,
    sender: &mpsc::UnboundedSender<TransportEvent>,
    pending_calls: &Mutex<HashMap<String, OperationId>>,
) {
    match update {
        VeilidUpdate::AppMessage(message) => {
            let route = message
                .route_id()
                .map(|route| RouteTarget(route.to_string()));
            let _ = sender.send(TransportEvent::AppMessage {
                route,
                payload: Bytes::copy_from_slice(message.message()),
            });
        }
        VeilidUpdate::AppCall(call) => {
            let call_id = call.id().to_string();
            pending_calls
                .lock()
                .await
                .insert(call_id.clone(), call.id());
            let route = call.route_id().map(|route| RouteTarget(route.to_string()));
            let _ = sender.send(TransportEvent::AppCall {
                call_id,
                route,
                payload: Bytes::copy_from_slice(call.message()),
            });
        }
        VeilidUpdate::RouteChange(change) => {
            for route in change.dead_routes.iter().chain(&change.dead_remote_routes) {
                let _ = sender.send(TransportEvent::RouteChanged {
                    route: RouteTarget(route.to_string()),
                    dead: true,
                });
            }
        }
        VeilidUpdate::Shutdown => {
            let _ = sender.send(TransportEvent::Shutdown);
        }
        VeilidUpdate::Log(_)
        | VeilidUpdate::Attachment(_)
        | VeilidUpdate::Network(_)
        | VeilidUpdate::Config(_)
        | VeilidUpdate::ValueChange(_) => {}
    }
}

fn parse_route(target: &RouteTarget) -> Result<RouteId, TransportError> {
    RouteId::try_from(target.0.as_str())
        .map_err(|error| TransportError::InvalidTarget(error.to_string()))
}

fn unwrap_api_result<T>(result: ApiResult<T>) -> Result<T, TransportError> {
    match result {
        ApiResult::Ok { value } => Ok(value),
        ApiResult::Err { error } => Err(classify_error(error)),
    }
}

fn unwrap_string_result<T>(result: ApiResultWithString<T>) -> Result<String, TransportError> {
    match result {
        ApiResultWithString::Ok { value } => Ok(value),
        ApiResultWithString::Err { error } => Err(classify_error(error)),
    }
}

fn unwrap_vec_result(result: ApiResultWithVecU8) -> Result<Vec<u8>, TransportError> {
    match result {
        ApiResultWithVecU8::Ok { value } => Ok(value),
        ApiResultWithVecU8::Err { error } => Err(classify_error(error)),
    }
}

fn classify_error(error: VeilidAPIError) -> TransportError {
    let message = error.to_string();
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("timeout") {
        TransportError::Timeout
    } else if normalized.contains("no connection")
        || normalized.contains("try again")
        || normalized.contains("temporarily")
        || normalized.contains("not attached")
    {
        TransportError::Retryable(message)
    } else {
        TransportError::Fatal(message)
    }
}

#[async_trait]
impl VeilidTransport for RemoteVeilidTransport {
    async fn import_route(&self, route_blob: Bytes) -> Result<RouteTarget, TransportError> {
        match self
            .request(RequestOp::ImportRemotePrivateRoute {
                blob: route_blob.to_vec(),
            })
            .await?
        {
            ResponseOp::ImportRemotePrivateRoute { result } => {
                Ok(RouteTarget(unwrap_string_result(result)?))
            }
            _ => Err(TransportError::Fatal(
                "unexpected import-route response".to_owned(),
            )),
        }
    }

    async fn allocate_route(&self) -> Result<(RouteTarget, Bytes), TransportError> {
        match self.request(RequestOp::NewPrivateRoute).await? {
            ResponseOp::NewPrivateRoute { result } => {
                let route = unwrap_api_result(result)?;
                Ok((
                    RouteTarget(route.route_id.to_string()),
                    Bytes::from(route.blob),
                ))
            }
            _ => Err(TransportError::Fatal(
                "unexpected new-route response".to_owned(),
            )),
        }
    }

    async fn release_route(&self, target: &RouteTarget) -> Result<(), TransportError> {
        match self
            .request(RequestOp::ReleasePrivateRoute {
                route_id: parse_route(target)?,
            })
            .await?
        {
            ResponseOp::ReleasePrivateRoute { result } => unwrap_api_result(result),
            _ => Err(TransportError::Fatal(
                "unexpected release-route response".to_owned(),
            )),
        }
    }

    async fn app_call(
        &self,
        target: &RouteTarget,
        payload: Bytes,
    ) -> Result<Bytes, TransportError> {
        match self
            .routing_request(RoutingContextRequestOp::AppCall {
                target: parse_route(target)?.into(),
                message: payload.to_vec(),
            })
            .await?
        {
            RoutingContextResponseOp::AppCall { result } => {
                Ok(Bytes::from(unwrap_vec_result(result)?))
            }
            _ => Err(TransportError::Fatal(
                "unexpected AppCall response".to_owned(),
            )),
        }
    }

    async fn app_message(
        &self,
        target: &RouteTarget,
        payload: Bytes,
    ) -> Result<(), TransportError> {
        match self
            .routing_request(RoutingContextRequestOp::AppMessage {
                target: parse_route(target)?.into(),
                message: payload.to_vec(),
            })
            .await?
        {
            RoutingContextResponseOp::AppMessage { result } => unwrap_api_result(result),
            _ => Err(TransportError::Fatal(
                "unexpected AppMessage response".to_owned(),
            )),
        }
    }

    async fn app_call_reply(&self, call_id: &str, payload: Bytes) -> Result<(), TransportError> {
        let operation_id = self
            .pending_calls
            .lock()
            .await
            .remove(call_id)
            .ok_or_else(|| {
                TransportError::InvalidTarget("unknown or already-replied AppCall".to_owned())
            })?;
        match self
            .request(RequestOp::AppCallReply {
                call_id: operation_id,
                message: payload.to_vec(),
            })
            .await?
        {
            ResponseOp::AppCallReply { result } => unwrap_api_result(result),
            _ => Err(TransportError::Fatal(
                "unexpected AppCall reply response".to_owned(),
            )),
        }
    }

    async fn next_event(&self) -> Result<TransportEvent, TransportError> {
        self.events
            .lock()
            .await
            .recv()
            .await
            .ok_or(TransportError::Shutdown)
    }
}
