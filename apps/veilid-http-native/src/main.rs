//! Trusted Electron sidecar: native Veilid transport plus bounded local IPC.

mod runtime;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::Parser;
use runtime::{ClientResponseEvent, ClientRuntime};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use veilid_http_http::{HeaderField, RequestHead};
use veilid_http_ipc::{
    FrameKind, Hello, IpcFrame, MAX_STREAM_CREDITS, StreamCredit, read_frame, write_frame,
};
use veilid_http_transport::RouteTarget;
use veilid_http_veilid_native::{NativeTransportConfig, NativeVeilidTransport};

#[derive(Debug, Parser)]
#[command(version, about = "Native VeilidHttp client sidecar")]
struct Config {
    /// Private state controlled by the trusted Electron main process.
    #[arg(long, env = "VHTTP_CLIENT_DATA_DIR", default_value = "./data/client")]
    data_dir: PathBuf,
    /// Unix-domain socket or Windows named-pipe path selected by Electron.
    #[arg(long, env = "VHTTP_IPC_PATH")]
    ipc_path: String,
    /// Random launch secret supplied only through the child-process environment.
    #[arg(long, env = "VHTTP_IPC_SECRET", hide_env_values = true)]
    ipc_secret: String,
    /// Process-wide limit for simultaneously active browser HTTP requests.
    #[arg(long, env = "VHTTP_CLIENT_MAX_ACTIVE_REQUESTS", default_value_t = 64)]
    max_active_requests: usize,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
enum Request {
    Health,
    Fingerprint {
        route_blob_base64: String,
    },
    ImportRoute {
        route_blob_base64: String,
    },
    Environment,
    HttpRequest {
        site_id: String,
        method: String,
        path_and_query: String,
        headers: Vec<(String, String)>,
        #[serde(default)]
        has_body: bool,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Response<T> {
    ok: bool,
    result: Option<T>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Environment {
    environment: &'static str,
    transport: &'static str,
    protocol_version: u8,
    capabilities: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HttpResponseMetadata {
    status: u16,
    headers: Vec<(String, String)>,
}

struct State {
    data_dir: PathBuf,
    runtime: Arc<ClientRuntime>,
    imported_routes: Mutex<HashMap<String, RouteTarget>>,
    active_requests: Arc<Semaphore>,
}

#[derive(Debug)]
struct ActiveRequest {
    request_body: Option<mpsc::Sender<Bytes>>,
    response_credits: Arc<Semaphore>,
    cancel: watch::Sender<bool>,
    _permit: OwnedSemaphorePermit,
}

fn route_path(data_dir: &Path, site_id: &str) -> PathBuf {
    data_dir.join("routes").join(format!("{site_id}.blob"))
}

fn persist_route(data_dir: &Path, blob: &[u8]) -> Result<String> {
    let fingerprint = veilid_http_route::fingerprint(blob);
    let routes = data_dir.join("routes");
    fs::create_dir_all(&routes).with_context(|| format!("create {}", routes.display()))?;
    let destination = route_path(data_dir, &fingerprint);
    let temporary = destination.with_extension("blob.tmp");
    fs::write(&temporary, blob).with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, &destination)
        .with_context(|| format!("atomically replace {}", destination.display()))?;
    Ok(fingerprint)
}

fn success<T: Serialize>(value: T) -> Result<Response<serde_json::Value>> {
    Ok(Response {
        ok: true,
        result: Some(serde_json::to_value(value)?),
        error: None,
    })
}

fn failure(error: impl std::fmt::Display) -> Response<serde_json::Value> {
    Response {
        ok: false,
        result: None,
        error: Some(error.to_string()),
    }
}

impl State {
    async fn import_route(&self, blob: Bytes) -> Result<(String, RouteTarget)> {
        let fingerprint = persist_route(&self.data_dir, &blob)?;
        let target = self.runtime.import_route(blob).await?;
        self.imported_routes
            .lock()
            .await
            .insert(fingerprint.clone(), target.clone());
        Ok((fingerprint, target))
    }

    async fn target_for_site(&self, site_id: &str) -> Result<RouteTarget> {
        if let Some(target) = self.imported_routes.lock().await.get(site_id).cloned() {
            return Ok(target);
        }
        let path = route_path(&self.data_dir, site_id);
        let blob = fs::read(&path)
            .with_context(|| format!("read imported route {}", path.display()))?;
        let (fingerprint, target) = self.import_route(Bytes::from(blob)).await?;
        if fingerprint != site_id {
            bail!("persisted RouteBlob fingerprint does not match requested site identifier");
        }
        Ok(target)
    }

    async fn handle_control(
        &self,
        request: Request,
    ) -> Result<(Response<serde_json::Value>, Bytes)> {
        match request {
            Request::Health => Ok((success(serde_json::json!({ "status": "ready" }))?, Bytes::new())),
            Request::Environment => Ok((
                success(Environment {
                    environment: "electron",
                    transport: "veilid-native",
                    protocol_version: 1,
                    capabilities: vec![
                        "service-worker",
                        "indexed-db",
                        "route-registry",
                        "private-return-route",
                        "vhttp-atomic",
                        "vhttp-streaming",
                        "streaming-binary-ipc",
                        "per-stream-ipc-flow-control",
                    ],
                })?,
                Bytes::new(),
            )),
            Request::Fingerprint { route_blob_base64 } => {
                let blob = veilid_http_route::decode_route_blob(&route_blob_base64)?;
                Ok((
                    success(serde_json::json!({
                        "fingerprint": veilid_http_route::fingerprint(&blob)
                    }))?,
                    Bytes::new(),
                ))
            }
            Request::ImportRoute { route_blob_base64 } => {
                let blob = veilid_http_route::decode_route_blob(&route_blob_base64)?;
                let (fingerprint, _) = self.import_route(Bytes::from(blob)).await?;
                Ok((success(serde_json::json!({ "fingerprint": fingerprint }))?, Bytes::new()))
            }
            Request::HttpRequest { .. } => bail!("HTTP requests use the streaming IPC path"),
        }
    }
}

fn secrets_equal(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.as_bytes()
        .iter()
        .zip(right.as_bytes())
        .fold(0_u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

async fn queue_response<T: Serialize>(
    outbound: &mpsc::Sender<IpcFrame>,
    kind: FrameKind,
    request_id: u64,
    metadata: &T,
    payload: Bytes,
) -> Result<()> {
    outbound
        .send(IpcFrame::from_metadata(kind, request_id, metadata, payload)?)
        .await
        .context("queue IPC response")
}

#[allow(clippy::too_many_arguments)]
async fn start_http_request(
    state: Arc<State>,
    outbound: mpsc::Sender<IpcFrame>,
    active: Arc<Mutex<HashMap<u64, ActiveRequest>>>,
    request_id: u64,
    site_id: String,
    method: String,
    path_and_query: String,
    headers: Vec<(String, String)>,
    has_body: bool,
) -> Result<()> {
    if !veilid_http_route::is_site_id(&site_id) {
        bail!("invalid site identifier");
    }
    if active.lock().await.contains_key(&request_id) {
        bail!("duplicate active IPC request identifier");
    }
    let permit = Arc::clone(&state.active_requests)
        .try_acquire_owned()
        .map_err(|_| anyhow::anyhow!("client active request limit reached"))?;
    let target = state.target_for_site(&site_id).await?;
    let request = state.runtime.start_request(
        target,
        RequestHead {
            method,
            path_and_query,
            headers: headers
                .into_iter()
                .map(|(name, value)| HeaderField { name, value })
                .collect(),
        },
        has_body,
    );
    let cancel = request.cancellation_handle();
    let request_body = request.request_body;
    let mut responses = request.responses;
    let response_credits = Arc::new(Semaphore::new(0));
    active.lock().await.insert(
        request_id,
        ActiveRequest {
            request_body,
            response_credits: Arc::clone(&response_credits),
            cancel,
            _permit: permit,
        },
    );

    tokio::spawn(async move {
        let mut head_sent = false;
        while let Some(event) = responses.recv().await {
            let queued = match event {
                ClientResponseEvent::Head(head) => {
                    head_sent = true;
                    let metadata = HttpResponseMetadata {
                        status: head.status,
                        headers: head
                            .headers
                            .into_iter()
                            .map(|header| (header.name, header.value))
                            .collect(),
                    };
                    queue_response(
                        &outbound,
                        FrameKind::Response,
                        request_id,
                        &success(metadata).unwrap_or_else(|error| failure(error)),
                        Bytes::new(),
                    )
                    .await
                }
                ClientResponseEvent::Data(chunk) => {
                    let permit = Arc::clone(&response_credits)
                        .acquire_owned()
                        .await
                        .map_err(|_| anyhow::anyhow!("local response stream was cancelled"));
                    match permit {
                        Ok(permit) => {
                            permit.forget();
                            queue_response(
                                &outbound,
                                FrameKind::StreamData,
                                request_id,
                                &(),
                                chunk,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    }
                }
                ClientResponseEvent::End => {
                    let result = queue_response(
                        &outbound,
                        FrameKind::StreamEnd,
                        request_id,
                        &(),
                        Bytes::new(),
                    )
                    .await;
                    if result.is_ok() {
                        break;
                    }
                    result
                }
                ClientResponseEvent::Error(error) => {
                    let kind = if head_sent {
                        FrameKind::Cancel
                    } else {
                        FrameKind::Response
                    };
                    let result = queue_response(
                        &outbound,
                        kind,
                        request_id,
                        &failure(error),
                        Bytes::new(),
                    )
                    .await;
                    if result.is_ok() {
                        break;
                    }
                    result
                }
            };
            if let Err(error) = queued {
                tracing::debug!(%error, request_id, "stopped forwarding IPC response stream");
                break;
            }
        }
        response_credits.close();
        if let Some(active_request) = active.lock().await.remove(&request_id) {
            let _ = active_request.cancel.send(true);
        }
    });
    Ok(())
}

async fn serve_connection<S>(stream: S, state: Arc<State>, expected_secret: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let hello = read_frame(&mut reader).await.context("read IPC hello")?;
    if hello.kind != FrameKind::Hello {
        bail!("first IPC frame must authenticate the Electron parent");
    }
    let supplied = hello.decode_metadata::<Hello>().context("decode IPC hello")?;
    if !secrets_equal(&supplied.secret, expected_secret) {
        bail!("invalid IPC launch secret");
    }

    let (outbound, mut outbound_receiver) = mpsc::channel::<IpcFrame>(16);
    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outbound_receiver.recv().await {
            write_frame(&mut writer, &frame).await?;
        }
        Ok::<(), veilid_http_ipc::IpcError>(())
    });
    queue_response(
        &outbound,
        FrameKind::Response,
        hello.request_id,
        &success(serde_json::json!({ "status": "ready" }))?,
        Bytes::new(),
    )
    .await?;

    let active = Arc::new(Mutex::new(HashMap::<u64, ActiveRequest>::new()));
    loop {
        let frame = match read_frame(&mut reader).await {
            Ok(frame) => frame,
            Err(veilid_http_ipc::IpcError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                break;
            }
            Err(error) => return Err(error.into()),
        };

        match frame.kind {
            FrameKind::Request => {
                let request = match frame.decode_metadata::<Request>() {
                    Ok(request) => request,
                    Err(error) => {
                        queue_response(
                            &outbound,
                            FrameKind::Response,
                            frame.request_id,
                            &failure(error),
                            Bytes::new(),
                        )
                        .await?;
                        continue;
                    }
                };
                match request {
                    Request::HttpRequest {
                        site_id,
                        method,
                        path_and_query,
                        headers,
                        has_body,
                    } => {
                        if !frame.payload.is_empty() {
                            queue_response(
                                &outbound,
                                FrameKind::Response,
                                frame.request_id,
                                &failure("HTTP request bodies must use StreamData IPC frames"),
                                Bytes::new(),
                            )
                            .await?;
                            continue;
                        }
                        if let Err(error) = start_http_request(
                            Arc::clone(&state),
                            outbound.clone(),
                            Arc::clone(&active),
                            frame.request_id,
                            site_id,
                            method,
                            path_and_query,
                            headers,
                            has_body,
                        )
                        .await
                        {
                            queue_response(
                                &outbound,
                                FrameKind::Response,
                                frame.request_id,
                                &failure(error),
                                Bytes::new(),
                            )
                            .await?;
                        }
                    }
                    control => {
                        let state = Arc::clone(&state);
                        let outbound = outbound.clone();
                        tokio::spawn(async move {
                            let (metadata, payload) = match state.handle_control(control).await {
                                Ok(value) => value,
                                Err(error) => (failure(error), Bytes::new()),
                            };
                            let _ = queue_response(
                                &outbound,
                                FrameKind::Response,
                                frame.request_id,
                                &metadata,
                                payload,
                            )
                            .await;
                        });
                    }
                }
            }
            FrameKind::StreamData => {
                let sender = active
                    .lock()
                    .await
                    .get(&frame.request_id)
                    .and_then(|request| request.request_body.clone());
                match sender {
                    Some(sender) => {
                        if sender.send(frame.payload).await.is_err() {
                            queue_response(
                                &outbound,
                                FrameKind::Cancel,
                                frame.request_id,
                                &failure("native request-body stream is closed"),
                                Bytes::new(),
                            )
                            .await?;
                        }
                    }
                    None => {
                        queue_response(
                            &outbound,
                            FrameKind::Cancel,
                            frame.request_id,
                            &failure("no live request body accepts this StreamData frame"),
                            Bytes::new(),
                        )
                        .await?;
                    }
                }
            }
            FrameKind::StreamEnd => {
                let mut active_requests = active.lock().await;
                match active_requests.get_mut(&frame.request_id) {
                    Some(request) if request.request_body.is_some() => {
                        request.request_body.take();
                    }
                    _ => {
                        drop(active_requests);
                        queue_response(
                            &outbound,
                            FrameKind::Cancel,
                            frame.request_id,
                            &failure("no live request body accepts this StreamEnd frame"),
                            Bytes::new(),
                        )
                        .await?;
                    }
                }
            }
            FrameKind::StreamCredit => {
                let credit = match frame
                    .decode_metadata::<StreamCredit>()
                    .and_then(StreamCredit::validate)
                {
                    Ok(credit) => credit,
                    Err(error) => {
                        queue_response(
                            &outbound,
                            FrameKind::Cancel,
                            frame.request_id,
                            &failure(error),
                            Bytes::new(),
                        )
                        .await?;
                        continue;
                    }
                };
                let response_credits = active
                    .lock()
                    .await
                    .get(&frame.request_id)
                    .map(|request| Arc::clone(&request.response_credits));
                let Some(response_credits) = response_credits else {
                    queue_response(
                        &outbound,
                        FrameKind::Cancel,
                        frame.request_id,
                        &failure("no live response stream accepts this credit update"),
                        Bytes::new(),
                    )
                    .await?;
                    continue;
                };
                let credits = usize::try_from(credit.credits)
                    .map_err(|_| anyhow::anyhow!("stream credit count does not fit usize"))?;
                let maximum = usize::try_from(MAX_STREAM_CREDITS)
                    .map_err(|_| anyhow::anyhow!("stream credit bound does not fit usize"))?;
                if response_credits.available_permits().saturating_add(credits) > maximum {
                    response_credits.close();
                    queue_response(
                        &outbound,
                        FrameKind::Cancel,
                        frame.request_id,
                        &failure("response stream credit window exceeded"),
                        Bytes::new(),
                    )
                    .await?;
                    continue;
                }
                response_credits.add_permits(credits);
            }
            FrameKind::Cancel => {
                if let Some(request) = active.lock().await.remove(&frame.request_id) {
                    request.response_credits.close();
                    let _ = request.cancel.send(true);
                }
            }
            FrameKind::Hello | FrameKind::Response | FrameKind::Event => {
                queue_response(
                    &outbound,
                    FrameKind::Response,
                    frame.request_id,
                    &failure("invalid IPC frame kind from Electron"),
                    Bytes::new(),
                )
                .await?;
            }
        }
    }

    for (_, request) in active.lock().await.drain() {
        request.response_credits.close();
        let _ = request.cancel.send(true);
    }
    drop(outbound);
    writer_task.await.context("join IPC writer")??;
    Ok(())
}

#[cfg(unix)]
async fn serve(config: &Config, state: Arc<State>) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    use tokio::net::UnixListener;

    let path = Path::new(&config.ipc_path);
    if path.exists() {
        fs::remove_file(path)
            .with_context(|| format!("remove stale IPC socket {}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create IPC directory {}", parent.display()))?;
    }
    let listener = UnixListener::bind(path)
        .with_context(|| format!("bind IPC socket {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("protect IPC socket {}", path.display()))?;
    let result = async {
        loop {
            let (stream, _) = listener
                .accept()
                .await
                .context("accept Electron IPC connection")?;
            if let Err(error) = serve_connection(stream, Arc::clone(&state), &config.ipc_secret).await {
                tracing::warn!(%error, "Electron IPC connection ended with an error");
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let _ = fs::remove_file(path);
    result
}

#[cfg(windows)]
async fn serve(config: &Config, state: Arc<State>) -> Result<()> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let mut first = true;
    loop {
        let server = if first {
            first = false;
            ServerOptions::new()
                .first_pipe_instance(true)
                .create(&config.ipc_path)
        } else {
            ServerOptions::new().create(&config.ipc_path)
        }
        .with_context(|| format!("create named pipe {}", config.ipc_path))?;
        server
            .connect()
            .await
            .context("accept Electron named-pipe connection")?;
        if let Err(error) = serve_connection(server, Arc::clone(&state), &config.ipc_secret).await {
            tracing::warn!(%error, "Electron named-pipe connection ended with an error");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let config = Config::parse();
    if config.ipc_secret.len() < 32 {
        bail!("VHTTP_IPC_SECRET is too short");
    }
    if config.max_active_requests == 0 {
        bail!("VHTTP_CLIENT_MAX_ACTIVE_REQUESTS must be non-zero");
    }
    fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("create {}", config.data_dir.display()))?;

    let transport = Arc::new(
        NativeVeilidTransport::start(NativeTransportConfig::client(
            config.data_dir.join("veilid"),
        ))
        .await
        .context("start native Veilid transport")?,
    );
    let runtime = ClientRuntime::start(transport, config.data_dir.clone())
        .await
        .context("start native VHTTP client runtime")?;
    let state = Arc::new(State {
        data_dir: config.data_dir.clone(),
        runtime,
        imported_routes: Mutex::new(HashMap::new()),
        active_requests: Arc::new(Semaphore::new(config.max_active_requests)),
    });
    serve(&config, state).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_route_uses_deterministic_site_id() {
        let root = std::env::temp_dir().join(format!("veilid-http-native-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let first = persist_route(&root, b"private route").unwrap();
        let second = persist_route(&root, b"private route").unwrap();
        assert_eq!(first, second);
        assert!(route_path(&root, &first).is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn launch_secret_comparison_checks_every_byte() {
        assert!(secrets_equal(
            "abcdefghijklmnopqrstuvwxyz012345",
            "abcdefghijklmnopqrstuvwxyz012345"
        ));
        assert!(!secrets_equal(
            "abcdefghijklmnopqrstuvwxyz012345",
            "abcdefghijklmnopqrstuvwxyz012346"
        ));
        assert!(!secrets_equal("short", "longer"));
    }
}
