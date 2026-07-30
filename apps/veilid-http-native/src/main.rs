//! Trusted Electron sidecar: native Veilid transport plus bounded local IPC.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
};
use veilid_http_http::{
    HeaderField, RequestHead, decode_atomic_response, encode_atomic_request,
};
use veilid_http_ipc::{FrameKind, Hello, IpcFrame, read_frame, write_frame};
use veilid_http_transport::{RouteTarget, VeilidTransport};
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
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
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
    transport: Arc<NativeVeilidTransport>,
    imported_routes: Mutex<HashMap<String, RouteTarget>>,
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
        let target = self
            .transport
            .import_route(blob)
            .await
            .context("import private RouteBlob into native Veilid node")?;
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
        let blob = fs::read(&path).with_context(|| format!("read imported route {}", path.display()))?;
        let (fingerprint, target) = self.import_route(Bytes::from(blob)).await?;
        if fingerprint != site_id {
            bail!("persisted RouteBlob fingerprint does not match requested site identifier");
        }
        Ok(target)
    }

    async fn handle(
        &self,
        request: Request,
        payload: Bytes,
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
                        "vhttp-atomic",
                        "binary-ipc",
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
            Request::HttpRequest {
                site_id,
                method,
                path_and_query,
                headers,
            } => {
                if !veilid_http_route::is_site_id(&site_id) {
                    bail!("invalid site identifier");
                }
                let target = self.target_for_site(&site_id).await?;
                let transaction_id = rand::random::<[u8; 16]>();
                let request_head = RequestHead {
                    method,
                    path_and_query,
                    headers: headers
                        .into_iter()
                        .map(|(name, value)| HeaderField { name, value })
                        .collect(),
                };
                let frame = encode_atomic_request(transaction_id, request_head, &payload, true)
                    .context("encode atomic VHTTP request")?;
                let reply = self
                    .transport
                    .app_call(&target, frame)
                    .await
                    .context("send VHTTP AppCall")?;
                let response = decode_atomic_response(reply, transaction_id, 8 * 1024 * 1024)
                    .context("decode atomic VHTTP response")?;
                let metadata = HttpResponseMetadata {
                    status: response.head.status,
                    headers: response
                        .head
                        .headers
                        .into_iter()
                        .map(|header| (header.name, header.value))
                        .collect(),
                };
                Ok((success(metadata)?, response.body))
            }
        }
    }
}

async fn serve_connection<S>(stream: S, state: Arc<State>, expected_secret: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let hello = read_frame(&mut reader).await.context("read IPC hello")?;
    if hello.kind != FrameKind::Hello {
        bail!("first IPC frame must authenticate the Electron parent");
    }
    let supplied = hello.decode_metadata::<Hello>().context("decode IPC hello")?;
    if supplied.secret.as_bytes() != expected_secret.as_bytes() {
        bail!("invalid IPC launch secret");
    }
    let accepted = IpcFrame::from_metadata(
        FrameKind::Response,
        hello.request_id,
        &success(serde_json::json!({ "status": "ready" }))?,
        Bytes::new(),
    )?;
    write_frame(&mut writer, &accepted).await?;

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
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        if frame.kind != FrameKind::Request {
            let response = IpcFrame::from_metadata(
                FrameKind::Response,
                frame.request_id,
                &failure("unexpected IPC frame kind"),
                Bytes::new(),
            )?;
            write_frame(&mut writer, &response).await?;
            continue;
        }
        let request = match frame.decode_metadata::<Request>() {
            Ok(request) => request,
            Err(error) => {
                let response = IpcFrame::from_metadata(
                    FrameKind::Response,
                    frame.request_id,
                    &failure(error),
                    Bytes::new(),
                )?;
                write_frame(&mut writer, &response).await?;
                continue;
            }
        };
        let (metadata, payload) = match state.handle(request, frame.payload).await {
            Ok(value) => value,
            Err(error) => (failure(error), Bytes::new()),
        };
        let response = IpcFrame::from_metadata(
            FrameKind::Response,
            frame.request_id,
            &metadata,
            payload,
        )?;
        write_frame(&mut writer, &response).await?;
    }
}

#[cfg(unix)]
async fn serve(config: &Config, state: Arc<State>) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    use tokio::net::UnixListener;

    let path = Path::new(&config.ipc_path);
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("remove stale IPC socket {}", path.display()))?;
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
            let (stream, _) = listener.accept().await.context("accept Electron IPC connection")?;
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

    loop {
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&config.ipc_path)
            .with_context(|| format!("create named pipe {}", config.ipc_path))?;
        server.connect().await.context("accept Electron named-pipe connection")?;
        if let Err(error) = serve_connection(server, Arc::clone(&state), &config.ipc_secret).await {
            tracing::warn!(%error, "Electron named-pipe connection ended with an error");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();
    let config = Config::parse();
    if config.ipc_secret.len() < 32 {
        bail!("VHTTP_IPC_SECRET is too short");
    }
    fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("create {}", config.data_dir.display()))?;

    let transport = NativeVeilidTransport::start(NativeTransportConfig::client(
        config.data_dir.join("veilid"),
    ))
    .await
    .context("start native Veilid transport")?;
    let state = Arc::new(State {
        data_dir: config.data_dir.clone(),
        transport: Arc::new(transport),
        imported_routes: Mutex::new(HashMap::new()),
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
}
