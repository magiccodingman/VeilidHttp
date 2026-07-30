//! Docker-side VHTTP bridge process.

mod completion;
mod streaming;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use clap::Parser;
use completion::{CompletionClaim, CompletionStore};
use futures::StreamExt as _;
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use url::Url;
use veilid_http_http::{
    HeaderField, ResponseHead, attach_route_headers, decode_atomic_request,
    encode_atomic_response, upstream_url,
};
use veilid_http_stream::{StreamError, encode_error};
use veilid_http_transport::{RouteTarget, TransportEvent, VeilidTransport};
use veilid_http_veilid_remote::{RemoteEndpoint, RemoteVeilidTransport};
use streaming::{StreamingBridge, StreamingConfig};

#[derive(Debug, Parser)]
#[command(version, about = "Translate VHTTP/1 transactions to one HTTP upstream")]
struct Config {
    #[arg(long, env = "VHTTP_UPSTREAM_URL")]
    upstream_url: String,
    #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
    data_dir: PathBuf,
    #[arg(long, env = "VEILID_CLIENT_ENDPOINT", default_value = "127.0.0.1:5959")]
    veilid_client_endpoint: String,
    #[arg(long, env = "VEILID_EXPECTED_VERSION_PREFIX", default_value = "0.5.5")]
    expected_veilid_version_prefix: String,
    #[arg(long, env = "VHTTP_IDLE_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    idle_timeout: Duration,
    #[arg(long, env = "VHTTP_OVERALL_TIMEOUT", default_value = "60m", value_parser = parse_duration)]
    overall_timeout: Duration,
    #[arg(long, env = "VHTTP_REMOTE_TIMEOUT", default_value = "2m", value_parser = parse_duration)]
    remote_timeout: Duration,
    #[arg(long, env = "VHTTP_FRAME_BYTES", default_value_t = 30 * 1024)]
    frame_bytes: usize,
    #[arg(long, env = "VHTTP_SEND_WINDOW_FRAMES", default_value_t = 32)]
    send_window_frames: usize,
    #[arg(long, env = "VHTTP_FORWARD_ROUTE_HEADER", default_value = "X-Veilid-Route-Fingerprint")]
    route_header: String,
    #[arg(long, env = "VHTTP_MAX_ATOMIC_BODY_BYTES", default_value_t = 8 * 1024 * 1024)]
    max_atomic_body_bytes: usize,
    #[arg(long, env = "VHTTP_MAX_REQUEST_BYTES", default_value_t = 0)]
    max_request_bytes: u64,
    #[arg(long, env = "VHTTP_MAX_RESPONSE_BYTES", default_value_t = 0)]
    max_response_bytes: u64,
    #[arg(long, env = "VHTTP_MAX_PENDING_BYTES", default_value_t = 8 * 1024 * 1024)]
    max_pending_bytes: usize,
    #[arg(long, env = "VHTTP_MAX_OUT_OF_ORDER_BYTES", default_value_t = 4 * 1024 * 1024)]
    max_out_of_order_bytes: usize,
    #[arg(long, env = "VHTTP_COMPLETED_RETENTION", default_value = "15m", value_parser = parse_duration)]
    completed_retention: Duration,
    #[arg(long, env = "VHTTP_COMPLETED_RESPONSE_BYTES", default_value_t = 1024 * 1024)]
    completed_response_bytes: usize,
    #[arg(long, env = "VHTTP_COMPLETED_MAX_ENTRIES", default_value_t = 4096)]
    completed_max_entries: usize,
    /// Validate configuration and persistence layout without starting the adapter.
    #[arg(long)]
    check: bool,
    /// Keep a validation-only bridge process alive for explicit configuration tests.
    #[arg(long)]
    validation_supervisor: bool,
}

#[derive(Debug, Serialize)]
struct Ready<'a> {
    status: &'static str,
    upstream: &'a str,
    frame_bytes: usize,
    send_window_frames: usize,
    idle_timeout_seconds: u64,
    overall_timeout_seconds: u64,
    adapter: &'static str,
    veilid_version: Option<&'a str>,
    route_fingerprint: Option<&'a str>,
}

#[derive(Debug, Clone)]
struct ActiveRoute {
    target: RouteTarget,
    fingerprint: String,
    blob: Bytes,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PersistedRoute<'a> {
    schema: &'static str,
    route_id: &'a str,
    fingerprint: &'a str,
    created_at_unix_seconds: u64,
    veilid_version: &'a str,
    blob_file: &'static str,
    base64_file: &'static str,
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(split);
    let amount: u64 = amount
        .parse()
        .map_err(|_| "duration must start with an integer")?;
    let seconds = match unit {
        "s" | "" => amount,
        "m" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(3600),
        _ => return Err("duration unit must be s, m, or h".into()),
    };
    Ok(Duration::from_secs(seconds))
}

fn ensure_layout(data_dir: &Path) -> Result<()> {
    for path in [
        data_dir.join("route"),
        data_dir.join("route/history"),
        data_dir.join("transfers"),
        data_dir.join("completed"),
        data_dir.join("spool"),
    ] {
        fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
    }
    Ok(())
}

fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = path.with_extension(format!(
        "{}.tmp",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("file")
    ));
    fs::write(&temporary, contents).with_context(|| format!("write {}", temporary.display()))?;
    fs::rename(&temporary, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

fn persist_route(data_dir: &Path, route: &ActiveRoute, veilid_version: &str) -> Result<()> {
    let route_dir = data_dir.join("route");
    let history_dir = route_dir.join("history");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs();
    let base64 = veilid_http_route::encode_route_blob(&route.blob);
    let metadata = PersistedRoute {
        schema: "org.veilidhttp.server-route/v1",
        route_id: &route.target.0,
        fingerprint: &route.fingerprint,
        created_at_unix_seconds: now,
        veilid_version,
        blob_file: "current.blob",
        base64_file: "current.base64",
    };
    let metadata_json = serde_json::to_vec_pretty(&metadata)?;

    atomic_write(&route_dir.join("current.blob"), &route.blob)?;
    atomic_write(&route_dir.join("current.base64"), base64.as_bytes())?;
    atomic_write(&route_dir.join("current.json"), &metadata_json)?;
    atomic_write(
        &history_dir.join(format!("{now}-{}.blob", route.fingerprint)),
        &route.blob,
    )?;
    atomic_write(
        &history_dir.join(format!("{now}-{}.json", route.fingerprint)),
        &metadata_json,
    )?;
    Ok(())
}

async fn allocate_route(
    transport: &RemoteVeilidTransport,
    data_dir: &Path,
) -> Result<ActiveRoute> {
    let (target, blob) = transport
        .allocate_route()
        .await
        .context("allocate reliable private route")?;
    let route = ActiveRoute {
        target,
        fingerprint: veilid_http_route::fingerprint(&blob),
        blob,
    };
    persist_route(data_dir, &route, transport.server_version())?;
    tracing::info!(
        event = "VHTTP_ROUTE_READY",
        fingerprint = %route.fingerprint,
        route_id = %route.target.0,
        blob_file = %data_dir.join("route/current.blob").display(),
        base64_file = %data_dir.join("route/current.base64").display(),
        "VeilidHttp private route is ready"
    );
    Ok(route)
}

fn small_error_response(
    transaction_id: [u8; 16],
    status: u16,
    message: impl AsRef<str>,
) -> Result<Bytes> {
    let body = message.as_ref().as_bytes();
    encode_atomic_response(
        transaction_id,
        ResponseHead {
            status,
            headers: vec![
                HeaderField {
                    name: "content-type".to_owned(),
                    value: "text/plain; charset=utf-8".to_owned(),
                },
                HeaderField {
                    name: "cache-control".to_owned(),
                    value: "no-store".to_owned(),
                },
                HeaderField {
                    name: "content-length".to_owned(),
                    value: body.len().to_string(),
                },
            ],
        },
        body,
        true,
    )
    .context("encode atomic error response")
}

async fn forward_atomic(
    client: &reqwest::Client,
    config: &Config,
    site_fingerprint: &str,
    encoded_request: Bytes,
) -> Result<Bytes> {
    let transaction_id = veilid_http_wire::Frame::decode(encoded_request.clone())
        .map(|frame| frame.transaction_id)
        .unwrap_or([0; 16]);
    let request = match decode_atomic_request(encoded_request, config.max_atomic_body_bytes) {
        Ok(request) => request,
        Err(error) => {
            return small_error_response(
                transaction_id,
                400,
                format!("invalid VHTTP request: {error}"),
            );
        }
    };
    let target = upstream_url(&config.upstream_url, &request.head.path_and_query)?;
    let method = reqwest::Method::from_bytes(request.head.method.as_bytes())
        .context("parse reconstructed HTTP method")?;
    let headers = attach_route_headers(
        request.head.headers,
        site_fingerprint,
        &config.route_header,
    );
    let mut upstream_request = client.request(method, target).body(request.body.clone());
    for header in headers {
        if header.name.eq_ignore_ascii_case("host")
            || header.name.eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        let name = reqwest::header::HeaderName::from_bytes(header.name.as_bytes())
            .with_context(|| format!("invalid request header name {}", header.name))?;
        let value = reqwest::header::HeaderValue::from_bytes(header.value.as_bytes())
            .with_context(|| format!("invalid request header value for {}", header.name))?;
        upstream_request = upstream_request.header(name, value);
    }

    let response = match upstream_request.send().await {
        Ok(response) => response,
        Err(error) => {
            return small_error_response(
                request.transaction_id,
                502,
                format!("upstream request failed: {error}"),
            );
        }
    };
    let status = response.status().as_u16();
    let response_headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value.to_str().ok().map(|value| HeaderField {
                name: name.as_str().to_owned(),
                value: value.to_owned(),
            })
        })
        .collect::<Vec<_>>();
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                return small_error_response(
                    request.transaction_id,
                    502,
                    format!("upstream response stream failed: {error}"),
                );
            }
        };
        if body.len().saturating_add(chunk.len()) > config.max_atomic_body_bytes {
            return small_error_response(
                request.transaction_id,
                409,
                "response requires stream-capable RequestOpen with a client return route",
            );
        }
        body.extend_from_slice(&chunk);
    }

    match encode_atomic_response(
        request.transaction_id,
        ResponseHead {
            status,
            headers: response_headers,
        },
        &body,
        true,
    ) {
        Ok(response) => Ok(response),
        Err(error) => small_error_response(
            request.transaction_id,
            409,
            format!("response requires the streamed VHTTP path: {error}"),
        ),
    }
}

async fn handle_atomic_call(
    transport: Arc<RemoteVeilidTransport>,
    client: reqwest::Client,
    config: Arc<Config>,
    completion: Arc<CompletionStore>,
    call_id: String,
    site_fingerprint: String,
    payload: Bytes,
) {
    let transaction_id = match veilid_http_wire::Frame::decode(payload.clone()) {
        Ok(frame) => frame.transaction_id,
        Err(error) => {
            let response = small_error_response(
                [0; 16],
                400,
                format!("invalid VHTTP frame: {error}"),
            );
            if let Ok(response) = response {
                let _ = transport.app_call_reply(&call_id, response).await;
            }
            return;
        }
    };
    let waiting_started = Instant::now();

    loop {
        match completion.claim(transaction_id).await {
            CompletionClaim::Execute => {
                let response = match forward_atomic(
                    &client,
                    &config,
                    &site_fingerprint,
                    payload.clone(),
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        completion.abandon(transaction_id).await;
                        match small_error_response(
                            transaction_id,
                            500,
                            format!("atomic translation failed: {error}"),
                        ) {
                            Ok(response) => response,
                            Err(encoding_error) => {
                                tracing::error!(%encoding_error, "failed to encode atomic failure");
                                return;
                            }
                        }
                    }
                };
                if let Err(error) = completion
                    .record(transaction_id, Some(response.clone()))
                    .await
                {
                    tracing::error!(%error, "failed to persist atomic completion record");
                }
                if let Err(error) = transport.app_call_reply(&call_id, response).await {
                    tracing::error!(%error, "failed to reply to atomic Veilid AppCall");
                }
                return;
            }
            CompletionClaim::Replay(response) => {
                if let Err(error) = transport.app_call_reply(&call_id, response).await {
                    tracing::error!(%error, "failed to replay retained atomic response");
                }
                return;
            }
            CompletionClaim::Tombstone => {
                let response = small_error_response(
                    transaction_id,
                    409,
                    "transaction already completed; retained response is unavailable",
                );
                if let Ok(response) = response {
                    let _ = transport.app_call_reply(&call_id, response).await;
                }
                return;
            }
            CompletionClaim::Wait(notify) => {
                let elapsed = waiting_started.elapsed();
                if elapsed >= config.overall_timeout {
                    let response = small_error_response(
                        transaction_id,
                        504,
                        "timed out waiting for the original transaction execution",
                    );
                    if let Ok(response) = response {
                        let _ = transport.app_call_reply(&call_id, response).await;
                    }
                    return;
                }
                let remaining = config.overall_timeout.saturating_sub(elapsed);
                let recheck_after = remaining.min(Duration::from_millis(50));
                let _ = tokio::time::timeout(recheck_after, notify.notified()).await;
            }
        }
    }
}

async fn run_live(config: Arc<Config>, upstream: Url) -> Result<()> {
    let endpoint = config
        .veilid_client_endpoint
        .parse::<RemoteEndpoint>()
        .context("parse VEILID_CLIENT_ENDPOINT")?;
    let transport = Arc::new(
        RemoteVeilidTransport::connect(endpoint, config.remote_timeout)
            .await
            .context("connect to official veilid-server client API")?,
    );
    if !transport
        .server_version()
        .starts_with(&config.expected_veilid_version_prefix)
    {
        bail!(
            "veilid-server {} does not match remote API schema prefix {}",
            transport.server_version(),
            config.expected_veilid_version_prefix
        );
    }
    let route = allocate_route(&transport, &config.data_dir).await?;
    let active_route = Arc::new(RwLock::new(route));
    let completion = CompletionStore::open(
        config.data_dir.join("completed"),
        config.completed_retention,
        config.completed_response_bytes,
        config.completed_max_entries,
    )?;
    let client = reqwest::Client::builder()
        .timeout(config.overall_timeout)
        .build()
        .context("build upstream HTTP client")?;
    let transport_trait: Arc<dyn VeilidTransport> = transport.clone();
    let streaming = StreamingBridge::new(
        transport_trait,
        client.clone(),
        Arc::clone(&completion),
        StreamingConfig {
            upstream_url: config.upstream_url.clone(),
            route_header: config.route_header.clone(),
            frame_bytes: config.frame_bytes,
            window_frames: u32::try_from(config.send_window_frames)
                .context("send window does not fit u32")?,
            max_pending_bytes: config.max_pending_bytes,
            max_out_of_order_bytes: config.max_out_of_order_bytes,
            max_request_bytes: config.max_request_bytes,
            max_response_bytes: config.max_response_bytes,
            overall_timeout: config.overall_timeout,
        },
    );

    let current = active_route.read().await;
    let ready = Ready {
        status: "ready",
        upstream: upstream.as_str(),
        frame_bytes: config.frame_bytes,
        send_window_frames: config.send_window_frames,
        idle_timeout_seconds: config.idle_timeout.as_secs(),
        overall_timeout_seconds: config.overall_timeout.as_secs(),
        adapter: "veilid-server-remote",
        veilid_version: Some(transport.server_version()),
        route_fingerprint: Some(&current.fingerprint),
    };
    println!("{}", serde_json::to_string(&ready)?);
    drop(current);

    loop {
        match transport.next_event().await.context("receive Veilid update")? {
            TransportEvent::AppCall {
                call_id,
                route,
                payload,
            } => {
                let current = active_route.read().await.clone();
                if route.as_ref() != Some(&current.target) {
                    tracing::warn!(?route, "ignoring AppCall for a non-current route");
                    continue;
                }
                if StreamingBridge::is_stream_open(payload.clone()) {
                    let streaming = streaming.clone();
                    let transport = Arc::clone(&transport);
                    tokio::spawn(async move {
                        if let Err(error) = streaming
                            .handle_open(&call_id, current.fingerprint, payload.clone())
                            .await
                        {
                            tracing::error!(%error, "failed to accept streamed RequestOpen");
                            if let Ok(frame) = veilid_http_wire::Frame::decode(payload) {
                                if let Ok(encoded) = encode_error(
                                    frame.transaction_id,
                                    &StreamError {
                                        code: "request-open-rejected".to_owned(),
                                        message: error.to_string(),
                                        retryable: false,
                                    },
                                ) {
                                    let _ = transport.app_call_reply(&call_id, encoded).await;
                                }
                            }
                        }
                    });
                } else {
                    tokio::spawn(handle_atomic_call(
                        Arc::clone(&transport),
                        client.clone(),
                        Arc::clone(&config),
                        Arc::clone(&completion),
                        call_id,
                        current.fingerprint,
                        payload,
                    ));
                }
            }
            TransportEvent::AppMessage { route, payload } => {
                let current = active_route.read().await.clone();
                if route.as_ref() != Some(&current.target) {
                    tracing::warn!(?route, "ignoring AppMessage for a non-current route");
                    continue;
                }
                if let Err(error) = streaming.handle_message(payload).await {
                    tracing::warn!(%error, "rejected VHTTP continuation AppMessage");
                }
            }
            TransportEvent::RouteChanged { route, dead: true } => {
                let current = active_route.read().await.clone();
                if route == current.target {
                    tracing::warn!(fingerprint = %current.fingerprint, "active private route died; cancelling streams and allocating replacement");
                    streaming.cancel_all().await;
                    let replacement = allocate_route(&transport, &config.data_dir).await?;
                    *active_route.write().await = replacement;
                }
            }
            TransportEvent::RouteChanged { .. } => {}
            TransportEvent::Shutdown => bail!("veilid-server remote API shut down"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();
    let config = Arc::new(Config::parse());
    let upstream = Url::parse(&config.upstream_url).context("invalid VHTTP_UPSTREAM_URL")?;
    if !matches!(upstream.scheme(), "http" | "https") {
        bail!("upstream must use http or https");
    }
    if config.frame_bytes > veilid_http_wire::DEFAULT_FRAME_LIMIT {
        bail!("frame bytes exceed conservative VHTTP limit");
    }
    if config.send_window_frames == 0 || config.send_window_frames > 64 {
        bail!("send window must be in 1..=64");
    }
    if config.max_pending_bytes < config.frame_bytes {
        bail!("VHTTP_MAX_PENDING_BYTES must hold at least one frame");
    }
    if config.completed_response_bytes > config.max_atomic_body_bytes {
        bail!("VHTTP_COMPLETED_RESPONSE_BYTES cannot exceed VHTTP_MAX_ATOMIC_BODY_BYTES");
    }
    ensure_layout(&config.data_dir)?;

    if config.check || config.validation_supervisor {
        let ready = Ready {
            status: if config.check {
                "configuration-valid"
            } else {
                "validation-supervisor"
            },
            upstream: upstream.as_str(),
            frame_bytes: config.frame_bytes,
            send_window_frames: config.send_window_frames,
            idle_timeout_seconds: config.idle_timeout.as_secs(),
            overall_timeout_seconds: config.overall_timeout.as_secs(),
            adapter: "veilid-server-remote",
            veilid_version: None,
            route_fingerprint: None,
        };
        println!("{}", serde_json::to_string(&ready)?);
        if config.validation_supervisor {
            tokio::signal::ctrl_c()
                .await
                .context("wait for shutdown signal")?;
        }
        return Ok(());
    }

    run_live(config, upstream).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hostile_network_timeouts() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }

    #[test]
    fn route_files_are_atomically_published() {
        let root = std::env::temp_dir().join(format!(
            "veilid-http-bridge-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        ensure_layout(&root).unwrap();
        let route = ActiveRoute {
            target: RouteTarget("route-id".to_owned()),
            fingerprint: veilid_http_route::fingerprint(b"route"),
            blob: Bytes::from_static(b"route"),
        };
        persist_route(&root, &route, "test").unwrap();
        assert_eq!(
            fs::read(root.join("route/current.blob")).unwrap(),
            b"route"
        );
        assert!(root.join("route/current.json").is_file());
        assert!(root.join("route/current.base64").is_file());
        fs::remove_dir_all(root).unwrap();
    }
}
