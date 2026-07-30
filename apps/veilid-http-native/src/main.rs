//! Private Electron sidecar boundary.
//!
//! V1 persists imported RouteBlobs and exposes a narrow JSON-lines control channel.
//! The native Veilid transport adapter plugs into this process without expanding the
//! renderer's privileges.

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{fs, path::{Path, PathBuf}};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Debug, Parser)]
#[command(version, about = "Native VeilidHttp client sidecar")]
struct Config {
    /// Private state controlled by the trusted Electron main process.
    #[arg(long, env = "VHTTP_CLIENT_DATA_DIR", default_value = "./data/client")]
    data_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Request {
    Health { request_id: String },
    Fingerprint { request_id: String, route_blob_base64: String },
    ImportRoute { request_id: String, route_blob_base64: String },
    Environment { request_id: String },
    HttpRequest {
        request_id: String,
        site_id: String,
        method: String,
        path_and_query: String,
        headers: Vec<(String, String)>,
        has_body: bool,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Response<T> {
    request_id: String,
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

fn success<T: Serialize>(request_id: String, value: T) -> Result<Response<serde_json::Value>> {
    Ok(Response {
        request_id,
        ok: true,
        result: Some(serde_json::to_value(value)?),
        error: None,
    })
}

fn failure(request_id: String, error: impl std::fmt::Display) -> Response<serde_json::Value> {
    Response { request_id, ok: false, result: None, error: Some(error.to_string()) }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();
    let config = Config::parse();
    fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("create {}", config.data_dir.display()))?;

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(Request::Health { request_id }) => success(request_id, serde_json::json!({ "status": "ready" }))?,
            Ok(Request::Environment { request_id }) => success(
                request_id,
                Environment {
                    environment: "electron",
                    transport: "veilid-native-pending-adapter",
                    protocol_version: 1,
                    capabilities: vec!["service-worker", "indexed-db", "route-registry", "vhttp-core"],
                },
            )?,
            Ok(Request::Fingerprint { request_id, route_blob_base64 }) => {
                match veilid_http_route::decode_route_blob(&route_blob_base64) {
                    Ok(blob) => success(request_id, serde_json::json!({ "fingerprint": veilid_http_route::fingerprint(&blob) }))?,
                    Err(error) => failure(request_id, error),
                }
            }
            Ok(Request::ImportRoute { request_id, route_blob_base64 }) => {
                match veilid_http_route::decode_route_blob(&route_blob_base64)
                    .map_err(anyhow::Error::from)
                    .and_then(|blob| persist_route(&config.data_dir, &blob))
                {
                    Ok(fingerprint) => success(request_id, serde_json::json!({ "fingerprint": fingerprint }))?,
                    Err(error) => failure(request_id, error),
                }
            }
            Ok(Request::HttpRequest {
                request_id,
                site_id,
                method,
                path_and_query,
                headers,
                has_body,
            }) => {
                if !veilid_http_route::is_site_id(&site_id) {
                    failure(request_id, "invalid site identifier")
                } else if !route_path(&config.data_dir, &site_id).is_file() {
                    failure(request_id, "the requested private route is not imported")
                } else {
                    let _request_shape = (method, path_and_query, headers, has_body);
                    failure(request_id, "native Veilid request transport is not connected in this foundation build")
                }
            }
            Err(error) => failure("unknown".into(), error),
        };
        let mut encoded = serde_json::to_vec(&response).context("encode sidecar response")?;
        encoded.push(b'\n');
        stdout.write_all(&encoded).await?;
        stdout.flush().await?;
    }
    Ok(())
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
