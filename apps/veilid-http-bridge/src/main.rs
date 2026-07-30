//! Docker-side VHTTP bridge process.
//!
//! The protocol and configuration are implemented. The exact current
//! `veilid-server` remote adapter remains isolated behind `VeilidTransport`.

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::Serialize;
use std::{fs, path::PathBuf, time::Duration};
use url::Url;

#[derive(Debug, Parser)]
#[command(version, about = "Translate VHTTP/1 transactions to one HTTP upstream")]
struct Config {
    #[arg(long, env = "VHTTP_UPSTREAM_URL")]
    upstream_url: String,
    #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
    data_dir: PathBuf,
    #[arg(long, env = "VHTTP_IDLE_TIMEOUT", default_value = "5m", value_parser = parse_duration)]
    idle_timeout: Duration,
    #[arg(long, env = "VHTTP_OVERALL_TIMEOUT", default_value = "60m", value_parser = parse_duration)]
    overall_timeout: Duration,
    #[arg(long, env = "VHTTP_FRAME_BYTES", default_value_t = 30 * 1024)]
    frame_bytes: usize,
    #[arg(long, env = "VHTTP_SEND_WINDOW_FRAMES", default_value_t = 32)]
    send_window_frames: usize,
    #[arg(long, env = "VHTTP_FORWARD_ROUTE_HEADER", default_value = "X-Veilid-Route-Fingerprint")]
    route_header: String,
    /// Validate configuration and persistence layout without starting the adapter.
    #[arg(long)]
    check: bool,
    /// Keep a validation-only bridge process alive for Docker development.
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
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let split = value.find(|character: char| !character.is_ascii_digit()).unwrap_or(value.len());
    let (amount, unit) = value.split_at(split);
    let amount: u64 = amount.parse().map_err(|_| "duration must start with an integer")?;
    let seconds = match unit {
        "s" | "" => amount,
        "m" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(3600),
        _ => return Err("duration unit must be s, m, or h".into()),
    };
    Ok(Duration::from_secs(seconds))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();
    let config = Config::parse();
    let upstream = Url::parse(&config.upstream_url).context("invalid VHTTP_UPSTREAM_URL")?;
    if !matches!(upstream.scheme(), "http" | "https") { bail!("upstream must use http or https"); }
    if config.frame_bytes > veilid_http_wire::DEFAULT_FRAME_LIMIT { bail!("frame bytes exceed conservative VHTTP limit"); }
    if config.send_window_frames == 0 { bail!("send window must be non-zero"); }

    for path in [
        config.data_dir.join("route"),
        config.data_dir.join("transfers"),
        config.data_dir.join("completed"),
        config.data_dir.join("spool"),
    ] {
        fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
    }

    let ready = Ready {
        status: if config.check {
            "configuration-valid"
        } else if config.validation_supervisor {
            "validation-supervisor"
        } else {
            "adapter-required"
        },
        upstream: upstream.as_str(),
        frame_bytes: config.frame_bytes,
        send_window_frames: config.send_window_frames,
        idle_timeout_seconds: config.idle_timeout.as_secs(),
        overall_timeout_seconds: config.overall_timeout.as_secs(),
        adapter: "veilid-server-remote-pending",
    };
    println!("{}", serde_json::to_string(&ready)?);

    if config.check {
        return Ok(());
    }
    if config.validation_supervisor {
        tokio::signal::ctrl_c().await.context("wait for shutdown signal")?;
        return Ok(());
    }
    bail!("live veilid-server remote adapter is not implemented in this foundation build")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_hostile_network_timeouts() {
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
    }
}
