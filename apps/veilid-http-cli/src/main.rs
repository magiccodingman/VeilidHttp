//! Operator CLI for inspecting, exporting, and validating VeilidHttp bridge state.

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, Parser)]
#[command(
    name = "veilid-http-cli",
    version,
    about = "VeilidHttp route and server tooling"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect or export the current server RouteBlob.
    Route {
        #[command(subcommand)]
        command: RouteCommand,
    },
    /// Report whether the bridge has published a usable private route.
    Status {
        #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
        data_dir: PathBuf,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect retained transfer state.
    Transfers {
        #[command(subcommand)]
        command: TransferCommand,
    },
}

#[derive(Debug, Subcommand)]
enum RouteCommand {
    /// Show current route metadata and measured blob sizes.
    Show {
        #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
        data_dir: PathBuf,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
    /// Export the current route.
    Export {
        #[arg(long, default_value = "/data/bridge/route/current.blob")]
        file: PathBuf,
        #[arg(long, value_enum, default_value = "base64")]
        format: ExportFormat,
    },
    /// Fingerprint an arbitrary RouteBlob file.
    Fingerprint { file: PathBuf },
}

#[derive(Debug, Subcommand)]
enum TransferCommand {
    /// List retained transfer journal/spool entries.
    List {
        #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
        data_dir: PathBuf,
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportFormat {
    Base64,
    Descriptor,
}

#[derive(Debug, Serialize)]
struct Descriptor<'a> {
    schema: &'static str,
    name: &'a str,
    #[serde(rename = "routeBlob")]
    route_blob: String,
    #[serde(rename = "startPath")]
    start_path: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RouteMetadata {
    schema: String,
    route_id: String,
    fingerprint: String,
    created_at_unix_seconds: u64,
    veilid_version: String,
    blob_file: String,
    base64_file: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RouteReport {
    metadata: RouteMetadata,
    blob_bytes: usize,
    base64_bytes: usize,
    fingerprint_verified: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusReport {
    status: &'static str,
    data_dir: String,
    route_present: bool,
    route_metadata_present: bool,
    route_fingerprint: Option<String>,
    veilid_version: Option<String>,
    active_transfer_entries: usize,
    completed_entries: usize,
    spool_entries: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct TransferEntry {
    area: String,
    name: String,
    bytes: u64,
}

fn read(path: &Path) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn count_entries(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(fs::read_dir(path)
        .with_context(|| format!("failed to list {}", path.display()))?
        .filter_map(Result::ok)
        .count())
}

fn route_report(data_dir: &Path) -> Result<RouteReport> {
    let route_dir = data_dir.join("route");
    let blob = read(&route_dir.join("current.blob"))?;
    let metadata: RouteMetadata = serde_json::from_slice(&read(&route_dir.join("current.json"))?)
        .context("failed to parse current route metadata")?;
    let measured = veilid_http_route::fingerprint(&blob);
    let encoded = veilid_http_route::encode_route_blob(&blob);
    Ok(RouteReport {
        fingerprint_verified: measured == metadata.fingerprint,
        metadata,
        blob_bytes: blob.len(),
        base64_bytes: encoded.len(),
    })
}

fn list_transfer_entries(data_dir: &Path) -> Result<Vec<TransferEntry>> {
    let mut entries = Vec::new();
    for area in ["transfers", "completed", "spool"] {
        let directory = data_dir.join(area);
        if !directory.exists() {
            continue;
        }
        for item in fs::read_dir(&directory)
            .with_context(|| format!("failed to list {}", directory.display()))?
        {
            let item = item?;
            let metadata = item.metadata()?;
            entries.push(TransferEntry {
                area: area.to_owned(),
                name: item.file_name().to_string_lossy().into_owned(),
                bytes: if metadata.is_file() {
                    metadata.len()
                } else {
                    0
                },
            });
        }
    }
    entries.sort_by(|left, right| (&left.area, &left.name).cmp(&(&right.area, &right.name)));
    Ok(entries)
}

fn handle_route(command: RouteCommand) -> Result<()> {
    match command {
        RouteCommand::Show { data_dir, json } => {
            let report = route_report(&data_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("fingerprint={}", report.metadata.fingerprint);
                println!("fingerprint_verified={}", report.fingerprint_verified);
                println!("route_id={}", report.metadata.route_id);
                println!("veilid_version={}", report.metadata.veilid_version);
                println!(
                    "created_at_unix_seconds={}",
                    report.metadata.created_at_unix_seconds
                );
                println!("blob_bytes={}", report.blob_bytes);
                println!("base64_bytes={}", report.base64_bytes);
            }
            if !report.fingerprint_verified {
                bail!("current RouteBlob does not match its persisted fingerprint");
            }
        }
        RouteCommand::Export { file, format } => {
            let bytes = read(&file)?;
            let encoded = veilid_http_route::encode_route_blob(&bytes);
            match format {
                ExportFormat::Base64 => println!("{encoded}"),
                ExportFormat::Descriptor => println!(
                    "{}",
                    serde_json::to_string_pretty(&Descriptor {
                        schema: "org.veilidhttp.app/v1",
                        name: "VeilidHttp Site",
                        route_blob: encoded,
                        start_path: "/",
                    })?
                ),
            }
        }
        RouteCommand::Fingerprint { file } => {
            println!("{}", veilid_http_route::fingerprint(&read(&file)?));
        }
    }
    Ok(())
}

fn status_report(data_dir: &Path) -> Result<StatusReport> {
    if !data_dir.exists() {
        bail!("data directory does not exist: {}", data_dir.display());
    }
    let route = route_report(data_dir).ok();
    Ok(StatusReport {
        status: if route
            .as_ref()
            .is_some_and(|value| value.fingerprint_verified)
        {
            "ready"
        } else {
            "not-ready"
        },
        data_dir: data_dir.display().to_string(),
        route_present: data_dir.join("route/current.blob").is_file(),
        route_metadata_present: data_dir.join("route/current.json").is_file(),
        route_fingerprint: route
            .as_ref()
            .map(|value| value.metadata.fingerprint.clone()),
        veilid_version: route
            .as_ref()
            .map(|value| value.metadata.veilid_version.clone()),
        active_transfer_entries: count_entries(&data_dir.join("transfers"))?,
        completed_entries: count_entries(&data_dir.join("completed"))?,
        spool_entries: count_entries(&data_dir.join("spool"))?,
    })
}

fn handle_status(data_dir: PathBuf, json: bool) -> Result<()> {
    let report = status_report(&data_dir)?;
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("status={}", report.status);
        println!("data_dir={}", report.data_dir);
        println!("route_present={}", report.route_present);
        println!("route_metadata_present={}", report.route_metadata_present);
        println!(
            "route_fingerprint={}",
            report.route_fingerprint.as_deref().unwrap_or("")
        );
        println!(
            "veilid_version={}",
            report.veilid_version.as_deref().unwrap_or("")
        );
        println!("active_transfer_entries={}", report.active_transfer_entries);
        println!("completed_entries={}", report.completed_entries);
        println!("spool_entries={}", report.spool_entries);
    }
    if report.status != "ready" {
        bail!("VeilidHttp bridge has not published a verified current private route");
    }
    Ok(())
}

fn handle_transfers(command: TransferCommand) -> Result<()> {
    match command {
        TransferCommand::List { data_dir, json } => {
            let entries = list_transfer_entries(&data_dir)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&entries)?);
            } else if entries.is_empty() {
                println!("no retained transfer entries");
            } else {
                for entry in entries {
                    println!(
                        "area={} name={} bytes={}",
                        entry.area, entry.name, entry.bytes
                    );
                }
            }
        }
    }
    Ok(())
}

fn run(command: Command) -> Result<()> {
    match command {
        Command::Route { command } => handle_route(command),
        Command::Status { data_dir, json } => handle_status(data_dir, json),
        Command::Transfers { command } => handle_transfers(command),
    }
}

fn main() -> Result<()> {
    run(Cli::parse().command)
}
