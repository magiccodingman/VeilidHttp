use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use std::{fs, path::PathBuf};

#[derive(Debug, Parser)]
#[command(name = "veilid-http-cli", version, about = "VeilidHttp route and server tooling")]
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
    /// Validate environment and persistence paths.
    Status {
        #[arg(long, env = "VHTTP_DATA_DIR", default_value = "/data/bridge")]
        data_dir: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum RouteCommand {
    /// Show fingerprint and file sizes.
    Show {
        #[arg(long, default_value = "/data/bridge/route/current.blob")]
        file: PathBuf,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ExportFormat { Base64, Descriptor }

#[derive(Debug, Serialize)]
struct Descriptor<'a> {
    schema: &'static str,
    name: &'a str,
    #[serde(rename = "routeBlob")]
    route_blob: String,
    #[serde(rename = "startPath")]
    start_path: &'static str,
}

fn read(path: &PathBuf) -> Result<Vec<u8>> {
    fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Route { command } => match command {
            RouteCommand::Show { file } => {
                let bytes = read(&file)?;
                println!("fingerprint={}", veilid_http_route::fingerprint(&bytes));
                println!("blob_bytes={}", bytes.len());
                println!("base64_bytes={}", veilid_http_route::encode_route_blob(&bytes).len());
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
            RouteCommand::Fingerprint { file } => println!("{}", veilid_http_route::fingerprint(&read(&file)?)),
        },
        Command::Status { data_dir } => {
            if !data_dir.exists() { bail!("data directory does not exist: {}", data_dir.display()); }
            println!("data_dir={}", data_dir.display());
            println!("route_present={}", data_dir.join("route/current.blob").is_file());
        }
    }
    Ok(())
}
