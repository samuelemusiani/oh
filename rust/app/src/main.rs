use crate::client::Client;
use crate::server::Server;
use clap::Parser;
use get_if_addrs::get_if_addrs;
use log::{debug, info};
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;

mod client;
mod error;
mod server;

#[derive(Parser, Debug)]
#[command(name = "cli")]
pub struct Args {
    #[arg(short = 'a', long = "address")]
    pub multicast_addr: IpAddr,

    #[arg(short = 'i', long = "interface")]
    pub interface: Option<String>,

    #[arg(short = 't', long = "port", default_value_t = 5000)]
    pub port: u16,

    #[arg(short = 'p', long = "send_packet_loss", value_parser = parse_probability)]
    pub loss_send: Option<f64>,

    #[arg(short = 'q', long = "receive_packet_loss", value_parser = parse_probability)]
    pub loss_receive: Option<f64>,

    #[arg(short = 's', long = "serve", num_args = 1..)]
    pub server_files: Option<Vec<PathBuf>>,

    #[arg(conflicts_with = "server_files")]
    pub download_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy)]
pub enum LossModel {
    None,
    Symmetric { p: f64 },
    Asymmetric { p: f64, q: f64 },
}

impl Args {
    pub const fn loss_model(&self) -> LossModel {
        match (self.loss_send, self.loss_receive) {
            (None, None) => LossModel::None,
            (Some(p), None) | (None, Some(p)) => LossModel::Symmetric { p },
            (Some(p), Some(q)) => LossModel::Asymmetric { p, q },
        }
    }
}

fn parse_probability(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("'{s}' is not a valid number"))?;
    if (0.0..=1.0).contains(&v) { Ok(v) } else { Err(format!("probability must be in [0.0, 1.0], got {v}")) }
}

fn get_ipv4_for_interface(iface_name: &str) -> Result<Ipv4Addr, String> {
    let interfaces = get_if_addrs().map_err(|e| format!("Failed to get interfaces: {}", e))?;
    for iface in interfaces {
        if iface.name == iface_name {
            if let std::net::IpAddr::V4(ipv4) = iface.addr.ip() {
                return Ok(ipv4);
            }
        }
    }
    Err(format!("Could not find IPv4 address for interface '{}'", iface_name))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(debug_assertions)]
    {
        simple_logger::SimpleLogger::new().env().init().expect("Failed to initialize logging");
    }
    #[cfg(not(debug_assertions))]
    {
        simple_logger::SimpleLogger::new().env().with_level(log::LevelFilter::Info).init().expect("Failed to initialize logging");
    }
    let args = Args::parse();
    info!("Parsed arguments: {args:?}");

    let loss = args.loss_model();
    info!("Selected packet loss model: {loss:?}");

    let iface_ip = match args.interface {
        Some(ref iface_name) => get_ipv4_for_interface(iface_name).map_err(|e| anyhow::anyhow!(e))?,
        None => Ipv4Addr::UNSPECIFIED,
    };

    if let Some(mut files) = args.server_files {
        if let [only] = files.as_slice() {
            if only.is_dir() {
                let mut entries = Vec::new();
                let mut read_dir = tokio::fs::read_dir(only).await?;
                while let Some(entry) = read_dir.next_entry().await? {
                    if entry.file_type().await?.is_file() {
                        entries.push(entry.path());
                    }
                }
                entries.sort();
                files = entries;
            }
        }
        info!("Sending files: {files:?}");
        debug!("Generating data structures for files...");
        let manifest = protocol::build_manifest_from_files(&files).await?;
        let manifest = Box::leak(Box::new(manifest));
        info!("Generated manifest: {}", base16::encode_lower(&manifest.root_hash()));
        let server = Arc::new(Server::new(manifest, args.multicast_addr, iface_ip, args.port).await?);
        info!("Server initialized, running...");
        server.run().await?;
        let _ = tokio::signal::ctrl_c().await;
        info!("Shutting down server...");
        return Ok(());
    }
    if let Some(download_directory) = args.download_dir {
        info!("Receiving into {}", download_directory.display());
        let client = Arc::new(Client::new(args.multicast_addr, iface_ip, args.port, download_directory).await?);
        info!("Client initialized, running...");
        client.run().await?;
        let _ = tokio::signal::ctrl_c().await;
        info!("Shutting down client...");
        return Ok(());
    }
    return Ok(());
}
