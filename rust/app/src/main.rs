use clap::{Parser, Subcommand};
use log::{debug, info};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::client::Client;
use crate::server::Server;

mod client;
mod error;
mod server;

#[derive(Parser, Debug)]
#[command(name = "cli")]
pub struct Args {
    #[arg(short = 'a', long = "address")]
    pub multicast_addr: IpAddr,

    #[arg(short = 'i', long = "interface")]
    pub interface: Option<IpAddr>,

    #[arg(short = 't', long = "port", default_value_t = 5000)]
    pub port: u16,

    #[arg(short = 'p', long = "send_packet_loss", value_parser = parse_probability)]
    pub loss_send: Option<f64>,

    #[arg(short = 'q', long = "receive_packet_loss", value_parser = parse_probability)]
    pub loss_receive: Option<f64>,

    #[command(subcommand)]
    pub mode: Mode,
}

#[derive(Subcommand, Debug)]
pub enum Mode {
    Send {
        #[arg(required = true, num_args = 1..)]
        files: Vec<PathBuf>,
    },
    Receive {
        download_directory: PathBuf,
    },
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(debug_assertions)]
    {
        simple_logger::SimpleLogger::new().env().init().expect("Failed to initialize logging");
    }
    #[cfg(not(debug_assertions))]
    {
        simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).env().init().expect("Failed to initialize logging");
    }
    let args = Args::parse();
    info!("Parsed arguments: {args:?}");

    let loss = args.loss_model();
    info!("Selected packet loss model: {loss:?}");

    match args.mode {
        Mode::Send { files } => {
            info!("Sending files: {files:?}");
            debug!("Generating data structures for files...");
            let manifest = protocol::build_manifest_from_files(&files).await?;
            let manifest = Box::leak(Box::new(manifest));
            info!("Generated manifest: {}", base16::encode_lower(&manifest.root_hash()));
            let server = Arc::new(Server::new(manifest, args.multicast_addr, args.interface, args.port).await?);
            info!("Server initialized, running...");
            server.run().await?;
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutting down server...");
            Ok(())
        }
        Mode::Receive { download_directory } => {
            println!("Receiving into {}", download_directory.display());
            let client = Arc::new(Client::new(args.multicast_addr, args.interface, args.port, download_directory).await?);
            info!("Client initialized, running...");
            client.run().await?;
            let _ = tokio::signal::ctrl_c().await;
            info!("Shutting down client...");
            Ok(())
        }
    }
}
