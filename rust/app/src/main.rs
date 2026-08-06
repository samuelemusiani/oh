use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;
use std::net::IpAddr;
use std::path::PathBuf;

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
    let v: f64 = s
        .parse()
        .map_err(|_| format!("'{s}' is not a valid number"))?;
    if (0.0..=1.0).contains(&v) {
        Ok(v)
    } else {
        Err(format!("probability must be in [0.0, 1.0], got {v}"))
    }
}

fn main() -> Result<()> {
    #[cfg(debug_assertions)]
    {
        simple_logger::SimpleLogger::new()
            .env()
            .init()
            .expect("Failed to initialize logging");
    }
    #[cfg(not(debug_assertions))]
    {
        simple_logger::SimpleLogger::new()
            .with_level(log::LevelFilter::Info)
            .env()
            .init()
            .expect("Failed to initialize logging");
    }
    let args = Args::parse();
    info!("Parsed arguments: {args:?}");

    let loss = args.loss_model();
    info!("Selected packet loss model: {loss:?}");

    match args.mode {
        Mode::Send { files } => {
            println!("Sending {files:?}");
            todo!();
        }
        Mode::Receive { download_directory } => {
            println!("Receiving into {}", download_directory.display());
            todo!();
        }
    }
}
