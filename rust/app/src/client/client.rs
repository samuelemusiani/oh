use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use log::{debug, error, trace};
use tokio::net::UdpSocket;

use crate::error::AppError;

pub struct Client {
    socket: UdpSocket,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    output_directory: PathBuf,
}

impl Client {
    pub async fn new(
        multicast_addr: std::net::IpAddr,
        interface: Option<std::net::IpAddr>,
        port: u16,
        output_directory: PathBuf,
    ) -> Result<Self, AppError> {
        let output_dir_metadata = output_directory.metadata()?;
        if !output_dir_metadata.is_dir() {
            return Err(AppError::InvalidArgument(format!("Output directory {} is not a directory", output_directory.display())));
        }
        let multicast_addr: Ipv4Addr = match multicast_addr {
            std::net::IpAddr::V4(ipv4_addr) => ipv4_addr,
            std::net::IpAddr::V6(ipv6_addr) => {
                return Err(AppError::InvalidArgument(format!("IPv6 multicast address {} is not supported", ipv6_addr)));
            }
        };
        let interface = match interface {
            Some(std::net::IpAddr::V4(ipv4_addr)) => ipv4_addr,
            Some(std::net::IpAddr::V6(ipv6_addr)) => {
                return Err(AppError::InvalidArgument(format!("IPv6 interface address {} is not supported", ipv6_addr)));
            }
            None => Ipv4Addr::UNSPECIFIED,
        };
        let socket = UdpSocket::bind((interface, port)).await?;
        socket.join_multicast_v4(multicast_addr, interface)?;

        Ok(Client { socket, multicast_addr, multicast_port: port, output_directory })
    }

    pub async fn run(&'static self) -> Result<(), AppError> {
        self.receiver().await
    }

    async fn receiver(&'static self) -> Result<(), AppError> {
        loop {
            let mut buf = vec![0u8; 65536];
            let (len, addr) = self.socket.recv_from(&mut buf).await?;
            trace!("Received {} bytes from {}", len, addr);
            tokio::task::spawn(async move {
                match self.handle_packet(buf, addr, len).await {
                    Ok(_) => debug!("Handled packet from {}", addr),
                    Err(e) => error!("Error handling packet from {}: {}", addr, e),
                }
            });
        }
    }
    async fn handle_packet(&'static self, buf: Vec<u8>, addr: SocketAddr, len: usize) -> Result<(), AppError> {
        let packet = protocol::parse_packet(&buf[..len])?;
        match packet {
            protocol::packet::Packet::Advertisement(advertisement_packet) => todo!(),
            protocol::packet::Packet::Metadata(metadata_packet) => todo!(),
            protocol::packet::Packet::RequestLeaves(request_leaves_packet) => todo!(),
            protocol::packet::Packet::RequestLeaf(request_leaf_packet) => todo!(),
            protocol::packet::Packet::Data(data_packet) => todo!(),
            protocol::packet::Packet::MerkleTree(merkle_tree_packet) => todo!(),
            protocol::packet::Packet::FeedbackRequest(feedback_request_packet) => todo!(),
            protocol::packet::Packet::Feedback(feedback_packet) => todo!(),
        }
    }
}
