use std::net::{Ipv4Addr, SocketAddr};

use log::{debug, error, info, trace};
use protocol::packet::Packet;
use tokio::{
    net::UdpSocket,
    sync::{
        Mutex,
        mpsc::{Receiver, Sender},
    },
};

use crate::{error::AppError, server::cc::CongestionControl};

pub struct Server {
    socket: UdpSocket,
    manifest: protocol::data::ManifestTree,
    packet_queue: Mutex<Receiver<Packet<'static>>>,
    packet_queue_sender: Sender<Packet<'static>>,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    congestion_control: CongestionControl,
}

impl Server {
    pub async fn new(
        manifest: protocol::data::ManifestTree,
        multicast_addr: std::net::IpAddr,
        interface: Option<std::net::IpAddr>,
        port: u16,
    ) -> Result<Self, AppError> {
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
        let (packet_queue_sender, packet_queue) = tokio::sync::mpsc::channel(100);
        Ok(Server {
            socket,
            manifest,
            packet_queue: Mutex::new(packet_queue),
            packet_queue_sender,
            multicast_addr,
            multicast_port: port,
            congestion_control: CongestionControl::new(),
        })
    }

    pub async fn run(&'static self) -> Result<(), AppError> {
        tokio::task::spawn(async move { self.sender().await.unwrap() });
        tokio::task::spawn(async move { self.advertise().await.unwrap() });
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
            Packet::Advertisement(advertisement_packet) => {
                // Only client needs to handle this
                debug!("Ignoring advertisement packet from {} with hash {}", addr, base16::encode_lower(&advertisement_packet.hash));
            }
            Packet::Metadata(metadata_packet) => {
                // Only client needs to handle this
                debug!("Ignoring metadata packet from {} with manifest hash {}", addr, base16::encode_lower(&metadata_packet.manifest_hash));
            }
            Packet::RequestLeaves(request_leaves_packet) => {
                info!("Handling request for leaves from {}, with hash {}", addr, base16::encode_lower(&request_leaves_packet.root_hash));
                if !self.manifest.contains_hash(&request_leaves_packet.root_hash) {
                    debug!(
                        "Ignoring request for leaves from {} with hash {} because we don't have it",
                        addr,
                        base16::encode_lower(&request_leaves_packet.root_hash)
                    );
                    return Ok(());
                }
            }
            Packet::RequestLeaf(request_leaf_packet) => todo!(),
            Packet::Data(data_packet) => {
                // TODO: Check if some other server is serving the same hash as we are and cancel our transmission if the other servers IP is lower than ours.
                debug!("Ignoring data packet from {} with root hash {}", addr, base16::encode_lower(&data_packet.root_hash));
            }
            Packet::MerkleTree(merkle_tree_packet) => {
                // Only client needs to handle this
                debug!("Ignoring merkle tree packet from {} with root hash {}", addr, base16::encode_lower(&merkle_tree_packet.root_hash));
            }
            Packet::FeedbackRequest(feedback_request_packet) => {
                // Only client needs to handle this
                debug!("Ignoring feedback request packet from {}", addr);
            }
            Packet::Feedback(_) => {}
        }
        Ok(())
    }

    async fn sender(&'static self) -> Result<(), AppError> {
        let mut packet_queue = self.packet_queue.lock().await;
        while let Some(packet) = packet_queue.recv().await {
            trace!("Sending packet: {:?}", packet);
            let bytes = protocol::encode_packet(packet)?;
            trace!("Encoded packet to bytes: {:?}", bytes);
            self.socket.send_to(&bytes, (self.multicast_addr, self.multicast_port)).await?;
            debug!("Sent a packet")
        }
        Ok(())
    }

    async fn advertise(&'static self) -> Result<(), AppError> {
        loop {
            let advertisement_packet =
                protocol::packet::Packet::Advertisement(protocol::packet::AdvertisementPacket { hash: &self.manifest.root_hash });
            self.packet_queue_sender.send(advertisement_packet).await?;
            debug!("Sent advertisement packet with root hash: {}", base16::encode_lower(&self.manifest.root_hash));
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }

    async fn congestion_control(&'static self) -> Result<(), AppError> {
        todo!()
    }
}
