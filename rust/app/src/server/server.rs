use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use log::{debug, error, info, trace};
use protocol::packet::Packet;
use protocol::packet_owned::PacketOwned;
use tokio::{
    net::UdpSocket,
    sync::{Mutex, mpsc::Receiver},
};

use crate::{
    error::AppError,
    server::{
        cc::{self, CongestionControl},
        data_queue::{DataQueue, QueueableRequest},
    },
};

pub struct Server {
    socket: UdpSocket,
    manifest: &'static protocol::data::ManifestTree,
    packet_queue: Mutex<Receiver<PacketOwned>>,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    congestion_control: CongestionControl,
    data_queue: DataQueue,
}

impl Server {
    pub async fn new(
        manifest: &'static protocol::data::ManifestTree,
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
            multicast_addr,
            multicast_port: port,
            congestion_control: CongestionControl::new(),
            data_queue: DataQueue::new(packet_queue_sender),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), AppError> {
        let server = self.clone();
        tokio::task::spawn(async move { server.data_queue.worker().await.unwrap() });
        let server = self.clone();
        tokio::task::spawn(async move { server.sender().await.unwrap() });
        let server = self.clone();
        tokio::task::spawn(async move { server.advertise().await.unwrap() });
        let server = self.clone();
        tokio::task::spawn(async move { server.congestion_control().await.unwrap() });
        self.receiver().await
    }

    async fn receiver(self: Arc<Self>) -> Result<(), AppError> {
        loop {
            let mut buf = vec![0u8; 65536];
            let (len, addr) = self.socket.recv_from(&mut buf).await?;
            trace!("Received {} bytes from {}", len, addr);
            let client = self.clone();
            tokio::task::spawn(async move {
                match client.handle_packet(buf, addr, len).await {
                    Ok(_) => debug!("Handled packet from {}", addr),
                    Err(e) => error!("Error handling packet from {}: {}", addr, e),
                }
            });
        }
    }

    async fn handle_packet(&self, buf: Vec<u8>, addr: SocketAddr, len: usize) -> Result<(), AppError> {
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
                self.data_queue.enqueue(QueueableRequest::LeavesRequest { hash: *request_leaves_packet.root_hash }, &self.manifest).await;
            }
            Packet::RequestLeaf(request_leaf_packet) => {
                info!(
                    "Handling request for leaf from {}, with hash {} and index {}",
                    addr,
                    base16::encode_lower(&request_leaf_packet.root_hash),
                    request_leaf_packet.index
                );
                self.data_queue
                    .enqueue(QueueableRequest::LeafRequest { hash: *request_leaf_packet.root_hash, index: request_leaf_packet.index }, &self.manifest)
                    .await;
            }
            Packet::Data(data_packet) => {
                // TODO: Check if some other server is serving the same hash as we are and cancel our transmission if the other servers IP is lower than ours.
                debug!("Ignoring data packet from {} with root hash {}", addr, base16::encode_lower(&data_packet.root_hash));
            }
            Packet::MerkleTree(merkle_tree_packet) => {
                // Only client needs to handle this
                debug!("Ignoring merkle tree packet from {} with root hash {}", addr, base16::encode_lower(&merkle_tree_packet.root_hash));
            }
            Packet::FeedbackRequest(_) => {
                // Only client needs to handle this
                debug!("Ignoring feedback request packet from {}", addr);
            }
            Packet::Feedback(feedback_packet) => {
                if IpAddr::from(*feedback_packet.server_ip) != self.socket.local_addr()?.ip() {
                    debug!("Ignoring feedback packet from {} because it is not for us", addr);
                    return Ok(());
                }
                debug!("Handling feedback packet from {} with request id {}", addr, feedback_packet.request_id);
                self.congestion_control
                    .handle_feedback(
                        *feedback_packet.number_of_packets_received,
                        *feedback_packet.packet_index,
                        *feedback_packet.request_id,
                        addr.ip(),
                    )
                    .await;
            }
        }
        Ok(())
    }

    async fn sender(&self) -> Result<(), AppError> {
        let mut packet_queue = self.packet_queue.lock().await;
        while let Some(packet) = packet_queue.recv().await {
            trace!("Sending packet: {:?}", packet);
            let bytes = protocol::encode_owned_packet(packet)?;
            trace!("Encoded packet to bytes: {:?}", bytes);
            self.socket.send_to(&bytes, (self.multicast_addr, self.multicast_port)).await?;
            self.congestion_control.add_packet();
            // Sleep for a duration based on the current rate from congestion control to avoid sending too many packets too quickly
            tokio::time::sleep(std::time::Duration::from_millis((1500 / self.congestion_control.get_rate().await) as u64)).await;
            debug!("Sent a packet")
        }
        Ok(())
    }

    async fn advertise(&self) -> Result<(), AppError> {
        loop {
            let advertisement_packet =
                protocol::packet::Packet::Advertisement(protocol::packet::AdvertisementPacket { hash: &self.manifest.root_hash() });
            // Directly send here, these packets are not subject to congestion control
            self.socket.send_to(&protocol::encode_packet(advertisement_packet)?, (self.multicast_addr, self.multicast_port)).await?;
            debug!("Sent advertisement packet with root hash: {}", base16::encode_lower(&self.manifest.root_hash()));
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        }
    }

    async fn congestion_control(&self) -> Result<(), AppError> {
        loop {
            let feedback_request = protocol::packet::Packet::FeedbackRequest(protocol::packet::FeedbackRequestPacket {
                request_id: &self.congestion_control.get_feedback_id(),
                timespan: &cc::FEEDBACK_TIMESPAN,
                number_of_feedbacks: &cc::FEEDBACK_COUNT,
            });
            // Directly send here, these packets are not subject to congestion control
            self.socket.send_to(&protocol::encode_packet(feedback_request)?, (self.multicast_addr, self.multicast_port)).await?;
            debug!("Sent feedback request packet with request id: {}", self.congestion_control.get_feedback_id());
            tokio::time::sleep(std::time::Duration::from_millis((cc::FEEDBACK_TIMESPAN + cc::FEEDBACK_TIMESPAN / 2) as u64)).await;
            self.congestion_control.adjust_rate().await;
        }
    }
}
