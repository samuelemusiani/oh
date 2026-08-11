use log::{debug, info, trace};
use protocol::packet::Packet;
use tokio::{
    net::UdpSocket,
    sync::{
        Mutex,
        mpsc::{Receiver, Sender},
    },
};

use crate::error::AppError;

pub struct Server {
    socket: UdpSocket,
    manifest: protocol::data::ManifestTree,
    packet_queue: Mutex<Receiver<Packet<'static>>>,
    packet_queue_sender: Sender<Packet<'static>>,
    multicast_addr: std::net::IpAddr,
    multicast_port: u16,
}

impl Server {
    pub async fn new(
        manifest: protocol::data::ManifestTree,
        multicast_addr: std::net::IpAddr,
        interface: Option<std::net::IpAddr>,
        port: u16,
    ) -> Result<Self, AppError> {
        let socket;
        let interface = interface.unwrap_or_else(|| match multicast_addr {
            std::net::IpAddr::V4(_) => std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            std::net::IpAddr::V6(_) => std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
        });
        match interface {
            std::net::IpAddr::V4(addr) => {
                socket = UdpSocket::bind((addr, port)).await?;
            }
            std::net::IpAddr::V6(addr) => {
                socket = UdpSocket::bind((addr, port)).await?;
            }
        }
        match multicast_addr {
            std::net::IpAddr::V4(addr) => socket.join_multicast_v4(addr, std::net::Ipv4Addr::UNSPECIFIED)?,
            std::net::IpAddr::V6(addr) => socket.join_multicast_v6(&addr, 0)?,
        }

        let (packet_queue_sender, packet_queue) = tokio::sync::mpsc::channel(100);
        Ok(Server { socket, manifest, packet_queue: Mutex::new(packet_queue), packet_queue_sender, multicast_addr, multicast_port: port })
    }

    pub async fn run(&'static self) -> Result<(), AppError> {
        tokio::task::spawn(async move { self.sender().await.unwrap() });
        tokio::task::spawn(async move { self.advertise().await.unwrap() });
        self.receiver().await
    }

    async fn receiver(&'static self) -> Result<(), AppError> {
        let mut buf = [0u8; 65536];
        loop {
            let (len, addr) = self.socket.recv_from(&mut buf).await?;
            trace!("Received {} bytes from {}", len, addr);
            let packet = protocol::parse_packet(&buf[..len])?;
            trace!("Parsed packet: {:?}", packet);
        }
    }

    async fn sender(&'static self) -> Result<(), AppError> {
        let mut packet_queue = self.packet_queue.lock().await;
        while let Some(packet) = packet_queue.recv().await {
            trace!("Sending packet: {:?}", packet);
            let bytes = protocol::encode_packet(packet)?;
            trace!("Encoded packet to bytes: {:?}", bytes);
            match self.multicast_addr {
                std::net::IpAddr::V4(ipv4_addr) => self.socket.send_to(&bytes, format!("{}:{}", ipv4_addr, self.multicast_port)).await?,
                std::net::IpAddr::V6(ipv6_addr) => self.socket.send_to(&bytes, format!("{}:{}", ipv6_addr, self.multicast_port)).await?,
            };
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
}
