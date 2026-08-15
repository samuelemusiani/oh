use std::{
    net::{Ipv4Addr, Ipv6Addr, IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    collections::HashMap,
};

use log::{debug, error, trace, warn};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::error::AppError;

struct FeedbackState {
    last_reqest_id: u16,
    t_request_received: std::time::Instant,
    window_duration: std::time::Duration,
    num_feedbacks_to_send: u16,
    num_feedbacks_sent: u16,
    num_packets_received_in_window: u16,
    t_next_send: Option<std::time::Instant>,
}

pub struct Client {
    socket: UdpSocket,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    output_directory: PathBuf,
    feedback_states: Mutex<HashMap<Ipv6Addr, FeedbackState>>, // TODO: index by ip,port instead?
}

impl Client {
    pub async fn new(
        multicast_addr: IpAddr,
        interface: Option<IpAddr>,
        port: u16,
        output_directory: PathBuf,
    ) -> Result<Self, AppError> {
        let output_dir_metadata = output_directory.metadata()?;
        if !output_dir_metadata.is_dir() {
            return Err(AppError::InvalidArgument(format!("Output directory {} is not a directory", output_directory.display())));
        }
        let multicast_addr: Ipv4Addr = match multicast_addr {
            IpAddr::V4(ipv4_addr) => ipv4_addr,
            IpAddr::V6(ipv6_addr) => {
                return Err(AppError::InvalidArgument(format!("IPv6 multicast address {} is not supported", ipv6_addr)));
            }
        };
        let interface = match interface {
            Some(IpAddr::V4(ipv4_addr)) => ipv4_addr,
            Some(IpAddr::V6(ipv6_addr)) => {
                return Err(AppError::InvalidArgument(format!("IPv6 interface address {} is not supported", ipv6_addr)));
            }
            None => Ipv4Addr::UNSPECIFIED,
        };
        let socket = UdpSocket::bind((interface, port)).await?;
        socket.join_multicast_v4(multicast_addr, interface)?;

        Ok(Client {
            socket,
            multicast_addr,
            multicast_port: port,
            output_directory,
            feedback_states: Mutex::new(HashMap::default()),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), AppError> {
        let client = self.clone();
        let feedback_sender_job = tokio::task::spawn(async move {
            client.feedback_sender_run().await
        });
        self.receiver().await?;
        let _ = feedback_sender_job.await.unwrap();
        Ok(())
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

    async fn handle_packet(self: Arc<Self>, buf: Vec<u8>, addr: SocketAddr, len: usize) -> Result<(), AppError> {
        let packet = protocol::parse_packet(&buf[..len])?;
        match packet {
            protocol::packet::Packet::Advertisement(advertisement_packet) => todo!(),

            protocol::packet::Packet::Metadata(metadata_packet) => todo!(),

            protocol::packet::Packet::RequestLeaves(request_leaves_packet) => {
                // Only server needs to handle this
                debug!("Ignoring RequestLeaves packet from {} for root hash {}", addr, base16::encode_lower(request_leaves_packet.root_hash));
            },

            protocol::packet::Packet::RequestLeaf(request_leaf_packet) => {
                // Only server needs to handle this
                debug!("Ignoring RequestLeaf packet from {} for root hash {}", addr, base16::encode_lower(request_leaf_packet.root_hash));
            },

            protocol::packet::Packet::Data(data_packet) => {
                let len = data_packet.length; // this is not be I think (kinda awkward to convert here...)
                debug!("Handling Data packet from {} ({} bytes)", addr, len);

                let server_ip6 = match addr {
                    SocketAddr::V4(addr_ip4) => { addr_ip4.ip().to_ipv6_mapped() },
                    SocketAddr::V6(addr_ip6) => { *addr_ip6.ip() },
                };

                // Update feedback state of server
                // FIXME: respect use ECN markings, see https://datatracker.ietf.org/doc/draft-ietf-tsvwg-udp-ecn/
                let mut feedback_states = self.feedback_states.lock().await;
                if let Some(feedback_state) = feedback_states.get_mut(&server_ip6) {
                    feedback_state.num_packets_received_in_window =
                            u16::saturating_add(feedback_state.num_packets_received_in_window, 1);
                }

                // Use the data
                todo!("use the data");
            },

            protocol::packet::Packet::MerkleTree(merkle_tree_packet) => todo!(),

            protocol::packet::Packet::FeedbackRequest(feedback_request_packet) => {
                let req_id = u16::from_be(*feedback_request_packet.request_id);
                debug!("Handling FeedbackRequest from {} (reqest_id = {})", addr, req_id);

                let server_ip6 = match addr {
                    SocketAddr::V4(addr_ip4) => { addr_ip4.ip().to_ipv6_mapped() },
                    SocketAddr::V6(addr_ip6) => { *addr_ip6.ip() },
                };
                let timespan_ms = u16::from_be(*feedback_request_packet.timespan);
                let timespan = std::time::Duration::from_millis(timespan_ms.into());
                let num_feedbacks_to_send = u16::from_be(*feedback_request_packet.number_of_feedbacks);

                if timespan_ms == 0 && num_feedbacks_to_send != 0 {
                    warn!("FeedbackRequest has a timespan of 0 ms, but non-zero packets.");
                    // (Note: timespan_ms == 0 is valid. it means feedback should stop immediately)
                }

                // allow max this many feedback packets per second
                const MAX_NUM_FEEDBACK_PACKETS_PER_SEC: u32 = 2000;
                let max_feedbacks = u16::try_from(MAX_NUM_FEEDBACK_PACKETS_PER_SEC * u32::from(timespan_ms) / 1000).unwrap_or(u16::MAX);
                if num_feedbacks_to_send > max_feedbacks {
                    warn!("FeedbackRequest num feedbacks too big ({}). Range-limiting to {}.",
                            num_feedbacks_to_send, max_feedbacks);
                }
                let num_feedbacks_to_send = u16::min(num_feedbacks_to_send, max_feedbacks);

                // Update the server's feedback state
                let mut feedback_states = self.feedback_states.lock().await;
                if let Some(feedback_state) = feedback_states.get_mut(&server_ip6) {
                    // only update if more recent request id
                    if u16::wrapping_sub(req_id, feedback_state.last_reqest_id).cast_signed() > 0 {
                        feedback_state.last_reqest_id = req_id;
                        feedback_state.t_request_received = std::time::Instant::now();
                        feedback_state.window_duration = timespan;
                        feedback_state.num_feedbacks_to_send = num_feedbacks_to_send;
                        feedback_state.num_feedbacks_sent = 0;
                        feedback_state.num_packets_received_in_window = 0;
                    }
                } else {
                    feedback_states.insert(server_ip6, FeedbackState {
                        last_reqest_id: req_id,
                        t_request_received: std::time::Instant::now(),
                        window_duration: timespan,
                        num_feedbacks_to_send,
                        num_feedbacks_sent: 0,
                        num_packets_received_in_window: 0,
                        t_next_send: None,
                    });
                }
            },

            protocol::packet::Packet::Feedback(feedback_packet) => {
                // Only server needs to handle this
                debug!("Ignoring feedback packet from {} for {}", addr, base16::encode_lower(feedback_packet.server_ip));
            },
        }

        Ok(())
    }

    async fn feedback_sender_run(&self) -> Result<(), AppError> {
        loop {
            let tnow = std::time::Instant::now();
            let mut to_delete = vec![];

            let mut feedback_states = self.feedback_states.lock().await;

            for (server_ip, feedback_state) in feedback_states.iter_mut() {
                let tend = feedback_state.t_request_received + feedback_state.window_duration;

                // timed out?
                if tend >= tnow {
                    to_delete.push(*server_ip);
                    continue;
                }

                // finished?
                if feedback_state.num_feedbacks_to_send <= feedback_state.num_feedbacks_sent {
                    to_delete.push(*server_ip);
                    continue;
                }

                let t_next_send = if let Some(t_next_send) = feedback_state.t_next_send {
                    t_next_send
                } else {
                    // send randomly within the timeslot
                    let interval = feedback_state.window_duration.as_secs_f32() / f32::from(feedback_state.num_feedbacks_to_send);
                    let t_next_send = feedback_state.t_request_received + std::time::Duration::from_secs_f32(
                            interval * (f32::from(feedback_state.num_feedbacks_sent) + rand::random_range(0.0..1.0))
                        );
                    feedback_state.t_next_send = Some(t_next_send);
                    t_next_send
                };

                // time for next feedback packet?
                if t_next_send >= tnow {
                    let time_since_ms = (tnow - feedback_state.t_request_received).as_millis() as u64;
                    let window_timespan_ms = feedback_state.window_duration.as_millis() as u64;
                    let packet_idx = (u16::MAX as u64) * time_since_ms / window_timespan_ms;

                    let feedback_packet =
                        protocol::packet::Packet::Feedback(protocol::packet::FeedbackPacket{
                            request_id: &u16::to_be(feedback_state.last_reqest_id),
                            packet_index: &u16::to_be(packet_idx as u16),
                            number_of_packets_received: &u16::to_be(feedback_state.num_packets_received_in_window),
                            server_ip: &server_ip.octets(),
                        });
                    self.socket.send_to(&protocol::encode_packet(feedback_packet)?, (self.multicast_addr, self.multicast_port)).await?;
                    debug!("Sent feedback packet for {}", server_ip);

                    feedback_state.t_next_send = None;
                    feedback_state.num_feedbacks_sent += 1;
                }
            }

            // remove obsolete states
            for server_ip in to_delete {
                feedback_states.remove(&server_ip);
            }

            // TODO: awake me on feedback request
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
