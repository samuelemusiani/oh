use std::{
    net::{Ipv4Addr, Ipv6Addr, IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    collections::HashMap,
};

use log::{debug, error, trace, warn};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use protocol::Hash;
use crate::error::AppError;
use crate::client::merkle_pool::*;

// allow max this many feedback packets per second
const MAX_NUM_FEEDBACK_PACKETS_PER_SEC: u32 = 2000;

struct FeedbackState {
    last_reqest_id: u16,
    t_request_received: std::time::Instant,
    window_duration: std::time::Duration,
    num_feedbacks_to_send: u16,
    num_feedbacks_sent: u16,
    num_packets_received_in_window: u16,
    t_next_send: Option<std::time::Instant>,
}

struct FileMetadata {
    size: usize,
    merkle_tree_height: u8,
    name: Box<[u8]>,
}

// lock order of fields with locks is in delcaration order in struct
pub struct Client {
    socket: UdpSocket,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    output_directory: PathBuf,
    // Any hashes that we know about.
    merkle_pool: Mutex<MerklePool>,
    // How much data we received and how much feedback we should send, per sender ip.
    feedback_states: Mutex<HashMap<Ipv6Addr, FeedbackState>>,
    // Hashes that we want to request.
    // (We always try to request from lowest known tree level.)
    // Manifest hahses, as well as data hashes of files we want to have go in here.
    // TODO: Do not request (until timeout) if somone already requested, or if answer is already happening. Or: If anything is happening.
    // TODO: should store manifest hashes separately, as they're higher priority (might even request when there's ongoing traffic)
    // TODO: should also give up after N times for manifest hashes (unless it's seen again)
    desired_hashes: Mutex<Vec<MerklePoolIdx>>,
    file_metadatas: Mutex<HashMap<MerklePoolIdx, FileMetadata>>,
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
            merkle_pool: Default::default(),
            feedback_states: Default::default(),
            desired_hashes: Default::default(),
            file_metadatas: Default::default(),
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
            protocol::packet::Packet::Advertisement(advertisement_packet) => {
                debug!("Client: Handling Advertisement packet from {} (hash = {})",
                        addr, base16::encode_lower(advertisement_packet.hash));

                let (hidx, was_inserted) = {
                    let mut merkle_pool = self.merkle_pool.lock().await;
                    let (hidx, was_inserted) = merkle_pool.pool(advertisement_packet.hash);
                    merkle_pool.node_mut(hidx).is_manifest_root = true;
                    (hidx, was_inserted)
                };

                if was_inserted {
                    // did not see before ==> desire it
                    // (We desire any Manifest, to find out what files are offered.)
                    let mut desired_hashes = self.desired_hashes.lock().await;
                    desired_hashes.push(hidx);
                }
            },

            protocol::packet::Packet::Metadata(metadata_packet) => {
                debug!("Client: Handling Manifest packet from {} for data {} of manifest {}",
                        addr,base16::encode_lower(metadata_packet.data_hash),
                        base16::encode_lower(metadata_packet.manifest_hash));

                // Collect all new known hashes
                let mut merkle_pool = self.merkle_pool.lock().await;
                let (manifest_hidx,  manifest_hidx_new)   = merkle_pool.pool(metadata_packet.manifest_hash);
                let (data_hidx,      _data_hidx_new)      = merkle_pool.pool(metadata_packet.data_hash);
                let (extrameta_hidx, _extrameta_hidx_new) = merkle_pool.pool(metadata_packet.extra_metadata_hash);
                merkle_pool.node_mut( manifest_hidx).is_manifest_root = true;
                merkle_pool.node_mut(     data_hidx).is_data_root = true;
                merkle_pool.node_mut(extrameta_hidx).is_extrameta_root = true;
                drop(merkle_pool); // unlock

                if manifest_hidx_new {
                    // Oho, a new manifest that we don't know about.
                    let mut desired_hashes = self.desired_hashes.lock().await;
                    desired_hashes.push(manifest_hidx);
                }

                if let protocol::packet::MetadataPacketData::FileMetadata{
                            merkle_tree_depth, size_bits, size, name_length, name }
                        = metadata_packet.data {
                    // new file meta to store?
                    let mut file_metadatas = self.file_metadatas.lock().await;
                    if file_metadatas.get(&data_hidx).is_none() {
                        file_metadatas.insert(data_hidx, FileMetadata {
                            size: 0, //TODO
                            merkle_tree_height: u8::from_be(merkle_tree_depth),
                            name: Box::from(name), //TODO: name_length relevant?
                        });
                    }
                }

                //TODO: how to tell if whole manifest is received?
                // ==> compute whole merkle tree every time, by ordering by hash, and check if root hash is correct

                // TODO: tell data collector about new data
            },

            protocol::packet::Packet::RequestLeaves(request_leaves_packet) => {
                // Only server needs to handle this
                debug!("Client: Ignoring RequestLeaves packet from {} for root hash {}",
                        addr, base16::encode_lower(request_leaves_packet.root_hash));
                //TODO: don't ignore, but use for deduplication
            },

            protocol::packet::Packet::RequestLeaf(request_leaf_packet) => {
                // Only server needs to handle this
                debug!("Client: Ignoring RequestLeaf packet from {} for root hash {}",
                        addr, base16::encode_lower(request_leaf_packet.root_hash));
                //TODO: don't ignore, but use for deduplication
            },

            protocol::packet::Packet::Data(data_packet) => {
                let len = data_packet.length; // this is not be I think (kinda awkward to convert here...)
                debug!("Client: Handling Data packet from {} ({} bytes)", addr, len);

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

            protocol::packet::Packet::MerkleTree(merkle_tree_packet) => {
                debug!("Client: Handling MerkleTree packet from {} for root hash {}",
                        addr, base16::encode_lower(merkle_tree_packet.root_hash));

                let depth = usize::from(u8::from_be(merkle_tree_packet.depth));
                assert!(merkle_tree_packet.nodes.len() == (1 << depth));

                fn binary_tree_size(depth: usize) -> usize { (1 << depth) * 2 - 1 }
                fn left_child_idx(i: usize) -> usize { i * 2 + 1 }
                fn right_child_idx(i: usize) -> usize { left_child_idx(i) + 1 }

                // Find data root that matches
                // (Required, because otherwise we can't compute the merkle tree.)
                let root_hidx = {
                    let merkle_pool = self.merkle_pool.lock().await;
                    merkle_pool.try_pool(merkle_tree_packet.root_hash)
                };
                let root_hidx = if let Some(root_hidx) = root_hidx {
                    root_hidx
                } else {
                    debug!("MerkleTree packet root hash is not known: {}",
                            base16::encode_lower(merkle_tree_packet.root_hash));
                    return Ok(());
                };
                let full_tree_height = {
                    let file_metadatas = self.file_metadatas.lock().await;
                    let file_metadata = file_metadatas.get(&root_hidx);
                    if let Some(file_metadata) = file_metadata {
                        file_metadata.merkle_tree_height
                    } else {
                        debug!("Metadata for MerkleTree packet root hash is not known: {}",
                                base16::encode_lower(merkle_tree_packet.root_hash));
                        return Ok(());
                    }
                };

                if depth > full_tree_height.into() {
                    warn!("MerkleTree packet's depth is too high ({} > {})", depth, full_tree_height);
                    return Ok(());
                }

                // Build whole merkle tree

                let mut tree_hashes: Vec<Hash> = Vec::with_capacity(binary_tree_size(depth));
                if depth != 0 {
                    tree_hashes.resize(binary_tree_size(depth - 1), Default::default());
                }
                tree_hashes.extend_from_slice(merkle_tree_packet.nodes);
                assert!(tree_hashes.len() == binary_tree_size(depth));

                for level in (0..depth).rev() {
                    // compute nodes on level level from nodes on level level+1
                    let idx_begin = if level == 0 { 0 } else { binary_tree_size(level - 1) };
                    let idx_end = binary_tree_size(level);
                    for i_out in idx_begin..idx_end {
                        tree_hashes[i_out] = protocol::data::combine_hashes(
                                full_tree_height - level as u8,
                                &tree_hashes[left_child_idx(i_out)],
                                &tree_hashes[right_child_idx(i_out)]);
                    }
                }

                // Check correctness
                if tree_hashes[0] != *merkle_tree_packet.root_hash {
                    warn!("Computed merkle tree of MerkleTree packet doesn't match sent root.");
                    return Ok(());
                }

                // Store merkle tree in pool
                // insert hashes
                let mut tree_hidxs: Vec<MerklePoolIdx> = Vec::with_capacity(tree_hashes.len());
                let mut merkle_pool = self.merkle_pool.lock().await;
                for hash in tree_hashes {
                    let (hidx, _) = merkle_pool.pool(&hash);
                    tree_hidxs.push(hidx);
                }
                // set child-parent relations
                for level in 0..depth {
                    let idx_begin = if level == 0 { 0 } else { binary_tree_size(level - 1) };
                    let idx_end = binary_tree_size(level);
                    for p_i in idx_begin..idx_end {
                        let p_hidx = tree_hidxs[p_i];
                        let l_hidx = tree_hidxs[left_child_idx(p_i)];
                        let r_hidx = tree_hidxs[right_child_idx(p_i)];
                        let p_node = merkle_pool.node_mut(p_hidx);
                        p_node.left_child = Some(l_hidx);
                        p_node.right_child = Some(r_hidx);
                        let l_node = merkle_pool.node_mut(l_hidx);
                        l_node.parent = Some(p_hidx);
                        let r_node = merkle_pool.node_mut(r_hidx);
                        r_node.parent = Some(p_hidx);
                    }
                }
            },

            protocol::packet::Packet::FeedbackRequest(feedback_request_packet) => {
                let req_id = u16::from_be(*feedback_request_packet.request_id);
                debug!("Client: Handling FeedbackRequest from {} (reqest_id = {})", addr, req_id);

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

                // rate-limit feedback packets per second
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
                // Only server needs to handle this. And feedback packets are not
                // deduplicated.
                debug!("Client: Ignoring feedback packet from {} for {}", addr, base16::encode_lower(feedback_packet.server_ip));
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

            // TODO: awake me on feedback request, and sleep less long
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
