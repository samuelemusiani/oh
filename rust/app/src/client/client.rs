use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};

use log::{debug, error, trace, warn};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::client::merkle_pool::*;
use crate::error::AppError;
use protocol::Hash;
use protocol::data::{hash_chunk_from_iter, hash_empty_chunk, build_merkle_tree, hash_encoded_metadata_packet};

// TODO: Not yet implemented:
// * requesting manifests
// * requesting file data
// * directories
// * storing to file (we could have an abstract data receiver, that we feed with
//   metadata and file data and that instructs us to fetch files)
// * handling Data packets that don't use a file root hash but a subtree root as root hash
//   (maybe should have something where each merkle tree node is either complete
//   or not, and if it becomes complete, it notifies all its parents that it's complete.
//   eventually such a parent will be file root. (and file root could notify abstract receiver))
// * re-requesting metadata and file data

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

struct FileState {
    root_hash: MerklePoolIdx,
    // Value from server. Might be incorrect.
    #[allow(dead_code)]
    size: usize,
    name: Box<[u8]>,
    merkle_tree_height: u8,
    // Size is 2**merkle_tree_height.
    // Initialized with empty leaves.
    // Must not be modified after leaf_hashes_correct was set to true.
    leaf_hashes: Box<[MerklePoolIdx]>,
    // If true, the hashes build the merkle tree with the correct root.
    // If false, we need to request all leaves again (unless we have knowledge
    // about the merkle tree).
    leaf_hashes_correct: bool,
    // Whether all leaves are complete.
    // Only used when leaf_hashes_correct is true.
    // If this and leaf_hashes_correct are both true, the file is completely recevied.
    leaves_complete: bool,
}

impl FileState {
    fn update_leaf_hashes_correct(&mut self, merkle_pool: &MerklePool) {
        if self.leaf_hashes_correct {
            return;
        }

        // build merkle tree
        let leaf_hashes: Box<[Hash]> =
            self.leaf_hashes.iter()
            .map(|hidx| merkle_pool.node(*hidx).hash)
            .collect();
        let merkle_tree = build_merkle_tree(self.merkle_tree_height, &leaf_hashes, 0);

        // check if correct
        self.leaf_hashes_correct = merkle_tree[0] == merkle_pool.node(self.root_hash).hash;

        if self.leaf_hashes_correct {
            debug!("Got correct leaf hashes for file {}", str::from_utf8(&self.name).unwrap_or("invalid utf8"));
        }
    }

    fn update_leaves_complete(&mut self, leaf_states: &HashMap<MerklePoolIdx, LeafState>) {
        if !self.leaf_hashes_correct || self.leaves_complete {
            return;
        }
        self.leaves_complete =
            self.leaf_hashes.iter().all(|leaf_hidx| if let Some(leaf_state) = leaf_states.get(&leaf_hidx) { leaf_state.is_complete } else { false });
        if self.leaves_complete {
            debug!("Leaves complete for file {}", str::from_utf8(&self.name).unwrap_or("invalid utf8"));
        }
    }
}

struct LeafState {
    leaf_hash: MerklePoolIdx,
    // The roots that were used when sending the leaf.
    // When leaf is completed, all of the files in here need to be updated.
    subtrees_to_notify: HashSet<MerklePoolIdx>,
    // The data
    packets: Vec<Option<Box<[u8]>>>,
    // If true, we have received everything => hash(conmiau(packets)) = leaf_hash
    is_complete: bool,
}

impl LeafState {
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.packets
            .iter()
            .filter_map(|p| p.as_ref()) // ignore None
            .map(|p| p.len())
            .sum()
    }

    fn try_complete(&mut self, merkle_pool: &MerklePool) {
        let no_holes = self.packets.iter().all(|p| p.is_some());
        if !no_holes {
            return;
        }

        //TODO: need to apply FEC before hashing?

        let hash = hash_chunk_from_iter(self.packets.iter().map(|p| p.as_ref().unwrap().as_ref()));

        if let Some(hidx) = merkle_pool.try_pool(&hash) {
            if hidx == self.leaf_hash {
                self.is_complete = true;
            }
        }
    }
}

struct ManifestState {
    root_hash: MerklePoolIdx,
    leaf_hashes: HashSet<MerklePoolIdx>,
    is_complete: bool,
}

impl ManifestState {
    fn update_is_complete(&mut self, merkle_pool: &MerklePool) {
        if self.is_complete {
            return;
        }

        if self.leaf_hashes.len() == 0 {
            return; //TODO: do we support empty manifests?
        }

        // build merkle tree
        let mut leaf_hashes_sorted: Box<[Hash]> =
            self.leaf_hashes.iter()
            .map(|hidx| merkle_pool.node(*hidx).hash)
            .collect();
        leaf_hashes_sorted.sort_unstable(); //TODO: sorted correctly?

        let merkle_tree_height = leaf_hashes_sorted.len().next_power_of_two().ilog2() as u8;

        let merkle_tree = build_merkle_tree(merkle_tree_height, &leaf_hashes_sorted, 0);

        // check if correct
        let root_hash = &merkle_pool.node(self.root_hash).hash;
        self.is_complete = merkle_tree[0] == *root_hash;

        if self.is_complete {
            debug!("Manifest {} is complete.", base16::encode_lower(root_hash));
        }
    }
}

// lock order of fields with locks is in delcaration order in struct
pub struct Client {
    socket: UdpSocket,
    multicast_addr: Ipv4Addr,
    multicast_port: u16,
    output_directory: PathBuf,
    // Any hashes that we know about.
    merkle_pool: Mutex<MerklePool>,
    empty_chunk_hidx: MerklePoolIdx,
    // How much data we received and how much feedback we should send, per sender ip.
    feedback_states: Mutex<HashMap<Ipv6Addr, FeedbackState>>,
    // Hashes that we want to request.
    // (We always try to request from lowest known tree level.)
    // Manifest hahses, as well as data hashes of files we want to have go in here.
    // TODO: Do not request (until timeout) if somone already requested, or if answer is already happening. Or: If anything is happening.
    // TODO: should store manifest hashes separately, as they're higher priority (might even request when there's ongoing traffic)
    // TODO: should also give up after N times for manifest hashes (unless it's seen again)
    desired_hashes: Mutex<Vec<MerklePoolIdx>>,
    manifest_states: Mutex<HashMap<MerklePoolIdx, ManifestState>>,
    file_states: Mutex<HashMap<MerklePoolIdx, FileState>>,
    leaf_states: Mutex<HashMap<MerklePoolIdx, LeafState>>,
}

impl Client {
    pub async fn new(
            multicast_addr: IpAddr,
            interface: Ipv4Addr,
            port: u16,
            output_directory: PathBuf
        ) -> Result<Self, AppError>
    {
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
        let addr = SocketAddrV4::new(multicast_addr, port);
        let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
        socket.bind(&addr.into())?;
        socket.set_multicast_loop_v4(false)?;
        socket.set_multicast_ttl_v4(16)?;
        if interface != Ipv4Addr::UNSPECIFIED {
            socket.set_multicast_if_v4(&interface)?;
        }
        let socket = UdpSocket::from_std(socket.into())?;
        debug!("Joining multicast group {} on interface {}", multicast_addr, interface);
        socket.join_multicast_v4(multicast_addr, interface)?;

        let mut merkle_pool = MerklePool::default();
        let mut leaf_states = HashMap::new();

        // insert empty chunk with its data (= emtpy) in leaf
        let (empty_chunk_hidx, _) = merkle_pool.pool(&hash_empty_chunk());
        leaf_states.insert(
            empty_chunk_hidx,
            LeafState { leaf_hash: empty_chunk_hidx, subtrees_to_notify: Default::default(), packets: vec![], is_complete: true },
        );

        Ok(Client {
            socket,
            multicast_addr,
            multicast_port: port,
            output_directory,
            merkle_pool: merkle_pool.into(),
            empty_chunk_hidx,
            feedback_states: Default::default(),
            desired_hashes: Default::default(),
            manifest_states: Default::default(),
            file_states: Default::default(),
            leaf_states: leaf_states.into(),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), AppError> {
        let client = self.clone();
        let feedback_sender_job = tokio::task::spawn(async move { client.feedback_sender_run().await });
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

    async fn handle_packet_advertisement(
            &self,
            advertisement_packet: &protocol::packet::AdvertisementPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        debug!("Client: Handling Advertisement packet from {} (hash = {})",
                addr, base16::encode_lower(advertisement_packet.hash));

        let mut merkle_pool = self.merkle_pool.lock().await;
        let (root_hidx, root_hash_was_inserted) = merkle_pool.pool(advertisement_packet.hash);
        merkle_pool.node_mut(root_hidx).is_manifest_root = true;

        if root_hash_was_inserted { // TODO: remove this stupid thing. having seen the hash before does not imply it was advertised before
            // did not see before ==> desire it
            // (We desire any Manifest, to find out what files are offered.)
            let mut desired_hashes = self.desired_hashes.lock().await;
            desired_hashes.push(root_hidx);
        }

        let mut manifest_states = self.manifest_states.lock().await;
        if manifest_states.get(&root_hidx).is_none() {
            let manifest_state = manifest_states.entry(root_hidx)
                .insert_entry(ManifestState {
                    root_hash: root_hidx,
                    leaf_hashes: Default::default(),
                    is_complete: false,
                })
                .into_mut();

            manifest_state.update_is_complete(&merkle_pool); // (can be true only if manifest is empty)
        }

        return Ok(());
    }

    async fn handle_packet_metadata(
            &self,
            metadata_packet: &protocol::packet::MetadataPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize,
            leaf_hash: Hash
        ) -> Result<(), AppError>
    {
        debug!(
            "Client: Handling Manifest packet from {} for data {} of manifest {}",
            addr,
            base16::encode_lower(metadata_packet.data_hash),
            base16::encode_lower(metadata_packet.manifest_hash)
        );

        // Collect all new known hashes
        let mut merkle_pool = self.merkle_pool.lock().await;
        let (manifest_hidx, manifest_hidx_new) = merkle_pool.pool(metadata_packet.manifest_hash);
        let (data_hidx, _data_hidx_new) = merkle_pool.pool(metadata_packet.data_hash);
        let (extrameta_hidx, _extrameta_hidx_new) = merkle_pool.pool(metadata_packet.extra_metadata_hash);
        merkle_pool.node_mut(manifest_hidx).is_manifest_root = true;
        merkle_pool.node_mut(data_hidx).is_data_root = true;
        merkle_pool.node_mut(extrameta_hidx).is_extrameta_root = true;
        // drop(merkle_pool); // unlock

        if manifest_hidx_new {
            // Oho, a new manifest that we don't know about.
            let mut desired_hashes = self.desired_hashes.lock().await;
            desired_hashes.push(manifest_hidx);
        }

        // Update completedness of the manifest
        {
            let mut manifest_states = self.manifest_states.lock().await;
            let manifest_state = manifest_states.entry(manifest_hidx)
                .or_insert(ManifestState {
                    root_hash: manifest_hidx,
                    leaf_hashes: Default::default(),
                    is_complete: false,
                });

            let (leaf_hidx, _) = merkle_pool.pool(&leaf_hash);
            let is_new_leaf_hash = manifest_state.leaf_hashes.insert(leaf_hidx);

            if is_new_leaf_hash {
                manifest_state.update_is_complete(&merkle_pool);
            }
        }

        if let protocol::packet::MetadataPacketData::FileMetadata { merkle_tree_depth, size_bits, size, name_length, name } =
            metadata_packet.data
        {
            let merkle_tree_depth = u8::from_be(merkle_tree_depth);
            // TODO: reject if merkle_tree_depth is too high
            let num_leaves = 1 << (merkle_tree_depth as usize);
            let name_length: usize = u8::from_be(name_length).into();
            let mut name = name;
            if name.len() < name_length {
                warn!("name_length fields seems to be wrong");
            } else {
                name = &name[0..name_length];
            }

            // new file meta to store?
            let mut file_states = self.file_states.lock().await;
            if file_states.get(&data_hidx).is_none() {
                let size: Option<usize> = match size_bits {
                    0b00 => {
                        if let Ok(size) = <[u8; 8]>::try_from(size) {
                            usize::try_from(u64::from_be_bytes(size)).ok()
                        } else {
                            None
                        }
                    },
                    0b01 => {
                        <[u8; 16]>::try_from(size)
                            .map_or(None, |size|
                                usize::try_from(u128::from_be_bytes(size)).ok()
                            )
                    }
                    _ => None, // not supported
                };
                let leaf_hashes = vec![self.empty_chunk_hidx; num_leaves].into_boxed_slice();
                let file_state = file_states
                    .entry(data_hidx)
                    .insert_entry(FileState {
                        root_hash: data_hidx,
                        size: size.unwrap_or(usize::MAX),
                        name: Box::from(name),
                        merkle_tree_height: merkle_tree_depth,
                        leaf_hashes,
                        leaf_hashes_correct: false,
                        leaves_complete: false,
                    })
                    .into_mut();

                // leaves might already be complete (if file is empty)
                file_state.update_leaf_hashes_correct(&merkle_pool);

                if file_state.leaf_hashes_correct {
                    let leaf_states = self.leaf_states.lock().await;
                    file_state.update_leaves_complete(&leaf_states);
                }
            }
        }

        // TODO: tell data collector about new data

        return Ok(());
    }

    async fn handle_packet_request_leaves(
            &self,
            request_leaves_packet: &protocol::packet::RequestLeavesPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        // Only server needs to handle this
        debug!("Client: Ignoring RequestLeaves packet from {} for root hash {}", addr, base16::encode_lower(request_leaves_packet.root_hash));
        //TODO: don't ignore, but use for deduplication

        return Ok(());
    }

    async fn handle_packet_request_leaf(
            &self,
            request_leaf_packet: &protocol::packet::RequestLeafPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        // Only server needs to handle this
        debug!("Client: Ignoring RequestLeaf packet from {} for root hash {}", addr, base16::encode_lower(request_leaf_packet.root_hash));
        //TODO: don't ignore, but use for deduplication

        return Ok(());
    }

    async fn handle_packet_data(
            &self,
            data_packet: &protocol::packet::DataPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        let len = data_packet.length; // this is not big endian, I think (kinda awkward to convert here, btw...)
        debug!("Client: Handling Data packet from {} ({} bytes)", addr, len);

        let leaf_st_idx = u32::from_be(data_packet.leaf_index);
        let packet_idx = u32::from_be(data_packet.packet_index);

        let server_ip6 = match addr {
            SocketAddr::V4(addr_ip4) => addr_ip4.ip().to_ipv6_mapped(),
            SocketAddr::V6(addr_ip6) => *addr_ip6.ip(),
        };

        // Update feedback state of server
        // FIXME: respect use ECN markings, see https://datatracker.ietf.org/doc/draft-ietf-tsvwg-udp-ecn/
        {
            let mut feedback_states = self.feedback_states.lock().await;
            if let Some(feedback_state) = feedback_states.get_mut(&server_ip6) {
                feedback_state.num_packets_received_in_window = u16::saturating_add(feedback_state.num_packets_received_in_window, 1);
            }
        }

        // Collect new known hashes
        let mut merkle_pool = self.merkle_pool.lock().await;
        let (st_root_hidx, _st_root_new) = merkle_pool.pool(data_packet.root_hash);
        let (leaf_hidx, _leaf_new) = merkle_pool.pool(data_packet.leaf_hash);
        // drop(merkle_pool); // unlock

        // Update the file
        let mut file_states = self.file_states.lock().await;
        let mut leaf_states = self.leaf_states.lock().await;
        //TODO: handle non-root st_root
        if let Some(file_state) = file_states.get_mut(&st_root_hidx) {
            // Get non-sub tree and idx in it (here trivial)
            let leaf_idx = leaf_st_idx as usize;
            let _root_hidx = st_root_hidx;

            // Update file's leaf hashes
            if leaf_idx >= file_state.leaf_hashes.len() {
                warn!("Got leaf index in Data packet that is out of bounds: {} >= {}", leaf_idx, file_state.leaf_hashes.len());
                return Ok(());
            }
            if !file_state.leaf_hashes_correct && file_state.leaf_hashes[leaf_idx] != leaf_hidx {
                file_state.leaf_hashes[leaf_idx] = leaf_hidx;

                // new leaf hash => might have correct hashes now
                file_state.update_leaf_hashes_correct(&merkle_pool);

                // when leaf hashes become correct, init leaves_complete
                if file_state.leaf_hashes_correct {
                    file_state.update_leaves_complete(&leaf_states);
                }
            }
        }

        let leaf_state = leaf_states.entry(leaf_hidx).or_insert(LeafState {
            leaf_hash: leaf_hidx,
            subtrees_to_notify: Default::default(),
            packets: vec![],
            is_complete: false,
        });

        // Vice versa: make root known to leaf
        let was_inserted = leaf_state.subtrees_to_notify.insert(st_root_hidx);
        if was_inserted && leaf_state.is_complete {
            // Leaf is already complete, but wasn't known to file yet
            // => could notify file
            // But nothing to do, file_state.leaves_complete is already
            // updated when the leaf hash is added to file_state.
        }

        // Store the data
        if !leaf_state.is_complete {
            if packet_idx as usize > leaf_state.packets.len() {
                leaf_state.packets.resize(packet_idx as usize + 1, None);
            }
            leaf_state.packets[packet_idx as usize] = Some(Box::from(data_packet.data));

            // do we have everything from this leaf?
            leaf_state.try_complete(&merkle_pool);
            if leaf_state.is_complete {
                // notify files about completedness
                for st_root_hidx in leaf_state.subtrees_to_notify.clone() {
                    if let Some(file_state) = file_states.get_mut(&st_root_hidx) {
                        file_state.update_leaves_complete(&leaf_states);
                    }
                }
            }
        }

        return Ok(());
    }

    async fn handle_packet_merkle_tree(
            &self,
            merkle_tree_packet: &protocol::packet::MerkleTreePacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        debug!("Client: Handling MerkleTree packet from {} for root hash {}",
                addr, base16::encode_lower(merkle_tree_packet.root_hash));

        let depth = usize::from(u8::from_be(merkle_tree_packet.depth));
        assert!(merkle_tree_packet.nodes.len() == (1 << depth));

        fn binary_tree_size(depth: usize) -> usize {
            (1 << depth) * 2 - 1
        }
        fn left_child_idx(i: usize) -> usize {
            i * 2 + 1
        }
        fn right_child_idx(i: usize) -> usize {
            left_child_idx(i) + 1
        }

        // Find data root that matches
        // (Required, because otherwise we can't compute the merkle tree.)
        let root_hidx = {
            let merkle_pool = self.merkle_pool.lock().await;
            merkle_pool.try_pool(merkle_tree_packet.root_hash)
        };
        let root_hidx = if let Some(root_hidx) = root_hidx {
            root_hidx
        } else {
            debug!("MerkleTree packet root hash is not known: {}", base16::encode_lower(merkle_tree_packet.root_hash));
            return Ok(());
        };
        let full_tree_height = {
            let file_states = self.file_states.lock().await;
            let file_state = file_states.get(&root_hidx);
            if let Some(file_state) = file_state {
                file_state.merkle_tree_height
            } else {
                debug!("Metadata for MerkleTree packet root hash is not known: {}", base16::encode_lower(merkle_tree_packet.root_hash));
                return Ok(());
            }
        };

        if depth > full_tree_height.into() {
            warn!("MerkleTree packet's depth is too high ({} > {})", depth, full_tree_height);
            return Ok(());
        }

        // Build whole merkle tree
        let tree_hashes = build_merkle_tree(depth as u8, merkle_tree_packet.nodes,
                full_tree_height - depth as u8);

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
        // (TODO: wrap tree_hashes in a struct, for simpler access)
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

        return Ok(());
    }

    async fn handle_packet_feedback_request(
            &self,
            feedback_request_packet: &protocol::packet::FeedbackRequestPacket,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        let req_id = feedback_request_packet.request_id;
        debug!("Client: Handling FeedbackRequest from {} (reqest_id = {})", addr, req_id);

        let server_ip6 = match addr {
            SocketAddr::V4(addr_ip4) => addr_ip4.ip().to_ipv6_mapped(),
            SocketAddr::V6(addr_ip6) => *addr_ip6.ip(),
        };
        let timespan_ms = feedback_request_packet.timespan;
        let timespan = std::time::Duration::from_millis(timespan_ms.into());
        let num_feedbacks_to_send = feedback_request_packet.number_of_feedbacks;

        if timespan_ms == 0 && num_feedbacks_to_send != 0 {
            warn!("FeedbackRequest has a timespan of 0 ms, but non-zero packets.");
            // (Note: timespan_ms == 0 is valid. it means feedback should stop immediately)
        }

        // rate-limit feedback packets per second
        let max_feedbacks = u16::try_from(MAX_NUM_FEEDBACK_PACKETS_PER_SEC * u32::from(timespan_ms) / 1000).unwrap_or(u16::MAX);
        if num_feedbacks_to_send > max_feedbacks {
            warn!("FeedbackRequest num feedbacks too big ({}). Range-limiting to {}.", num_feedbacks_to_send, max_feedbacks);
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
            feedback_states.insert(
                server_ip6,
                FeedbackState {
                    last_reqest_id: req_id,
                    t_request_received: std::time::Instant::now(),
                    window_duration: timespan,
                    num_feedbacks_to_send,
                    num_feedbacks_sent: 0,
                    num_packets_received_in_window: 0,
                    t_next_send: None,
                },
            );
        }

        return Ok(());
    }

    async fn handle_packet_feedback(
            &self,
            feedback_packet: &protocol::packet::FeedbackPacket<'_>,
            addr: SocketAddr,
            _packet_len: usize
        ) -> Result<(), AppError>
    {
        // Only server needs to handle this. And feedback packets are not
        // deduplicated.
        debug!("Client: Ignoring feedback packet from {} for {}", addr, base16::encode_lower(feedback_packet.server_ip));

        return Ok(());
    }

    async fn handle_packet(self: Arc<Self>, buf: Vec<u8>, addr: SocketAddr, len: usize) -> Result<(), AppError> {
        let packet = protocol::parse_packet(&buf[..len])?;
        match packet {
            protocol::packet::Packet::Advertisement(advertisement_packet) => {
                return self.handle_packet_advertisement(&advertisement_packet, addr, len).await;
            }
            protocol::packet::Packet::Metadata(metadata_packet) => {
                let leaf_hash = hash_encoded_metadata_packet(&mut buf.clone());
                return self.handle_packet_metadata(&metadata_packet, addr, len, leaf_hash).await;
            }
            protocol::packet::Packet::RequestLeaves(request_leaves_packet) => {
                return self.handle_packet_request_leaves(&request_leaves_packet, addr, len).await;
            }
            protocol::packet::Packet::RequestLeaf(request_leaf_packet) => {
                return self.handle_packet_request_leaf(&request_leaf_packet, addr, len).await;
            }
            protocol::packet::Packet::Data(data_packet) => {
                return self.handle_packet_data(&data_packet, addr, len).await;
            }
            protocol::packet::Packet::MerkleTree(merkle_tree_packet) => {
                return self.handle_packet_merkle_tree(&merkle_tree_packet, addr, len).await;
            }
            protocol::packet::Packet::FeedbackRequest(feedback_request_packet) => {
                return self.handle_packet_feedback_request(&feedback_request_packet, addr, len).await;
            }
            protocol::packet::Packet::Feedback(feedback_packet) => {
                return self.handle_packet_feedback(&feedback_packet, addr, len).await;
            }
        }
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
                    let t_next_send = feedback_state.t_request_received
                        + std::time::Duration::from_secs_f32(
                            interval * (f32::from(feedback_state.num_feedbacks_sent) + rand::random_range(0.0..1.0)),
                        );
                    feedback_state.t_next_send = Some(t_next_send);
                    t_next_send
                };

                // time for next feedback packet?
                if t_next_send >= tnow {
                    let time_since_ms = (tnow - feedback_state.t_request_received).as_millis() as u64;
                    let window_timespan_ms = feedback_state.window_duration.as_millis() as u64;
                    let packet_idx = (u16::MAX as u64) * time_since_ms / window_timespan_ms;

                    let feedback_packet = protocol::packet::Packet::Feedback(protocol::packet::FeedbackPacket {
                        request_id: feedback_state.last_reqest_id,
                        packet_index: packet_idx as u16,
                        number_of_packets_received: feedback_state.num_packets_received_in_window,
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
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}
