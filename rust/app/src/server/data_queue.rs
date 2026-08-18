use std::collections::VecDeque;

use log::{debug, error, info, trace};
use protocol::{Hash, data::MerkleHash, packet_owned::PacketOwned};
use tokio::sync::{Mutex, Notify, RwLock, mpsc::Sender};

use crate::error::AppError;

#[derive(Debug, Clone)]
pub enum QueueableRequest {
    LeavesRequest { hash: Hash },
    LeafRequest { hash: Hash, index: u32 },
}

impl TryFrom<&protocol::packet::Packet<'_>> for QueueableRequest {
    type Error = &'static str;

    fn try_from(packet: &protocol::packet::Packet<'_>) -> Result<Self, Self::Error> {
        match packet {
            protocol::packet::Packet::RequestLeaves(request_leaves_packet) => {
                Ok(QueueableRequest::LeavesRequest { hash: *request_leaves_packet.root_hash })
            }
            protocol::packet::Packet::RequestLeaf(request_leaf_packet) => {
                Ok(QueueableRequest::LeafRequest { hash: *request_leaf_packet.root_hash, index: request_leaf_packet.index })
            }
            _ => Err("Invalid packet type for QueueableRequest"),
        }
    }
}

impl PartialEq for QueueableRequest {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (QueueableRequest::LeavesRequest { hash: hash1 }, QueueableRequest::LeavesRequest { hash: hash2 }) => hash1 == hash2,
            (QueueableRequest::LeafRequest { hash: hash1, index: index1 }, QueueableRequest::LeafRequest { hash: hash2, index: index2 }) => {
                hash1 == hash2 && index1 == index2
            }
            _ => false,
        }
    }
}

pub struct ProcessedRequest {
    pub request: QueueableRequest,
    pub response: Response,
}

impl PartialEq for ProcessedRequest {
    fn eq(&self, other: &Self) -> bool {
        &self.request == &other.request
    }
}

pub enum Response {
    MetadataResponse {
        metadata: Vec<&'static protocol::data::Metadata>,
        manifest_hash: Hash,
    },
    DataResponse {
        data: &'static protocol::data::DataTree,
        data_leaves: Vec<&'static protocol::data::DataLeaf>,
        root_hash: Hash,
        leaf_index: Option<u32>,
    },
    MerkleTreeResponse {
        root_node: &'static protocol::data::MerkleNode,
        depth: u8,
        nodes: Vec<&'static Box<protocol::data::MerkleNode>>,
    },
    EmptyResponse,
}

pub struct DataQueue {
    queue: Mutex<VecDeque<ProcessedRequest>>,
    active_request: RwLock<Option<QueueableRequest>>,
    packet_queue_sender: Sender<PacketOwned>,
    new_item_event: Notify,
}

impl DataQueue {
    pub fn new(packet_queue_sender: Sender<PacketOwned>) -> Self {
        DataQueue { queue: Mutex::new(VecDeque::new()), active_request: RwLock::new(None), packet_queue_sender, new_item_event: Notify::new() }
    }

    pub async fn enqueue(&self, request: QueueableRequest, manifest: &'static protocol::data::ManifestTree) {
        let mut processed_request = ProcessedRequest { request, response: Response::EmptyResponse };
        // Super basic deduplication for requests, this could be improved
        let queue = self.queue.lock().await;
        if queue.contains(&processed_request) {
            return;
        }
        drop(queue);
        let active_request = self.active_request.read().await;
        if let Some(active_request) = &*active_request {
            if active_request == &processed_request.request {
                return;
            }
        }

        match &processed_request.request {
            QueueableRequest::LeavesRequest { hash } => {
                if let Some(sub_tree) = manifest.sub_tree(hash) {
                    let depth = sub_tree.height();
                    if depth > 32 {
                        // We can't send a merkle packet with a depth greater than 255, if the depth - 32 is still greater than 255 the client will get another merkle packet for each hash we send him now
                        let target_depth = depth - 32;
                        let nodes = sub_tree.get_nodes_at_depth(target_depth);
                        processed_request.response = Response::MerkleTreeResponse { root_node: sub_tree, depth: target_depth as u8, nodes };
                    } else {
                        let requested_leaf_hashes = sub_tree.get_leaf_hashes();
                        let requested_metadata: Vec<&protocol::data::Metadata> =
                            manifest.into_iter().filter(|metadata| requested_leaf_hashes.contains(&&metadata.merkle_hash())).collect();
                        processed_request.response =
                            Response::MetadataResponse { metadata: requested_metadata, manifest_hash: *manifest.root_hash() };
                    }
                } else {
                    for leaf in manifest {
                        match leaf {
                            protocol::data::Metadata::File(file_metadata) => {
                                // The hash is not part of this file's data tree,
                                // it may still be part of another file's.
                                let Some(sub_tree) = file_metadata.data_tree.sub_tree(hash) else {
                                    continue;
                                };
                                let depth = sub_tree.height();
                                if depth > 32 {
                                    // We can't send a merkle packet with a depth greater than 255, if the depth - 32 is still greater than 255 the client will get another merkle packet for each hash we send him now
                                    let target_depth = depth - 32;
                                    let nodes = sub_tree.get_nodes_at_depth(target_depth);
                                    processed_request.response =
                                        Response::MerkleTreeResponse { root_node: sub_tree, depth: target_depth as u8, nodes };
                                } else {
                                    let requested_leaf_hashes = sub_tree.get_leaf_hashes();
                                    let requested_data_leaves: Vec<&protocol::data::DataLeaf> = file_metadata
                                        .data_tree
                                        .into_iter()
                                        .filter(|data_leaf| requested_leaf_hashes.contains(&&data_leaf.merkle_hash()))
                                        .collect();
                                    processed_request.response = Response::DataResponse {
                                        data: &file_metadata.data_tree,
                                        data_leaves: requested_data_leaves,
                                        root_hash: *hash,
                                        leaf_index: None,
                                    };
                                }
                                break;
                            }
                            protocol::data::Metadata::Directory => todo!(),
                            protocol::data::Metadata::Stream => todo!(),
                        }
                    }
                }
            }
            QueueableRequest::LeafRequest { hash, index } => {
                if let Some(sub_tree) = manifest.sub_tree(hash) {
                    if sub_tree.height() > 32 {
                        //The requested leaf must not be part of a subtree with a height greater than 32
                        error!(
                            "Requested leaf with hash {} is part of a subtree with height greater than 32, ignoring request",
                            base16::encode_lower(hash)
                        );
                        return;
                    }
                    if let Some(leaf_node) = sub_tree.get_leaves().get(*index as usize) {
                        let leaf = manifest.get_leaf_from_node(leaf_node);
                        if let Some(leaf) = leaf {
                            processed_request.response = Response::MetadataResponse { metadata: vec![leaf], manifest_hash: *manifest.root_hash() }
                        } else {
                            // Client might have requested a padding leaf, which is not part of the data tree, so we ignore it
                            error!(
                                "Requested leaf with hash {} and index {} is not a valid leaf, ignoring request",
                                base16::encode_lower(hash),
                                index
                            );
                            return;
                        }
                    } else {
                        // The leaf is not in the data tree, so we ignore it
                        return;
                    }
                } else {
                    for leaf in manifest {
                        match leaf {
                            protocol::data::Metadata::File(file_metadata) => {
                                // The hash is not part of this file's data tree,
                                // it may still be part of another file's.
                                let Some(sub_tree) = file_metadata.data_tree.sub_tree(hash) else {
                                    continue;
                                };
                                if sub_tree.height() > 32 {
                                    //The requested leaf must not be part of a subtree with a height greater than 32
                                    error!(
                                        "Requested leaf with hash {} is part of a subtree with height greater than 32, ignoring request",
                                        base16::encode_lower(hash)
                                    );
                                    return;
                                }
                                if let Some(leaf_node) = sub_tree.get_leaves().get(*index as usize) {
                                    let leaf = file_metadata.data_tree.get_leaf_from_node(leaf_node);
                                    if let Some(leaf) = leaf {
                                        processed_request.response = Response::DataResponse {
                                            data: &file_metadata.data_tree,
                                            data_leaves: vec![leaf],
                                            root_hash: *hash,
                                            leaf_index: Some(*index),
                                        };
                                    } else {
                                        // Client might have requested a padding leaf, which is not part of the data tree, so we ignore it
                                        error!(
                                            "Requested leaf with hash {} and index {} is not a valid leaf, ignoring request",
                                            base16::encode_lower(hash),
                                            index
                                        );
                                    }
                                }
                                // The leaf index may be out of bounds, which is
                                // not an error: a client that lost the end of a
                                // file has no way of knowing where it ends.
                                break;
                            }
                            protocol::data::Metadata::Directory => todo!(),
                            protocol::data::Metadata::Stream => todo!(),
                        }
                    }
                }
            }
        }

        if matches!(processed_request.response, Response::EmptyResponse) {
            debug!("Nothing to send for request {:?}, ignoring it", processed_request.request);
            return;
        }

        self.queue.lock().await.push_back(processed_request);
        self.new_item_event.notify_one();
    }

    pub async fn worker(&self) -> Result<(), AppError> {
        loop {
            let mut queue = self.queue.lock().await;
            if let Some(request) = queue.pop_back() {
                drop(queue);
                let mut active_request = self.active_request.write().await;
                *active_request = Some(request.request);
                drop(active_request);
                match request.response {
                    Response::MetadataResponse { metadata, manifest_hash } => {
                        for meta in metadata {
                            match meta {
                                protocol::data::Metadata::File(file_metadata) => {
                                    let metadata_packet = protocol::packet_owned::MetadataPacketOwned {
                                        manifest_hash: manifest_hash,
                                        data_hash: *file_metadata.data_tree.root_hash(),
                                        extra_metadata_hash: [0; 32],
                                        data: protocol::packet_owned::MetadataPacketDataOwned::FileMetadata {
                                            merkle_tree_depth: file_metadata.data_tree.depth(),
                                            size_bits: 0,
                                            size: file_metadata.data_tree.extra_data.len().to_be_bytes().to_vec(),
                                            name_length: file_metadata.name.len() as u8,
                                            name: file_metadata.name.as_bytes().to_vec(),
                                        },
                                    };
                                    self.packet_queue_sender.send(PacketOwned::Metadata(metadata_packet)).await?;
                                }
                                protocol::data::Metadata::Directory => todo!(),
                                protocol::data::Metadata::Stream => todo!(),
                            }
                        }
                    }
                    Response::DataResponse { data, data_leaves, root_hash, leaf_index } => {
                        trace!("Responding with data response for root_hash: {}", base16::encode_lower(&root_hash));
                        for (idx, leaf) in data_leaves.iter().enumerate() {
                            info!(
                                "Peparing data packets for leaf: {}, with leaf-idx: {}, offset: {}, length: {}",
                                base16::encode_lower(&leaf.hash),
                                leaf_index.unwrap_or_else(|| idx as u32),
                                leaf.offset,
                                leaf.length
                            );
                            for (packet_index, chunk) in
                                // Just a rough constant number for chunks, refine later...
                                data.extra_data[leaf.offset as usize..(leaf.offset + leaf.length) as usize].chunks(1400).enumerate()
                            {
                                debug!(
                                    "Prepared data packet for leaf: {}, with leaf-idx: {}, packet-idx: {}, chunk-length: {}",
                                    base16::encode_lower(&leaf.hash),
                                    leaf_index.unwrap_or_else(|| idx as u32),
                                    packet_index,
                                    chunk.len()
                                );
                                let data_packet = protocol::packet_owned::DataPacketOwned {
                                    root_hash,
                                    leaf_hash: leaf.hash,
                                    leaf_index: leaf_index.unwrap_or_else(|| idx as u32),
                                    packet_index: packet_index as u32,
                                    length: chunk.len() as u16,
                                    data: chunk,
                                };
                                self.packet_queue_sender.send(PacketOwned::Data(data_packet)).await?;
                            }
                        }
                    }
                    Response::MerkleTreeResponse { root_node, depth, nodes } => {
                        let merkle_tree_packet = protocol::packet_owned::MerkleTreePacketOwned {
                            root_hash: *root_node.hash(),
                            merkle_tree_depth: depth,
                            leaf_hashes: nodes.into_iter().map(|node| *node.hash()).collect(),
                        };
                        self.packet_queue_sender.send(PacketOwned::MerkleTree(merkle_tree_packet)).await?;
                    }
                    Response::EmptyResponse => {
                        error!("Processed request has empty response!");
                    }
                }
            } else {
                drop(queue);
                self.new_item_event.notified().await;
            }
        }
    }
}
