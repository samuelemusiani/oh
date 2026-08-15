use std::collections::VecDeque;

use protocol::{Hash, data::MerkleHash, packet::Packet};
use tokio::sync::{Mutex, mpsc::Sender};

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
    MetadataResponse { metadata: Vec<&'static protocol::data::Metadata> },
    DataResponse { data: &'static protocol::data::DataTree, data_leaves: Vec<&'static protocol::data::DataLeaf> },
    MerkleTreeResponse { root_node: &'static protocol::data::MerkleNode, depth: u8, nodes: Vec<&'static Box<protocol::data::MerkleNode>> },
    EmptyResponse,
}

pub struct DataQueue {
    queue: Mutex<VecDeque<ProcessedRequest>>,
    packet_queue_sender: Sender<Packet<'static>>,
}

impl DataQueue {
    pub fn new(packet_queue_sender: Sender<Packet<'static>>) -> Self {
        DataQueue { queue: Mutex::new(VecDeque::new()), packet_queue_sender }
    }

    pub async fn enqueue(&self, request: QueueableRequest, manifest: &'static protocol::data::ManifestTree) {
        let mut processed_request = ProcessedRequest { request, response: Response::EmptyResponse };
        // Super basic deduplication for requests, this could be improved
        let queue = self.queue.lock().await;
        if queue.contains(&processed_request) {
            return;
        }
        drop(queue);

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
                        processed_request.response = Response::MetadataResponse { metadata: requested_metadata };
                    }
                } else {
                    for leaf in manifest {
                        match leaf {
                            protocol::data::Metadata::File(file_metadata) => {
                                if let Some(sub_tree) = file_metadata.data_tree.sub_tree(hash) {
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
                                        processed_request.response =
                                            Response::DataResponse { data: &file_metadata.data_tree, data_leaves: requested_data_leaves };
                                    }
                                } else {
                                    // The leaf neither in the manifest nor in any of the data trees, so we ignore it
                                    return;
                                }
                            }
                            protocol::data::Metadata::Directory => todo!(),
                            protocol::data::Metadata::Stream => todo!(),
                        }
                    }
                }
            }
            QueueableRequest::LeafRequest { hash, index } => {}
        }

        self.queue.lock().await.push_back(processed_request);
    }

    pub async fn dequeue(&mut self) -> Option<ProcessedRequest> {
        self.queue.lock().await.pop_front()
    }
}
