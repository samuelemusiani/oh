use std::collections::VecDeque;

use protocol::{Hash, packet::Packet};
use tokio::sync::mpsc::Sender;

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
    MetadataResponse { metadata: Vec<protocol::data::Metadata> },
    DataResponse { data: protocol::data::DataTree },
    MerkleTreeResponse { root_node: protocol::data::MerkleNode },
    EmptyResponse,
}

pub struct DataQueue {
    queue: VecDeque<ProcessedRequest>,
    packet_queue_sender: Sender<Packet<'static>>,
}

impl DataQueue {
    pub fn new(packet_queue_sender: Sender<Packet<'static>>) -> Self {
        DataQueue { queue: VecDeque::new(), packet_queue_sender }
    }

    pub fn enqueue(&mut self, request: QueueableRequest, manifest: &protocol::data::ManifestTree) {
        let mut processed_request = ProcessedRequest { request, response: Response::EmptyResponse };
        if self.queue.contains(&processed_request) {
            return;
        }

        match &processed_request.request {
            QueueableRequest::LeavesRequest { hash } => {
                if manifest.contains_hash(hash){

                }else{
                    for leaf in manifest {
                        match leaf{
                            protocol::data::Metadata::File(file_metadata) => todo!(),
                            protocol::data::Metadata::Directory => todo!(),
                            protocol::data::Metadata::Stream => todo!(),
                        }
                    }
                }
            }
            QueueableRequest::LeafRequest { hash, index } => {
                
            }
        }

        self.queue.push_back(processed_request);
    }

    pub fn dequeue(&mut self) -> Option<ProcessedRequest> {
        self.queue.pop_front()
    }
}
