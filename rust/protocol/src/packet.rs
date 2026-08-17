use crate::Hash;

#[derive(Debug)]
pub enum Packet<'a> {
    Advertisement(AdvertisementPacket<'a>),
    Metadata(MetadataPacket<'a>),
    RequestLeaves(RequestLeavesPacket<'a>),
    RequestLeaf(RequestLeafPacket<'a>),
    Data(DataPacket<'a>),
    MerkleTree(MerkleTreePacket<'a>),
    FeedbackRequest(FeedbackRequestPacket),
    Feedback(FeedbackPacket<'a>),
}
#[derive(Debug)]

pub struct AdvertisementPacket<'a> {
    pub hash: &'a Hash,
}

#[derive(Debug)]

pub struct MetadataPacket<'a> {
    pub manifest_hash: &'a Hash,
    pub data_hash: &'a Hash,
    pub extra_metadata_hash: &'a Hash,
    pub data: MetadataPacketData<'a>,
}

#[derive(Debug)]
pub enum MetadataPacketData<'a> {
    FileMetadata { merkle_tree_depth: u8, size_bits: u8, size: &'a [u8], name_length: u8, name: &'a [u8] },
    StreamMetadata { stream_id: &'a Hash },
    DirectoryMetadata { name_length: u8, name: &'a [u8] },
}

#[derive(Debug)]

pub struct RequestLeavesPacket<'a> {
    pub root_hash: &'a Hash,
}

#[derive(Debug)]

pub struct RequestLeafPacket<'a> {
    pub root_hash: &'a Hash,
    pub index: u32,
}
#[derive(Debug)]

pub struct DataPacket<'a> {
    pub root_hash: &'a Hash,
    pub leaf_hash: &'a Hash,
    pub leaf_index: u32,
    pub packet_index: u32,
    pub length: u16,
    pub data: &'a [u8],
}
#[derive(Debug)]

pub struct MerkleTreePacket<'a> {
    pub root_hash: &'a Hash,
    pub depth: u8,
    pub nodes: &'a [Hash],
}

#[derive(Debug)]

pub struct FeedbackRequestPacket {
    pub request_id: u16,
    pub timespan: u16,
    pub number_of_feedbacks: u16,
}

#[derive(Debug)]

pub struct FeedbackPacket<'a> {
    pub request_id: u16,
    pub packet_index: u16,
    pub number_of_packets_received: u16,
    pub server_ip: &'a [u8; 16],
}
