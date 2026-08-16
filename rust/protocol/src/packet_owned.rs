use crate::Hash;

// Owned versions of the packet types, which own their data instead of borrowing it
#[derive(Debug)]
pub enum PacketOwned {
    Advertisement(AdvertisementPacketOwned),
    Metadata(MetadataPacketOwned),
    RequestLeaves(RequestLeavesPacketOwned),
    RequestLeaf(RequestLeafPacketOwned),
    Data(DataPacketOwned),
    MerkleTree(MerkleTreePacketOwned),
    FeedbackRequest(FeedbackRequestPacketOwned),
    Feedback(FeedbackPacketOwned),
}

#[derive(Debug)]
pub struct AdvertisementPacketOwned {
    pub hash: Hash,
}

#[derive(Debug)]
pub struct MetadataPacketOwned {
    pub manifest_hash: Hash,
    pub data_hash: Hash,
    pub extra_metadata_hash: Hash,
    pub data: MetadataPacketDataOwned,
}

#[derive(Debug)]
pub enum MetadataPacketDataOwned {
    FileMetadata { merkle_tree_depth: u8, size_bits: u8, size: Vec<u8>, name_length: u8, name: Vec<u8> },
    StreamMetadata { stream_id: Hash },
    DirectoryMetadata { name_length: u8, name: Vec<u8> },
}

#[derive(Debug)]
pub struct RequestLeavesPacketOwned {
    pub root_hash: Hash,
}

#[derive(Debug)]
pub struct RequestLeafPacketOwned {
    pub root_hash: Hash,
    pub index: u32,
}

#[derive(Debug)]
pub struct DataPacketOwned {
    pub root_hash: Hash,
    pub leaf_hash: Hash,
    pub leaf_index: u32,
    pub packet_index: u32,
    pub length: u16,
    // The data of the leaf, which is a slice of the original file, not really interested in copying into our own memory which is why this is a ref
    pub data: &'static [u8],
}

#[derive(Debug)]
pub struct MerkleTreePacketOwned {
    pub root_hash: Hash,
    pub merkle_tree_depth: u8,
    pub leaf_hashes: Vec<Hash>,
}

#[derive(Debug)]
pub struct FeedbackRequestPacketOwned {
    pub request_id: u16,
    pub timespan: u16,
    pub number_of_packets: u16,
}

#[derive(Debug)]
pub struct FeedbackPacketOwned {
    pub request_id: u16,
    pub number_of_packets_received: u16,
    pub packet_index: u16,
    pub server_ip: [u8; 16],
}
