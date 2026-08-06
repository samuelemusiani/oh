use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

pub(crate) type Hash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
pub(crate) enum PacketType {
    Advertisement = 0,
    Metadata,
    RequestLeaves,
    RequestLeaf,
    Data,
    MerkleTree,
    StreamPoll,
    StreamState,
    FeedbackRequest,
    Feedback = 9,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct PacketHeader {
    pub version: u8,
    pub packet_type: PacketType,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct AdvertisementPacket {
    header: PacketHeader,
    _reserved: u8,
    pub hash: Hash,
}
#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
pub(crate) enum BlobType {
    File = 0,
    Directory = 1,
    Stream = 2,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct MetadataPacketHead {
    header: PacketHeader,
    pub manifest_hash: Hash,
    pub data_hash: Hash,
    pub extra_metadata_hash: Hash,
    pub blob_type: BlobType,
    _reserved: u8,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FileMetadata_1 {
    pub merkle_tree_depth: u8,
    pub size_bits: u8,
}
#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FileMetadata_2 {
    pub size: [u8],
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FileMetadata_3 {
    pub name_length: u8,
    pub name: [u8],
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct StreamMetadata {
    pub stream_id: [u8; 32],
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct DirectoryMetadata {
    pub name_length: u8,
    pub name: [u8],
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct RequestLeavesPacket {
    header: PacketHeader,
    _reserved: u8,
    pub root_hash: Hash,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct RequestLeafPacket {
    header: PacketHeader,
    _reserved: u16,
    pub index: u32,
    pub root_hash: Hash,
}

#[derive(Debug, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct DataPacket {
    header: PacketHeader,
    pub root_hash: Hash,
    pub leaf_hash: Hash,
    pub leaf_index: u32,
    pub packet_index: u32,
    _reserved: u16,
    pub length: u16,
    pub data: [u8],
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct MerkleTreePacket {
    header: PacketHeader,
    pub root_hash: Hash,
    _reserved: u8,
    pub depth: u8,
    pub nodes: [Hash],
}

#[derive(Debug, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FeedbackRequestPacket {
    header: PacketHeader,
    _reserved: u8,
    pub request_id: u16,
    pub timespan: u16,
    pub number_of_feedbacks: u16,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FeedbackPacket {
    header: PacketHeader,
    pub request_id: u16,
    _reserved: u16,
    pub packet_index: u16,
    pub number_of_packets_received: u16,
    _reserved2: u16,
    pub server_ip: [u8; 16],
}
