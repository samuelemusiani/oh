use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, network_endian::{U16, U32}};

use crate::Hash;

#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
pub(crate) enum PacketType {
    Advertisement = 0,
    Metadata,
    RequestLeaves,
    RequestLeaf,
    Data,
    MerkleTree,
    #[allow(dead_code)]
    StreamPoll,
    #[allow(dead_code)]
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

impl AdvertisementPacket {
    pub fn new(hash: Hash) -> Self {
        Self { header: PacketHeader { version: 1, packet_type: PacketType::Advertisement }, _reserved: 0, hash }
    }
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

impl MetadataPacketHead {
    pub fn new(manifest_hash: Hash, data_hash: Hash, extra_metadata_hash: Hash, blob_type: BlobType) -> Self {
        Self {
            header: PacketHeader { version: 1, packet_type: PacketType::Metadata },
            manifest_hash,
            data_hash,
            extra_metadata_hash,
            blob_type,
            _reserved: 0,
        }
    }
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FileMetadata_1 {
    pub merkle_tree_depth: u8,
    pub size_bits: u8,
}

impl FileMetadata_1 {
    pub fn new(merkle_tree_depth: u8, size_bits: u8) -> Self {
        Self { merkle_tree_depth, size_bits }
    }
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

impl RequestLeavesPacket {
    pub fn new(root_hash: Hash) -> Self {
        Self { header: PacketHeader { version: 1, packet_type: PacketType::RequestLeaves }, _reserved: 0, root_hash }
    }
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct RequestLeafPacket {
    header: PacketHeader,
    _reserved: U16,
    pub index: U32,
    pub root_hash: Hash,
}

impl RequestLeafPacket {
    pub fn new(root_hash: Hash, index: u32) -> Self {
        Self { header: PacketHeader { version: 1, packet_type: PacketType::RequestLeaf }, _reserved: U16::new(0), index: U32::new(index), root_hash }
    }
}

#[derive(Debug, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct DataPacket {
    header: PacketHeader,
    pub root_hash: Hash,
    pub leaf_hash: Hash,
    pub leaf_index: U32,
    pub packet_index: U32,
    _reserved: U16,
    pub length: U16,
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
    pub request_id: U16,
    pub timespan: U16,
    pub number_of_feedbacks: U16,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub(crate) struct FeedbackPacket {
    header: PacketHeader,
    pub request_id: U16,
    _reserved: U16,
    pub packet_index: U16,
    pub number_of_packets_received: U16,
    _reserved2: U16,
    pub server_ip: [u8; 16],
}

impl FeedbackPacket {
    pub fn new(request_id: u16, packet_index: u16, number_of_packets_received: u16, server_ip: [u8; 16]) -> Self {
        Self {
            header: PacketHeader { version: 1, packet_type: PacketType::Feedback },
            request_id: U16::new(request_id),
            _reserved: U16::new(0),
            packet_index: U16::new(packet_index),
            number_of_packets_received: U16::new(number_of_packets_received),
            _reserved2: U16::new(0),
            server_ip,
        }
    }
}