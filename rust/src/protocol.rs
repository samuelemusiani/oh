use std::net::IpAddr;
use std::time::Duration;

pub type ProtocolHash = [u8; 32];
pub type BlobHash = ProtocolHash;
pub type ManifestHash = ProtocolHash;
pub type StreamId = [u8; 16];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HashAlgorithm {
    Sha256,
    Blake3,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkingStrategy {
    Fixed {
        chunk_size: usize,
    },
    ContentDefined {
        min_chunk_size: usize,
        target_chunk_size: usize,
        max_chunk_size: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FecConfig {
    pub data_shards: u16,
    pub parity_shards: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolParameters {
    pub hash_algorithm: HashAlgorithm,
    pub chunking: ChunkingStrategy,
    pub fec: FecConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MerkleTreeKind {
    Manifest,
    Metadata,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestKind {
    Directory,
    FileBatch,
    Stream,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub data_tree_root: ProtocolHash,
    pub metadata_tree_root: ProtocolHash,
    pub size: u64,
    pub kind: ManifestKind,
    pub short_type_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataLeaf {
    pub kind: String,
    pub blob: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataLeaf {
    pub blob_hash: BlobHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    pub hash: BlobHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRequest {
    pub hash: BlobHash,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobResponse {
    pub hash: BlobHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeVerificationRequest {
    pub hash: BlobHash,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeVerificationNode {
    pub hash: ProtocolHash,
    pub level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailabilityQuery {
    pub hash: ProtocolHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAdvertisement {
    pub manifest_hash: ManifestHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEndpoint {
    pub address: IpAddr,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAvailability {
    pub manifest_hash: ManifestHash,
    pub endpoints: Vec<ServerEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CongestionFeedback {
    pub server: ServerEndpoint,
    pub interval: Duration,
    pub bytes_received: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMessage {
    pub stream_id: StreamId,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamState {
    pub stream_id: StreamId,
    pub next_sequence: u64,
}
