use std::path::PathBuf;

use tokio::fs::{File, ReadDir};
use zerocopy::{IntoBytes, TryFromBytes};

use crate::{
    data::Metadata,
    error::OHProtocolError,
    packet::{
        AdvertisementPacket, DataPacket, FeedbackPacket, FeedbackRequestPacket, MerkleTreePacket, MetadataPacket, MetadataPacketData, Packet,
        RequestLeafPacket, RequestLeavesPacket,
    },
};

pub mod data;
pub mod error;
pub mod packet;
mod zc;

trait TryFromBase16: Sized {
    type Error;

    fn try_from(self) -> Result<Hash, Self::Error>;
}

// Type alias for a 256-bit hash
pub type Hash = [u8; 32];

impl TryFromBase16 for String {
    type Error = OHProtocolError;

    fn try_from(self) -> Result<Hash, Self::Error> {
        let bytes = base16::decode(&self)?;
        let hash: Hash = bytes.as_bytes().try_into().map_err(|_| OHProtocolError::InvalidSize { expected: 32, actual: bytes.len() })?;
        Ok(hash)
    }
}

pub async fn build_manifest_from_dir(dir: &mut ReadDir) -> Result<data::ManifestTree, OHProtocolError> {
    let mut files: Vec<PathBuf> = Vec::new();
    while let Some(entry) = dir.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            files.push(entry.path());
        } else {
            println!("Skipping non-file entry: {:?}", entry.path());
        }
    }
    build_manifest_from_files(&files).await
}

pub async fn build_manifest_from_files(files: &[PathBuf]) -> Result<data::ManifestTree, OHProtocolError> {
    let mut metadata: Vec<Metadata> = Vec::new();
    for file_path in files {
        let file_name = file_path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let file = File::open(file_path).await?;
        let file_metadata = data::FileMetadata::new(file_name, file).await?;
        metadata.push(Metadata::File(file_metadata));
    }
    let manifest_tree = data::ManifestTree::new(metadata, ()).await?;
    Ok(manifest_tree)
}

//TODO: Add explicit size checks for arrays
pub fn parse_packet(data: &[u8]) -> Result<Packet<'_>, OHProtocolError> {
    if data.is_empty() {
        return Err(OHProtocolError::EmptyPayload);
    }
    let (header, _rest) = zc::PacketHeader::try_ref_from_prefix(data)?;
    if header.version != 1 {
        return Err(OHProtocolError::InvalidVersion(header.version));
    }
    match header.packet_type {
        zc::PacketType::Advertisement => {
            let (advertisement, _rest) = zc::AdvertisementPacket::try_ref_from_prefix(data)?;
            return Ok(Packet::Advertisement(AdvertisementPacket { hash: &advertisement.hash }));
        }
        zc::PacketType::Metadata => {
            let (metadata, rest) = zc::MetadataPacketHead::try_ref_from_prefix(data)?;
            match metadata.blob_type {
                zc::BlobType::File => {
                    let (file_metadata, rest) = zc::FileMetadata_1::try_ref_from_prefix(rest)?;
                    let file_metadata_2;
                    let file_metadata_3;
                    match file_metadata.size_bits {
                        0 => {
                            let (file_metadata_2_, rest) = zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 8)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        1 => {
                            let (file_metadata_2_, rest) = zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 16)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        2 => {
                            let (file_metadata_2_, rest) = zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 32)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        3 => {
                            let (file_metadata_2_, rest) = zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 40)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        _ => {
                            return Err(OHProtocolError::Validity);
                        }
                    }
                    return Ok(Packet::Metadata(MetadataPacket {
                        manifest_hash: &metadata.manifest_hash,
                        data_hash: &metadata.data_hash,
                        extra_metadata_hash: &metadata.extra_metadata_hash,
                        data: MetadataPacketData::FileMetadata {
                            merkle_tree_depth: file_metadata.merkle_tree_depth,
                            size_bits: file_metadata.size_bits,
                            size: &file_metadata_2.size,
                            name_length: file_metadata_3.name_length,
                            name: &file_metadata_3.name,
                        },
                    }));
                }
                zc::BlobType::Directory => {
                    let directory_metadata = zc::DirectoryMetadata::try_ref_from_bytes(rest)?;
                    return Ok(Packet::Metadata(MetadataPacket {
                        manifest_hash: &metadata.manifest_hash,
                        data_hash: &metadata.data_hash,
                        extra_metadata_hash: &metadata.extra_metadata_hash,
                        data: MetadataPacketData::DirectoryMetadata { name_length: directory_metadata.name_length, name: &directory_metadata.name },
                    }));
                }
                zc::BlobType::Stream => {
                    let stream_metadata = zc::StreamMetadata::try_ref_from_bytes(rest)?;
                    return Ok(Packet::Metadata(MetadataPacket {
                        manifest_hash: &metadata.manifest_hash,
                        data_hash: &metadata.data_hash,
                        extra_metadata_hash: &metadata.extra_metadata_hash,
                        data: MetadataPacketData::StreamMetadata { stream_id: &stream_metadata.stream_id },
                    }));
                }
            }
        }
        zc::PacketType::RequestLeaves => {
            let request_leaves = zc::RequestLeavesPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::RequestLeaves(RequestLeavesPacket { root_hash: &request_leaves.root_hash }));
        }
        zc::PacketType::RequestLeaf => {
            let request_leaf = zc::RequestLeafPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::RequestLeaf(RequestLeafPacket { root_hash: &request_leaf.root_hash, index: request_leaf.index }));
        }
        zc::PacketType::Data => {
            let data_packet = zc::DataPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::Data(DataPacket {
                root_hash: &data_packet.root_hash,
                leaf_hash: &data_packet.leaf_hash,
                leaf_index: data_packet.leaf_index,
                packet_index: data_packet.packet_index,
                length: data_packet.length,
                data: &data_packet.data,
            }));
        }
        zc::PacketType::MerkleTree => {
            let merkle_tree_packet = zc::MerkleTreePacket::try_ref_from_bytes(data)?;
            return Ok(Packet::MerkleTree(MerkleTreePacket {
                root_hash: &merkle_tree_packet.root_hash,
                depth: merkle_tree_packet.depth,
                nodes: &merkle_tree_packet.nodes,
            }));
        }
        zc::PacketType::StreamPoll => todo!(),
        zc::PacketType::StreamState => todo!(),
        zc::PacketType::FeedbackRequest => {
            let feedback_request_packet = zc::FeedbackRequestPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::FeedbackRequest(FeedbackRequestPacket {
                request_id: &feedback_request_packet.request_id,
                timespan: &feedback_request_packet.timespan,
                number_of_feedbacks: &feedback_request_packet.number_of_feedbacks,
            }));
        }
        zc::PacketType::Feedback => {
            let feedback_packet = zc::FeedbackPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::Feedback(FeedbackPacket {
                request_id: &feedback_packet.request_id,
                packet_index: &feedback_packet.packet_index,
                number_of_packets_received: &feedback_packet.number_of_packets_received,
                server_ip: &feedback_packet.server_ip,
            }));
        }
    }
}

//TODO: The alignment of some of the packets is broken, so we need to build them manually without relying on zerocopy.
//TODO: Some packets have unknwon length arrays, we need to build them manually
pub fn encode_packet(packet: Packet) -> Result<Vec<u8>, OHProtocolError> {
    match packet {
        Packet::Advertisement(advertisement_packet) => {
            let zc_packet = zc::AdvertisementPacket::new(*advertisement_packet.hash);
            return Ok(zc_packet.as_bytes().to_vec());
        }
        Packet::Metadata(metadata_packet) => {
            let blob_type = match metadata_packet.data {
                MetadataPacketData::FileMetadata { .. } => zc::BlobType::File,
                MetadataPacketData::DirectoryMetadata { .. } => zc::BlobType::Directory,
                MetadataPacketData::StreamMetadata { .. } => zc::BlobType::Stream,
            };
            let zc_packet_head = zc::MetadataPacketHead::new(
                *metadata_packet.manifest_hash,
                *metadata_packet.data_hash,
                *metadata_packet.extra_metadata_hash,
                blob_type,
            );
            match metadata_packet.data {
                MetadataPacketData::FileMetadata { merkle_tree_depth, size_bits, size, name_length, name } => {
                    let zc_file_metadata_1 = zc::FileMetadata_1::new(merkle_tree_depth, size_bits);
                    // Can't build to structs because the size of the arrays is not known at compile time, so we have to build the bytes manually
                    let packet_bytes = [zc_packet_head.as_bytes(), zc_file_metadata_1.as_bytes(), size, &[name_length], name].concat();
                    return Ok(packet_bytes);
                }
                MetadataPacketData::StreamMetadata { .. } => {
                    todo!("Streams will not be implemented in this version")
                }
                MetadataPacketData::DirectoryMetadata { .. } => {
                    todo!("Sub directory metadata will not be implemented in this version")
                }
            }
        }
        Packet::RequestLeaves(request_leaves_packet) => {
            let zc_packet = zc::RequestLeavesPacket::new(*request_leaves_packet.root_hash);
            return Ok(zc_packet.as_bytes().to_vec());
        }
        Packet::RequestLeaf(request_leaf_packet) => {
            let zc_packet = zc::RequestLeafPacket::new(*request_leaf_packet.root_hash, request_leaf_packet.index);
            return Ok(zc_packet.as_bytes().to_vec());
        }
        Packet::Data(data_packet) => {
            let header = zc::PacketHeader { version: 1, packet_type: zc::PacketType::Data };
            let packet_bytes = [
                header.as_bytes(),
                data_packet.root_hash,
                data_packet.leaf_hash,
                &data_packet.leaf_index.to_be_bytes(),
                &data_packet.packet_index.to_be_bytes(),
                0u16.to_be_bytes().as_ref(),
                &data_packet.length.to_be_bytes(),
                data_packet.data,
            ]
            .concat();
            return Ok(packet_bytes);
        }
        Packet::MerkleTree(merkle_tree_packet) => {
            let header = zc::PacketHeader { version: 1, packet_type: zc::PacketType::MerkleTree };
            let packet_bytes = [
                header.as_bytes(),
                merkle_tree_packet.root_hash,
                0u8.to_be_bytes().as_ref(),
                merkle_tree_packet.depth.to_be_bytes().as_ref(),
                merkle_tree_packet.nodes.concat().as_ref(),
            ]
            .concat();
            return Ok(packet_bytes);
        }
        Packet::FeedbackRequest(feedback_request_packet) => {
            let header = zc::PacketHeader { version: 1, packet_type: zc::PacketType::FeedbackRequest };
            let packet_bytes = [
                header.as_bytes(),
                feedback_request_packet.request_id.to_be_bytes().as_ref(),
                0u8.to_be_bytes().as_ref(),
                feedback_request_packet.timespan.to_be_bytes().as_ref(),
                feedback_request_packet.number_of_feedbacks.to_be_bytes().as_ref(),
            ]
            .concat();
            return Ok(packet_bytes);
        }
        Packet::Feedback(feedback_packet) => {
            let zc_packet = zc::FeedbackPacket::new(
                *feedback_packet.request_id,
                *feedback_packet.packet_index,
                *feedback_packet.number_of_packets_received,
                *feedback_packet.server_ip,
            );
            return Ok(zc_packet.as_bytes().to_vec());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::FileMetadata;

    use super::*;

    #[tokio::test]
    async fn test_build_data_tree() {
        let file = File::open("test_data/100MB.bin").await.unwrap();
        let metadata = FileMetadata::new("100MB.bin".to_string(), file).await.unwrap();
        let data_tree = metadata.data_tree;
        println!("Root hash: {:?}", data_tree.root_hash());
        println!("Number of leaves: {}", data_tree.len());
        assert_eq!(data_tree.len(), 4619);
    }

    #[tokio::test]
    async fn test_build_manifest_tree() {
        let mut dir = tokio::fs::read_dir("test_data").await.unwrap();
        let manifest_tree = build_manifest_from_dir(&mut dir).await.unwrap();
        println!("Root hash: {:?}", manifest_tree.root_hash());
        println!("Number of files: {}", manifest_tree.len());
        for metadata in &manifest_tree {
            match metadata {
                Metadata::File(file_metadata) => {
                    println!(
                        "File: {}, Metadata Hash: {:?}, Data Hash: {:?}",
                        file_metadata.name,
                        file_metadata.hash,
                        file_metadata.data_tree.root_hash()
                    );
                }
                Metadata::Directory => {
                    println!("Directory");
                }
                Metadata::Stream => {
                    println!("Stream");
                }
            }
        }
        assert_eq!(manifest_tree.len(), 4);
    }
}
