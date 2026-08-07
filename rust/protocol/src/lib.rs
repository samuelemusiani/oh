use tokio::fs::{File, ReadDir};
use zerocopy::TryFromBytes;

use crate::{
    error::OHProtocolError,
    packet::{
        AdvertisementPacket, DataPacket, FeedbackPacket, FeedbackRequestPacket, MerkleTreePacket,
        MetadataPacket, MetadataPacketData, Packet, RequestLeafPacket, RequestLeavesPacket,
    },
};

pub mod data;
pub mod error;
pub mod packet;
mod zc;

// Type alias for a 256-bit hash
pub type Hash = [u8; 32];

pub async fn build_manifest_from_dir(dir: &ReadDir) -> Result<data::Manifest, OHProtocolError> {
    todo!()
}

pub async fn build_manifest_from_files(files: &[File]) -> Result<data::Manifest, OHProtocolError> {
    todo!()
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
            return Ok(Packet::Advertisement(AdvertisementPacket {
                hash: &advertisement.hash,
            }));
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
                            let (file_metadata_2_, rest) =
                                zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 8)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        1 => {
                            let (file_metadata_2_, rest) =
                                zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 16)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        2 => {
                            let (file_metadata_2_, rest) =
                                zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 32)?;
                            file_metadata_2 = file_metadata_2_;
                            file_metadata_3 = zc::FileMetadata_3::try_ref_from_bytes(rest)?;
                        }
                        3 => {
                            let (file_metadata_2_, rest) =
                                zc::FileMetadata_2::try_ref_from_prefix_with_elems(rest, 40)?;
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
                        data: MetadataPacketData::DirectoryMetadata {
                            name_length: directory_metadata.name_length,
                            name: &directory_metadata.name,
                        },
                    }));
                }
                zc::BlobType::Stream => {
                    let stream_metadata = zc::StreamMetadata::try_ref_from_bytes(rest)?;
                    return Ok(Packet::Metadata(MetadataPacket {
                        manifest_hash: &metadata.manifest_hash,
                        data_hash: &metadata.data_hash,
                        extra_metadata_hash: &metadata.extra_metadata_hash,
                        data: MetadataPacketData::StreamMetadata {
                            stream_id: &stream_metadata.stream_id,
                        },
                    }));
                }
            }
        }
        zc::PacketType::RequestLeaves => {
            let request_leaves = zc::RequestLeavesPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::RequestLeaves(RequestLeavesPacket {
                root_hash: &request_leaves.root_hash,
            }));
        }
        zc::PacketType::RequestLeaf => {
            let request_leaf = zc::RequestLeafPacket::try_ref_from_bytes(data)?;
            return Ok(Packet::RequestLeaf(RequestLeafPacket {
                root_hash: &request_leaf.root_hash,
                index: request_leaf.index,
            }));
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

#[cfg(test)]
mod tests {
    use super::*;
}
