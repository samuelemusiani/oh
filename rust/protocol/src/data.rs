use std::collections::HashSet;

use futures_util::StreamExt;
use memmap2::Mmap;
use tokio::{fs::File, io::AsyncReadExt};
use zerocopy::IntoBytes;

use crate::{
    Hash, TryFromBase16, encode_packet,
    error::OHProtocolError,
    packet::{MetadataPacket, Packet},
};

const MAX_LEAF_DATA_SIZE: usize = u32::MAX as usize;
const TARGET_PAYLOAD_SIZE: usize = 1400;
const PACKETS_PER_CHUNK: usize = 16;

pub trait MerkleHash {
    fn merkle_hash(&self) -> Hash;
}

pub struct MerkleNode {
    hash: Hash,
    left: Option<Box<MerkleNode>>,
    right: Option<Box<MerkleNode>>,
}

impl MerkleNode {
    pub fn hash(&self) -> &Hash {
        &self.hash
    }
    pub fn left(&self) -> Option<&Box<MerkleNode>> {
        self.left.as_ref()
    }
    pub fn right(&self) -> Option<&Box<MerkleNode>> {
        self.right.as_ref()
    }
    pub fn is_leaf(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }
    pub fn get_leaf_hashes(&self) -> Vec<&Hash> {
        if self.is_leaf() {
            vec![&self.hash]
        } else {
            let mut hashes = Vec::new();
            if let Some(left) = &self.left {
                hashes.extend(left.get_leaf_hashes());
            }
            if let Some(right) = &self.right {
                hashes.extend(right.get_leaf_hashes());
            }
            hashes
        }
    }
}

impl PartialEq for MerkleNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

pub struct MerkleTree<T, E>
where
    T: MerkleHash,
{
    hash_collection: HashSet<Hash>,
    tree_node: Box<MerkleNode>,
    leaves: Vec<T>,
    pub extra_data: E,
}

impl<T: MerkleHash, E> MerkleTree<T, E> {
    pub async fn new(leaves: Vec<T>, extra_data: E) -> Result<Self, OHProtocolError> {
        let mut level = 1;
        let empty_hash = sha256::digest(0u8.as_bytes());
        let mut intermediate_hashes: Vec<Box<MerkleNode>> =
            leaves.iter().map(|leaf| Box::new(MerkleNode { hash: leaf.merkle_hash(), left: None, right: None })).collect();
        let padding_needed = Self::next_power_of_two(intermediate_hashes.len()) - intermediate_hashes.len();
        for _ in 0..padding_needed {
            let empty_node = Box::new(MerkleNode { hash: empty_hash.clone().try_from()?, left: None, right: None });
            intermediate_hashes.push(empty_node);
        }
        assert!(intermediate_hashes.len() % 2 == 0, "Merkle tree construction failed: number of leaves is not a power of two after padding");
        let mut hash_collection: HashSet<Hash> = HashSet::new();
        hash_collection.extend(intermediate_hashes.iter().map(|node| node.hash));
        while intermediate_hashes.len() > 1 {
            let mut next_level_hashes = Vec::new();
            while !intermediate_hashes.is_empty() {
                if intermediate_hashes.len() >= 2 {
                    let left_node = intermediate_hashes.remove(0);
                    let right_node = intermediate_hashes.remove(0);
                    let combined_hash = [level.as_bytes(), &left_node.hash, &right_node.hash].concat();
                    let parent_hash = sha256::digest(&combined_hash);
                    next_level_hashes.push(Box::new(MerkleNode { hash: parent_hash.try_from()?, left: Some(left_node), right: Some(right_node) }));
                } else {
                    panic!("Merkle tree construction failed: odd number of nodes at level {}", level);
                }
            }
            intermediate_hashes = next_level_hashes;
            hash_collection.extend(intermediate_hashes.iter().map(|node| node.hash));
            level += 1;
        }
        assert!(intermediate_hashes.len() == 1, "Merkle tree should have exactly one root node");
        Ok(Self { hash_collection, leaves, extra_data, tree_node: intermediate_hashes.pop().unwrap() })
    }

    pub fn contains_hash(&self, hash: &Hash) -> bool {
        self.tree_node.hash == *hash || self.hash_collection.contains(hash)
    }

    pub fn root_hash(&self) -> &Hash {
        &self.tree_node.hash
    }
    pub fn len(&self) -> usize {
        self.leaves.len()
    }
    pub fn depth(&self) -> u8 {
        let mut depth = 0;
        let mut len = self.leaves.len();
        if len % 2 != 0 {
            len += 1;
        }
        while len > 1 {
            len /= 2;
            depth += 1;
        }
        depth
    }
    pub fn sub_tree(&self, hash: &Hash) -> Option<&Box<MerkleNode>> {
        if !self.contains_hash(hash) {
            return None;
        }
        todo!()
    }
    fn next_power_of_two(n: usize) -> usize {
        let mut n = n;
        if n <= 1 {
            return 1;
        }
        n -= 1;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n + 1
    }
}

impl<'a, T: MerkleHash, E: Clone> IntoIterator for &'a MerkleTree<T, E> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.leaves.iter()
    }
}

pub type ManifestTree = MerkleTree<Metadata, ()>;
pub type DataTree = MerkleTree<DataLeaf, Mmap>;
pub enum Metadata {
    File(FileMetadata),
    Directory,
    Stream,
}

impl MerkleHash for Metadata {
    fn merkle_hash(&self) -> Hash {
        match self {
            Metadata::File(file_metadata) => file_metadata.hash,
            Metadata::Directory => todo!(),
            Metadata::Stream => todo!(),
        }
    }
}

pub struct FileMetadata {
    pub name: String,
    pub data_tree: DataTree,
    pub hash: Hash,
}

impl FileMetadata {
    pub async fn new(filename: String, mut file: File) -> Result<Self, OHProtocolError> {
        let file_size = file.metadata().await?.len();
        let file_size_bytes = file_size.to_be_bytes();
        let mut leaves = Vec::new();
        if file_size > 4096 {
            //TODO: chunking is very slow, either consider static chunking or using a faster chunking algorithm
            let mut chunker =
                fastcdc::v2020::AsyncStreamCDC::new(&mut file, 4096, PACKETS_PER_CHUNK * TARGET_PAYLOAD_SIZE, fastcdc::v2020::MAXIMUM_MAX);
            let mut stream = Box::pin(chunker.as_stream());
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.expect("Failed to read chunk");
                let chunk_hash = sha256::digest([0.as_bytes(), &chunk.data].concat());
                let leaf = DataLeaf { hash: chunk_hash.try_from()?, offset: chunk.offset, length: chunk.data.len() as u64 };
                leaves.push(leaf);
            }
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            let leaf_hash = sha256::digest([0.as_bytes(), &data].concat());
            let leaf = DataLeaf { hash: leaf_hash.try_from()?, offset: 0, length: data.len() as u64 };
            leaves.push(leaf);
        }
        let mmap = unsafe { Mmap::map(&file)? };

        let data_tree = DataTree::new(leaves, mmap).await?;
        let metadata_packet = MetadataPacket {
            manifest_hash: &[0; 32],
            data_hash: &data_tree.root_hash(),
            extra_metadata_hash: &[0; 32],
            data: crate::packet::MetadataPacketData::FileMetadata {
                merkle_tree_depth: data_tree.depth(),
                size_bits: 0, // File size is only u64 so we can hardcode this to 0
                size: file_size_bytes.as_slice(),
                name_length: filename.len() as u8,
                name: filename.as_bytes(),
            },
        };
        let encoded_packet = encode_packet(Packet::Metadata(metadata_packet))?;
        Ok(Self { name: filename, data_tree, hash: sha256::digest([0.as_bytes(), &encoded_packet].concat()).try_from()? })
    }
}

#[derive(Debug)]
pub struct DataLeaf {
    pub hash: Hash,
    // The offset of the leaf data in the original file
    offset: u64,
    // The length of the leaf data in the original file
    length: u64,
}

impl MerkleHash for DataLeaf {
    fn merkle_hash(&self) -> Hash {
        self.hash
    }
}
