use std::collections::HashSet;

use futures_util::StreamExt;
use memmap2::Mmap;
use sha2::{Digest, Sha256};
use tokio::{fs::File, io::AsyncReadExt};
use zerocopy::IntoBytes;

use crate::{
    Hash, encode_packet,
    error::OHProtocolError,
    packet::{MetadataPacket, Packet},
};

const MAX_LEAF_DATA_SIZE: usize = u32::MAX as usize;
const TARGET_PAYLOAD_SIZE: usize = 1400;
const PACKETS_PER_CHUNK: usize = 16;

pub fn combine_hashes(level: u8, left_hash: &Hash, right_hash: &Hash) -> Hash {
    let combined_hash = [level.as_bytes(), left_hash, right_hash].concat();
    return Sha256::digest(&combined_hash).try_into().unwrap();
}

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
    pub fn get_node_with_hash(self: &Box<MerkleNode>, hash: &Hash) -> Option<&Box<MerkleNode>> {
        if &self.hash == hash {
            return Some(self);
        }
        if let Some(left) = &self.left {
            if let Some(node) = left.get_node_with_hash(hash) {
                return Some(node);
            }
        }
        if let Some(right) = &self.right {
            if let Some(node) = right.get_node_with_hash(hash) {
                return Some(node);
            }
        }
        None
    }
    pub fn height(&self) -> u8 {
        if self.is_leaf() {
            0
        } else {
            let left_depth = self.left.as_ref().map_or(0, |left| left.height());
            let right_depth = self.right.as_ref().map_or(0, |right| right.height());
            1 + std::cmp::max(left_depth, right_depth)
        }
    }
    pub fn get_nodes_at_depth(self: &Box<MerkleNode>, depth: u8) -> Vec<&Box<MerkleNode>> {
        if depth == 0 {
            return vec![self];
        }
        let mut nodes = Vec::new();
        if let Some(left) = &self.left {
            nodes.extend(left.get_nodes_at_depth(depth - 1));
        }
        if let Some(right) = &self.right {
            nodes.extend(right.get_nodes_at_depth(depth - 1));
        }
        nodes
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
        let mut level: u8 = 1;
        let empty_hash = Sha256::digest(0u8.as_bytes());
        let mut intermediate_hashes: Vec<Box<MerkleNode>> =
            leaves.iter().map(|leaf| Box::new(MerkleNode { hash: leaf.merkle_hash(), left: None, right: None })).collect();
        let padding_needed = Self::next_power_of_two(intermediate_hashes.len()) - intermediate_hashes.len();
        for _ in 0..padding_needed {
            let empty_node = Box::new(MerkleNode { hash: empty_hash.clone().try_into().unwrap(), left: None, right: None });
            intermediate_hashes.push(empty_node);
        }
        assert!(
            intermediate_hashes.len() % 2 == 0,
            "Merkle tree construction failed: number of leaves is not a power of two after padding, was {}",
            intermediate_hashes.len()
        );
        let mut hash_collection: HashSet<Hash> = HashSet::new();
        hash_collection.extend(intermediate_hashes.iter().map(|node| node.hash));
        while intermediate_hashes.len() > 1 {
            let mut next_level_hashes = Vec::new();
            while !intermediate_hashes.is_empty() {
                if intermediate_hashes.len() >= 2 {
                    let left_node = intermediate_hashes.remove(0);
                    let right_node = intermediate_hashes.remove(0);
                    let parent_hash = combine_hashes(level, &left_node.hash, &right_node.hash);
                    next_level_hashes.push(Box::new(MerkleNode {
                        hash: parent_hash,
                        left: Some(left_node),
                        right: Some(right_node),
                    }));
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
        self.tree_node.get_node_with_hash(hash)
    }

    pub fn nodes_at_depth(&self, depth: u8) -> Vec<&Box<MerkleNode>> {
        self.tree_node.get_nodes_at_depth(depth)
    }

    fn next_power_of_two(n: usize) -> usize {
        let mut n = n;
        if n <= 1 {
            // While the next power of two of 1 is 1, we want to return 2 for our use case since we need the amount of padding to be devisible by 2
            return 2;
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

impl<'a, T: MerkleHash, E> IntoIterator for &'a MerkleTree<T, E> {
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
                let chunk_hash = Sha256::digest([0u8.as_bytes(), &chunk.data].concat());
                let leaf = DataLeaf { hash: chunk_hash.try_into().unwrap(), offset: chunk.offset, length: chunk.data.len() as u64 };
                leaves.push(leaf);
            }
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            let leaf_hash = Sha256::digest([0u8.as_bytes(), &data].concat());
            let leaf = DataLeaf { hash: leaf_hash.try_into().unwrap(), offset: 0, length: data.len() as u64 };
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
        Ok(Self { name: filename, data_tree, hash: Sha256::digest([0u8.as_bytes(), &encoded_packet].concat()).try_into().unwrap() })
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
#[cfg(test)]

mod tests {
    use sha2::{Digest, Sha256};
    use zerocopy::IntoBytes;

    use crate::Hash;
    use crate::data::MerkleHash;

    struct TestLeaf {
        hash: Hash,
    }
    impl TestLeaf {
        fn new() -> Self {
            let hash = Sha256::digest(b"test");
            TestLeaf { hash: hash.try_into().unwrap() }
        }
        fn new_with_number(num: u8) -> Self {
            let hash = Sha256::digest(&[num]);
            TestLeaf { hash: hash.try_into().unwrap() }
        }
    }

    impl MerkleHash for TestLeaf {
        fn merkle_hash(&self) -> Hash {
            self.hash
        }
    }

    #[tokio::test]
    async fn test_merkle_tree_nodes_at_depth() {
        let leaves = vec![TestLeaf::new_with_number(1), TestLeaf::new_with_number(2), TestLeaf::new_with_number(3), TestLeaf::new_with_number(4)];
        let tree = crate::data::MerkleTree::new(leaves, ()).await.unwrap();
        let depth_0_nodes = tree.nodes_at_depth(0);
        assert_eq!(depth_0_nodes.len(), 1);
        assert_eq!(depth_0_nodes[0].hash(), tree.root_hash());
        let depth_2_nodes = tree.nodes_at_depth(2);
        assert_eq!(depth_2_nodes.len(), 4);
        let expected_hashes = vec![
            TestLeaf::new_with_number(1).merkle_hash(),
            TestLeaf::new_with_number(2).merkle_hash(),
            TestLeaf::new_with_number(3).merkle_hash(),
            TestLeaf::new_with_number(4).merkle_hash(),
        ];
        for (node, expected_hash) in depth_2_nodes.iter().zip(expected_hashes.iter()) {
            assert_eq!(node.hash(), expected_hash);
        }

        let depth_3_nodes = tree.nodes_at_depth(3);
        assert_eq!(depth_3_nodes.len(), 0);
    }

    #[tokio::test]
    async fn test_merkle_tree_sub_tree() {
        let leaves = vec![TestLeaf::new_with_number(1), TestLeaf::new_with_number(2), TestLeaf::new_with_number(3), TestLeaf::new_with_number(4)];
        let tree = crate::data::MerkleTree::new(leaves, ()).await.unwrap();
        let leaf_hash = TestLeaf::new_with_number(2).merkle_hash();
        let sub_tree = tree.sub_tree(&leaf_hash);
        assert!(sub_tree.is_some());
        let sub_tree_node = sub_tree.unwrap();
        assert_eq!(sub_tree_node.hash(), &leaf_hash);
        let intermediate_hash =
            Sha256::digest([1u8.as_bytes(), &TestLeaf::new_with_number(1).merkle_hash(), &TestLeaf::new_with_number(2).merkle_hash()].concat());
        println!("Intermediate hash: {}", base16::encode_lower(&intermediate_hash));
        let intermediate_hash: Hash = intermediate_hash.try_into().unwrap();
        let sub_tree = tree.sub_tree(&intermediate_hash);
        assert!(sub_tree.is_some());
        let sub_tree_node = sub_tree.unwrap();
        assert_eq!(sub_tree_node.hash(), &intermediate_hash);
        assert!(sub_tree_node.left().is_some());
        assert!(sub_tree_node.right().is_some());
        let non_existent_hash = Sha256::digest(b"nonexistent");
        let non_existent_hash: Hash = non_existent_hash.try_into().unwrap();
        let sub_tree = tree.sub_tree(&non_existent_hash);
        assert!(sub_tree.is_none());
    }

    #[tokio::test]
    async fn test_merkle_node_get_nodes_at_depth() {
        let leaves = vec![TestLeaf::new_with_number(1), TestLeaf::new_with_number(2), TestLeaf::new_with_number(3), TestLeaf::new_with_number(4)];
        let tree = crate::data::MerkleTree::new(leaves, ()).await.unwrap();
        let root_node = &tree.tree_node;
        let depth_0_nodes = root_node.get_nodes_at_depth(0);
        assert_eq!(depth_0_nodes.len(), 1);
        assert_eq!(depth_0_nodes[0].hash(), tree.root_hash());
        let depth_2_nodes = root_node.get_nodes_at_depth(2);
        assert_eq!(depth_2_nodes.len(), 4);
        let expected_hashes = vec![
            TestLeaf::new_with_number(1).merkle_hash(),
            TestLeaf::new_with_number(2).merkle_hash(),
            TestLeaf::new_with_number(3).merkle_hash(),
            TestLeaf::new_with_number(4).merkle_hash(),
        ];
        for (node, expected_hash) in depth_2_nodes.iter().zip(expected_hashes.iter()) {
            assert_eq!(node.hash(), expected_hash);
        }
    }
}
