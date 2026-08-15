// use std::sync::Arc;
use std::collections::HashMap;

use protocol::Hash;

// A pool for MerkleTree nodes.
// For incomplete trees => this stores a forest (assuming nobody creates a loop).
// (The tree is cheaper to walk with MerklePoolIdx than with hashes in a hashtable.)
#[derive(Default)]
pub struct MerklePool {
	nodes: Vec<MerkleNode>,
	indices: HashMap<Hash, MerklePoolIdx>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MerklePoolIdx(pub usize);

pub struct MerkleNode {
	pub hash: Hash,
	pub parent: Option<MerklePoolIdx>,
	pub left_child: Option<MerklePoolIdx>,
	pub right_child: Option<MerklePoolIdx>,
	pub is_manifest_root: bool,
	pub is_data_root: bool,
	pub is_extrameta_root: bool,
}

impl MerklePool {
	pub fn try_pool(&self, hash: &Hash) -> Option<MerklePoolIdx> {
		return self.indices.get(hash).map(|idx| *idx);
	}

	// idx and was_inserted
	pub fn pool(&mut self, hash: &Hash) -> (MerklePoolIdx, bool) {
		if let Some(idx) = self.try_pool(hash) {
			return (idx, false);
		} else {
			let idx = self.nodes.len();
			let node = self.nodes.push_mut(MerkleNode {
				hash: *hash,
				parent: None,
				left_child: None,
				right_child: None,
				is_manifest_root: false,
				is_data_root: false,
				is_extrameta_root: false,
			});
			let idx = MerklePoolIdx(idx);
			self.indices.insert(node.hash, idx);
			return (idx, true);
		}
	}

	pub fn node(&self, idx: MerklePoolIdx) -> &MerkleNode {
		return &self.nodes[idx.0];
	}

	pub fn node_mut(&mut self, idx: MerklePoolIdx) -> &mut MerkleNode {
		return &mut self.nodes[idx.0];
	}
}
