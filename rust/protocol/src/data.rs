use futures_util::StreamExt;
use tokio::{fs::File, io::AsyncReadExt};
use zerocopy::IntoBytes;

use crate::{Hash, TryFromBase16, error::OHProtocolError};

const MAX_LEAF_DATA_SIZE: usize = u32::MAX as usize;
const TARGET_PAYLOAD_SIZE: usize = 1400;
const PACKETS_PER_CHUNK: usize = 16;

pub struct ManifestTree {
    pub root_hash: Hash,
    pub metadata: Vec<Metadata>,
}

pub enum Metadata {
    File(FileMetadata),
    Directory,
    Stream,
}

pub struct FileMetadata {
    pub name: String,
}

#[derive(Debug)]
pub struct DataTree {
    pub root_hash: Hash,
    pub leaves: Vec<DataLeaf>,
    pub file: File,
}
#[derive(Debug)]
pub struct DataLeaf {
    pub hash: Hash,
    // The offset of the leaf data in the original file
    offset: u64,
    // The length of the leaf data in the original file
    length: u64,
}

impl DataTree {
    pub async fn new(mut file: File) -> Result<Self, OHProtocolError> {
        let file_size = file.metadata().await?.len();
        let mut leaves = Vec::new();
        if file_size > 4096 {
            let mut chunker = fastcdc::v2020::AsyncStreamCDC::new(
                &mut file,
                4096,
                PACKETS_PER_CHUNK * TARGET_PAYLOAD_SIZE,
                fastcdc::v2020::MAXIMUM_MAX,
            );
            let mut stream = Box::pin(chunker.as_stream());
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.expect("Failed to read chunk");
                let chunk_hash = sha256::digest([0.as_bytes(), &chunk.data].concat());
                let leaf = DataLeaf {
                    hash: chunk_hash.try_from()?,
                    offset: chunk.offset,
                    length: chunk.data.len() as u64,
                };
                leaves.push(leaf);
            }
        } else {
            let mut data = Vec::new();
            file.read_to_end(&mut data).await?;
            let leaf_hash = sha256::digest([0.as_bytes(), &data].concat());
            let leaf = DataLeaf {
                hash: leaf_hash.try_from()?,
                offset: 0,
                length: data.len() as u64,
            };
            leaves.push(leaf);
        }
        let mut intermediate_hashes: Vec<Hash> = leaves.iter().map(|leaf| leaf.hash).collect();
        let mut level = 1;
        while intermediate_hashes.len() > 1 {
            let mut next_level_hashes = Vec::new();
            for i in (0..intermediate_hashes.len()).step_by(2) {
                if i + 1 < intermediate_hashes.len() {
                    let combined_hash = [
                        level.as_bytes(),
                        &intermediate_hashes[i],
                        &intermediate_hashes[i + 1],
                    ]
                    .concat();
                    let parent_hash = sha256::digest(&combined_hash);
                    next_level_hashes.push(parent_hash.try_from()?);
                } else {
                    let empty_hash = sha256::digest(
                        [level.as_bytes(), &intermediate_hashes[i], 0.as_bytes()].concat(),
                    );
                    let combined_hash = [
                        level.as_bytes(),
                        &intermediate_hashes[i],
                        &empty_hash.try_from()?,
                    ]
                    .concat();
                    let parent_hash = sha256::digest(&combined_hash);
                    next_level_hashes.push(parent_hash.try_from()?);
                }
            }
            intermediate_hashes = next_level_hashes;
            level += 1;
        }
        let root_hash = intermediate_hashes[0];
        Ok(Self {
            root_hash,
            leaves,
            file,
        })
    }
}
