use crate::Hash;

pub struct Manifest {
    pub root_hash: Hash,
    pub metadata: Vec<Metadata>,
}

pub enum Metadata {
    File,
    Directory,
    Stream,
}
