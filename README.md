# Optimistic HIPPO (OH)

Implementations in Go and Rust for the Optimistic HIPPO (OH) protocol, the spec for which is in `./typst/omftp.typ`.

## Feature support

### Server

- [ ] Rust Vs Go feature matrix

### Client

- [ ] Rust Vs Go feature matrix

## Protocol spec issues

The implementations conform to the the protocol specification as submitted in assignment 2.

There are however some issues that we found during implementation and would do differently in a follow-up version of the spec.
For example, because they make the implementation unnecessarily difficult, or cause avoidable complexity.
These issues include:

* MerkleTree packets do not contain information of the location of the merkle tree, i.e. how deep the full merkle tree is below the sent subtree.
  - This is especially an issue because to compute the merkle tree of the subtree, one needs this information:
    Combination of two merkle tree nodes is `hash(level, left_hash, right_hash)`, where level is counted up towards the root.
  - The protocol still works by using metadata information of the subtree root (from the manifest), but this requires having this metadata available for the subtree root hash in the packet.
* Metadata is sent in its own special packets.
  It could be encapsulated in leaf packets for simplicity.
* It is difficult to know when one has received the full manifest.
  If the client requests the leaves of a manifest, the server may answer with just Metadata packets.
  These packets do not include where in the merkle tree they are.
  - If the client can guess the correct order of the metadata packets that the server has used to construct the merkle tree, it can try to construct the merkle tree of from metadata packets that it received and compare the root hash with the advertised manifest hash.
    However this is expensive and requires correctly guessing the order.
  - The spec says that the order is deterministic, but implementation-specific (see spec subsection 2.2.1).
    We decided to use the same order in both implementations: order by metadata leaf hash.
  - One could also make the server send MerkleTree packets with a depth as high as possible before sending metadata.
* Some fields are weirdly misaligned due to reserved fields.
  - This is mostly an accident / oversight due to the indirection with regards to the header in our chosen packet notation format.
* Leaves don't have an offset meaning that they can't be saved to a file easily.
  - This wasn't needed in the protocol they came from as chunks were always the same length.
  - OH allows variable length chunking, so there is no-longer a consistent index-to-offset mapping.
* Leaves don't have a length or last-packet bit so the only way to verify if you have the whole leaf is to hash it.
  - This is also an artifact of being taken from a protocol where this wasn't needed.

More points (TODO: format properly):

* could also have smaller manifests (and multiple manifests per server), and send whole manifest in one packet
* leaf has no packet data offset => difficult to write to buffer
* for data merkle trees, it would also be useful to have the merkle tree available up-front, to check if a leaf really belongs to a file.
  And to not have to re-request all data if one leaf hash is wrong.
* Leaf packets don't need root hash if we always had merkle tree available.
* (do we really need size in file metadata that is > 64 bit?)
