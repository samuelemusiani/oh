# Optimistic HIPPO (OH)

Implementation repo for Optimistic HIPPO (OH)


## Protocol spec issues

The implementations conform to the the protocol specification as submitted in assignment 2.

There are however some issues that we found and do differently in a follow-up version
of the spec, for example because they make the implementation unnecessarily difficult,
or cause avoidable complexity.
These issues include:

* MerkleTree packets do not contain information of the location of the merkle tree, i.e.
	how deep the full merkle tree is below the sent subtree.
	This is especially an issue because to compute the merkle tree of the subtree,
	one needs this information: Combination of two merkle tree nodes is `hash(level, left_hash, right_hash)`,
	where level is counted up towards the root.
	The protocol still works by using metadata information of the subtree root (from
	the manifest), but this requires having this metadata available for the subtree
	root hash in the packet.
* Metadata is sent in its special packets. It could be encapsulated in leaf packets,
	for simplicity.
* It is difficult to know when one has received the full manifest.
	If the client requests the leaves of a manifest, the server may answer with just
	Metadata packets. These packets do not include where in the merkle tree they
	are.
	If the client can guess the correct order of the metadata packets that the server
	has used to construct the merkle tree, it can try to construct the merkle tree
	of from metadata packets that it received and compare the root hash with the
	advertised manifest hash.
	However this is expensive and requires correctly guessing the order. The spec
	says that the order is deterministic, but implementation-specific (see spec
	subsection 2.2.1). We decided to use the same order in both implementations:
	Order by metadata leaf hash.
	One could also make the server send MerkleTree packets with a depth as high as
	possible before sending metadata.
* Some fields are weirdly misaligned due to reserved fields.
