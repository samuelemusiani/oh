# Optimistic HIPPO (OH)

Implementations in Go and Rust for the Optimistic HIPPO (OH) protocol, the spec for which is in `./typst/omftp.typ`.

## Running

To build the implementations directly use the flake outputs `oh-go` and `oh-rs`.

The cross product has been expanded to allow for more flexibility.
For example, topology 1 now has a syntax / parameters like this:
`topo1-s{serverloss - 0 001 005}-d{receiverDelay - 1 10 100}-r{receiverLoss - 0 001 005 p001q01}-n{numberOfReceivers - 1 2 5 10}-s{serverImplementation - go rs}-r{receiverImplementation - go rs}`.
So, as a basic example, to run the go server and go client in a minimal and ideal scenario you do `nix run .#topo1-s0-d1-r0-n1-sgo-rgo`.

To see the full list of options run `nix flake show`, not all have been tested.

## Feature support

Overall, server-go/NullFEC can successfully serve files to client-go/NullFEC and client-rs, server-go/XorFEC can successfully serve files to client-go/XorFEC; and server-rs can serve to client-rs and client-go/NullFEC.

Below is a more detailed breakdown of the implementation's supported features.
We prefered implementing different subsets / levels of inteligence to see how they interoperate.
Generally, the Go implementation is "smarter" but it supports interoperating with the Rust implementation without modification or special considerations on either side.

### Server

| Feature | Go | Rust |
| :-: | :-: | :-: |
| FEC | Null, SemiNull, XOR | Null |
| Chunking | Fixed size | Fixed size and Content Defined |
| Advertisement Packet | ✅ | ✅ |
| Metadata Packet | ✅ | ✅ (files specifically)^[1]^[2]^[3] |
| Request-Leaves Packet | ✅ | ✅ |
| Data Packet | ✅ | ✅ |
| Merkle Tree Packet | ✅ | ✅ |
| Feedback Request Packet | ✅ | ✅ |
| Feedback Packet | ✅ | ✅ |
| Congestion Control | ✅ | ✅ |
| Multiple Servers | ✅ | ✅ |
| Stream Poll Packet | ❌ | ❌ |
| Stream State Packet | ❌ | ❌ |
| Streams | ❌ | ❌ |

- [1]: Only uses `size bits = 00` as all supported files are less than 64 chunks. The protocol supports up to 320 _chunks_.
- [2]: Only sends files, not nested directories. The protocol supports encoding them but server implementations need not generate them.
- [3]: Streams are not supported, see lower in the table.

### Client

| Feature | Go | Rust |
| :-: | :-: | :-: |
| FEC | Null, SemiNull, XOR | Null |
| Chunking | Fixed size | ✅ |
| Advertisement Packet | ✅ | ✅ |
| Metadata Packet | ✅ | ✅ |
| Request-Leaves Packet | ✅ | ✅ |
| Data Packet | ✅ | ✅ |
| Merkle Tree Packet | ✅ | ✅ |
| Feedback Request Packet | ✅ | ✅ |
| Feedback Packet | ✅ | ✅ |
| Congestion Control | ✅ | ✅ |
| Multiple Servers | ✅ | ✅ |
| Loss handling | ✅ | ✅ |
| Stream Poll Packet | ❌ | ❌ |
| Stream State Packet | ❌ | ❌ |
| Streams | ❌ | ❌ |

## Protocol spec issues

The implementations conform to the the protocol specification as submitted in assignment 2.

There are however some issues that we found during implementation and would do differently in a follow-up version of the spec.
Either, because they make the implementation unnecessarily difficult or cause avoidable complexity.
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
* Do we really need size in file metadata that is > 64 bit?

## Source Map

```
.
├── flake.nix
├── go
│   ├── client
│   │   ├── cc.go                   	# handles per-server feedback - responding to feedback request packets
│   │   ├── client.go               	# send/receive loops for the client
│   │   ├── file.go                 	# turns received chunks into trees and writes files
│   │   ├── helper.go
│   │   ├── metadata.go             	# follows up on advertisements and processes the resulting metadata
│   │   ├── packet.go
│   │   └── scheduler.go            	# handles server communication, retries and scheduling
│   ├── cmd
│   │   └── oh.go                   	# cli
│   ├── default.nix                 	# builds the go cli
│   ├── go.mod
│   ├── go.sum
│   ├── graph.py                    	# script to plot throughput
│   ├── internal
│   │   ├── chunker
│   │   │   └── chunker.go          	# implements fixed size chunking
│   │   ├── fec.go                  	# implements null, semi null, and xor FECs
│   │   ├── merkle_tree
│   │   │   ├── merkle_tree.go      	# turns chunks into a merkle tree
│   │   │   └── merkle_tree_test.go
│   │   ├── multicast.go            	# abstracts over multicast specifics
│   │   └── packet                  	# encode and decode all packets to the wire
│   │       ├── advertisement.go
│   │       ├── data.go
│   │       ├── feedback.go
│   │       ├── merkle_tree.go
│   │       ├── metadata.go
│   │       ├── packet.go
│   │       └── request.go
│   ├── mftp.go
│   ├── runner.py                   	# script for testing
│   └── server
│       ├── cc.go                   	# aggregates feedback and selects data rate
│       ├── file.go
│       ├── helper.go
│       ├── packet.go
│       ├── scheduler.go            	# sends and schedules packets to clients, priority and packet size handling
│       └── server.go               	# main loops for the server
├── nix
│   ├── config.nix                  	# constants
│   ├── null-fec.patch              	# lets nix build the go implementation with and without FEC
│   ├── seeded-random.nix           	# extended from template for random files
│   ├── topologies.nix              	# extended from template with larger cross product
│   └── util.nix                    	# small helper functions
├── rust
│   ├── app
│   │   ├── Cargo.toml
│   │   └── src
│   │       ├── client
│   │       │   ├── client.rs
│   │       │   ├── merkle_pool.rs  	# pool of merkle nodes whilst building the tree
│   │       │   └── mod.rs
│   │       ├── error.rs
│   │       ├── main.rs             	# CLI
│   │       └── server
│   │           ├── cc.rs           	# handle feedback packets and adjust sending rate
│   │           ├── data_queue.rs   	# packet queue that handles asynchrounous sending
│   │           ├── mod.rs
│   │           └── server.rs
│   ├── Cargo.toml
│   ├── default.nix                 	# builds the rust crates
│   └── protocol
│       ├── Cargo.toml
│       └── src
│           ├── data.rs             	# build a merkle tree and create data chunks
│           ├── error.rs
│           ├── lib.rs              	# exposes packet encode/decode and creating manifests
│           ├── packet_owned.rs
│           ├── packet.rs           	# internal packet definitions
│           └── zc.rs               	# packet layout definitions
└── typst
    ├── omftp.pdf                   	# spec as submitted in assignment 2
    └── omftp.typ                   	# source for the spec - build with `typst compile omftp.typ`
```

