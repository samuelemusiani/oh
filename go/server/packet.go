package server

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (f *file) MetadataPacket(manifestHash [32]byte) *packet.Metadata {
	size := make([]uint8, 8)
	binary.NativeEndian.PutUint64(size, f.size)
	return &packet.Metadata{
		ManifestHash:      manifestHash,
		DataHash:          [32]byte(f.mt.Root()),
		ExtraMetadataHash: [32]byte{},
		Data: &packet.FileMetadata{
			MerkleTreeDepth: f.mt.Depth(),
			Size:            size,
			Name:            f.name,
		},
	}
}

func (s *Server) AdvertisementPacket() (*packet.Advertisement, error) {
	// TODO: this prevents file updates, but we don't support them so...
	if s.currentManifest != nil {
		return &packet.Advertisement{
			Hash: [32]byte(s.currentManifest.Root()),
		}, nil
	}

	leafs := merkletree.NextPowerOfTwo(uint32(len(s.files)))
	h := hashFunc()

	leafHash := func(leaf uint32) ([]byte, error) {
		h.Reset()
		h.Write([]byte{0})
		if leaf >= uint32(len(s.files)) {
			return h.Sum(nil), nil
		}

		data, err := s.files[leaf].MetadataPacket([32]byte{}).Encode()
		if err != nil {
			return nil, fmt.Errorf("error encoding metadata packet for file %d: %w", leaf, err)
		}
		h.Write(data)
		return h.Sum(nil), nil
	}

	mt, err := merkletree.NewMerkleTree(leafs, leafHash, hashFunc())
	if err != nil {
		return nil, fmt.Errorf("error creating Merkle tree: %w", err)
	}

	s.currentManifest = mt

	for _, n := range mt.GetAllNodes() {
		s.hashLookup[[32]byte(n.Hash())] = mt
	}

	return &packet.Advertisement{
		Hash: [32]byte(mt.Root()),
	}, nil
}

func (f *file) ChunkPacket(index uint32) ([]packet.Data, error) {
	maxDataSize := packet.MaxDataPacketPayload
	if index >= uint32(len(f.chunks)) {
		return nil, fmt.Errorf("invalid chunk index %d for file %s", index, f.name)
	}

	data, err := f.chunks[index].Data(f.mmapFile)
	if err != nil {
		return nil, fmt.Errorf("error getting data for chunk %d of file %s: %w", index, f.name, err)
	}

	pkts := make([]packet.Data, 0)

	for i := 0; i < len(data); i += maxDataSize {
		pkts = append(pkts, packet.Data{
			RootHash:    [32]byte(f.mt.Root()),
			LeafHash:    [32]byte(f.mt.LeafHash(index)),
			LeafIndex:   index,
			PacketIndex: uint32(i / maxDataSize),
			Data:        data[i:min(i+maxDataSize, len(data))],
		})
	}

	return pkts, nil
}

func MerkleTreePacket(mt *merkletree.MerkleTree, root [32]byte) (*packet.MerkleTree, error) {
	depth := uint8(2) // TODO: Change this :)

	nodes, err := mt.GetNodesBasedOnRootHash(root[:], depth)
	if err != nil {
		return nil, fmt.Errorf("error getting nodes for root hash %x: %w", root, err)
	}

	realDepth := bits.TrailingZeros(uint(len(nodes)))
	if len(nodes) != 1<<realDepth {
		panic(fmt.Sprintf("unexpected number of nodes %d for depth %d", len(nodes), realDepth))
	}

	flatNodes := make([]byte, 0, len(nodes)*32)
	for _, node := range nodes {
		flatNodes = append(flatNodes, node.Hash()...)
	}

	return &packet.MerkleTree{
		RootHash: root,
		Depth:    uint8(realDepth),
		Nodes:    flatNodes,
	}, nil
}
