package server

import (
	"fmt"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (f *file) MetadataPacket() *packet.Metadata {
	return &packet.Metadata{
		Hash: [32]byte(f.mt.Root()),
		Size: f.size,
		Data: &packet.FileMetadata{
			Name: f.name,
		},
	}
}

func (s *Server) AdvertisementPacket() (*packet.Advertisement, error) {
	leafs := merkletree.NextPowerOfTwo(uint32(len(s.files)))
	h := hashFunc()

	leafHash := func(leaf uint32) ([]byte, error) {
		if leaf >= uint32(len(s.files)) {
			h.Reset()
			return h.Sum(nil), nil
		}

		h.Reset()
		data, err := s.files[leaf].MetadataPacket().Encode()
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

	return &packet.Advertisement{
		Hash: [32]byte(mt.Root()),
	}, nil
}
