package server

import (
	"encoding/binary"
	"fmt"

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
	leafs := merkletree.NextPowerOfTwo(uint32(len(s.files)))
	h := hashFunc()

	leafHash := func(leaf uint32) ([]byte, error) {
		if leaf >= uint32(len(s.files)) {
			h.Reset()
			return h.Sum(nil), nil
		}

		h.Reset()
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

	s.currentManifestHash = new([32]byte)
	*s.currentManifestHash = [32]byte(mt.Root())

	s.hashLookup[*s.currentManifestHash] = hashLookupEntry{
		t: hashTypeManifest,
	}

	return &packet.Advertisement{
		Hash: *s.currentManifestHash,
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
