package server

import (
	"fmt"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (f *file) MetadataPacket(manifestHash [32]byte) *packet.Metadata {
	return &packet.Metadata{
		ManifestHash: manifestHash,
		Hash:         [32]byte(f.mt.Root()),
		HashExtra:    [32]byte{},
		Size:         f.size,
		MTDepth:      f.mt.Depth(),
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

func (f *file) BlobPacket(index uint32) ([]packet.Data, error) {
	maxDataSize := 512
	if index >= uint32(len(f.chunks)) {
		return nil, fmt.Errorf("invalid blob index %d for file %s", index, f.name)
	}

	data, err := f.chunks[index].Data(f.mmapFile)
	if err != nil {
		return nil, fmt.Errorf("error getting data for blob %d of file %s: %w", index, f.name, err)
	}

	pkts := make([]packet.Data, 0)

	fmt.Println("offset:", f.chunks[index].Offset())

	for i := 0; i < len(data); i += maxDataSize {
		pkts = append(pkts, packet.Data{
			FileHash:   [32]byte(f.mt.Root()),
			BlobHash:   [32]byte(f.chunks[index].Hash()),
			BlobID:     index,
			BlobOffset: f.chunks[index].Offset(),
			BlobLen:    uint32(len(data)),
			PacketID:   uint32(i / maxDataSize),
			Data:       data[i:min(i+maxDataSize, len(data))],
		})
	}

	return pkts, nil
}
