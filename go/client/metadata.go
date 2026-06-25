package client

import (
	"fmt"
	"slices"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (c *Client) handleAdvertisement(a *packet.Advertisement) error {
	if _, ok := c.metadataBuff[a.Hash]; ok {
		// Already received this advertisement, ignore it
		return nil
	}

	c.metadataBuff[a.Hash] = make([]*packet.Metadata, 0)
	// Send request for metadata packets
	c.sendCh <- c.RequestPacket(a.Hash)

	return nil
}

func (c *Client) handleMetadata(m *packet.Metadata) error {
	if _, ok := c.metadataBuff[m.ManifestHash]; !ok {
		c.l["metadata"].Debug("Received metadata packet for unknown advertisement", "hash", m.ManifestHash)
		return nil // Ignore metadata packets for unknown advertisements
	}

	for _, existing := range c.metadataBuff[m.ManifestHash] {
		if existing.Hash == m.Hash {
			c.l["metadata"].Debug("Received duplicate metadata packet", "hash", m.Hash)
			return nil // Ignore duplicate metadata packets
		}
	}

	c.metadataBuff[m.ManifestHash] = append(c.metadataBuff[m.ManifestHash], m)
	c.l["metadata"].Debug("Received metadata packet", "hash", m.Hash, "manifest_hash", m.ManifestHash)

	// Check if we have received all metadata packets for this advertisement

	tmp := make([]*packet.Metadata, len(c.metadataBuff[m.ManifestHash]))
	copy(tmp, c.metadataBuff[m.ManifestHash])
	slices.SortFunc(tmp, func(a, b *packet.Metadata) int {
		return slices.Compare(a.Hash[:], b.Hash[:])
	})
	for i := range tmp {
		tmp[i].ManifestHash = [32]byte{} // Clear manifest hash
	}

	leafs := merkletree.NextPowerOfTwo(uint32(len(tmp)))
	h := hashFunc()

	leafHash := func(leaf uint32) ([]byte, error) {
		if leaf >= uint32(len(tmp)) {
			h.Reset()
			return h.Sum(nil), nil
		}

		h.Reset()
		data, err := tmp[leaf].Encode()
		if err != nil {
			return nil, fmt.Errorf("error encoding metadata packet for file %d: %w", leaf, err)
		}
		h.Write(data)
		return h.Sum(nil), nil
	}

	mt, err := merkletree.NewMerkleTree(leafs, leafHash, hashFunc())
	if err != nil {
		return fmt.Errorf("error creating Merkle tree: %w", err)
	}

	if slices.Compare(mt.Root(), m.ManifestHash[:]) != 0 {
		c.l["metadata"].Debug("Received metadata packets do not match manifest hash", "manifest_hash", m.ManifestHash, "computed_root", mt.Root())
		return nil // Ignore metadata packets that do not match the manifest hash
	}

	// New Metadata packets are valid, store them and request the file data
	for _, m := range tmp {
		c.filesToRequest <- m.Hash
	}

	return nil
}
