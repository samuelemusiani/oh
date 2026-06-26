package client

import (
	"fmt"
	"slices"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (c *Client) handleAdvertisement(a *packet.Advertisement) error {
	c.metadataBuffMutex.Lock()
	if _, ok := c.metadataBuff[a.Hash]; ok {
		c.metadataBuffMutex.Unlock()
		// Already received this advertisement, ignore it
		return nil
	}

	c.metadataBuff[a.Hash] = make([]*packet.Metadata, 0)
	c.metadataBuffMutex.Unlock()
	// Send request for metadata packets
	c.sendCh <- c.RequestPacket(a.Hash)

	return nil
}

func (c *Client) handleMetadata(m *packet.Metadata) error {
	c.metadataBuffMutex.Lock()
	if _, ok := c.metadataBuff[m.ManifestHash]; !ok {
		c.l["metadata"].Debug("Received metadata packet for unknown advertisement", "manifest_hash", htos(m.ManifestHash))
		c.metadataBuffMutex.Unlock()
		return nil // Ignore metadata packets for unknown advertisements
	}

	for _, existing := range c.metadataBuff[m.ManifestHash] {
		if existing.Hash == m.Hash {
			c.l["metadata"].Debug("Received duplicate metadata packet", "hash", htos(m.Hash))
			c.metadataBuffMutex.Unlock()
			return nil // Ignore duplicate metadata packets
		}
	}

	c.metadataBuff[m.ManifestHash] = append(c.metadataBuff[m.ManifestHash], m)
	c.l["metadata"].Debug("Received metadata packet", "hash", htos(m.Hash), "manifest_hash", htos(m.ManifestHash))

	// Check if we have received all metadata packets for this advertisement

	tmp := make([]packet.Metadata, len(c.metadataBuff[m.ManifestHash]))
	for i, m := range c.metadataBuff[m.ManifestHash] {
		tmp[i] = *m
	}

	c.metadataBuffMutex.Unlock()

	slices.SortFunc(tmp, func(a, b packet.Metadata) int {
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
		c.l["metadata"].Debug("Received metadata packets do not match manifest hash", "manifest_hash", htos(m.ManifestHash), "computed_root", htos([32]byte(mt.Root())))
		return nil // Ignore metadata packets that do not match the manifest hash
	}
	c.l["metadata"].Info("Received all metadata packets for advertisement", "manifest_hash", htos(m.ManifestHash), "num_files", len(tmp))

	// New Metadata packets are valid request the file data
	go func() {
		for _, m := range tmp {
			c.filesToRequest <- m.Hash
		}
	}()

	return nil
}
