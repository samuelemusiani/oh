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

	// We only put the minimum number of leaves. We allow the MT to grow
	mt := merkletree.NewPartialMerkleTree(1, a.Hash[:], hashFunc())
	mt.AllowGrow()
	c.treesMapMutex.Lock()
	c.trees[a.Hash] = mtType{mt: mt, t: merkleTreeMetadata}
	c.treesMapMutex.Unlock()

	c.scheduler.requestManifest(a.Hash)
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
		if existing.DataHash == m.DataHash {
			c.l["metadata"].Debug("Received duplicate metadata packet", "hash", htos(m.DataHash))
			c.metadataBuffMutex.Unlock()
			return nil // Ignore duplicate metadata packets
		}
	}

	c.metadataBuff[m.ManifestHash] = append(c.metadataBuff[m.ManifestHash], m)
	c.l["metadata"].Debug("Received metadata packet", "hash", htos(m.DataHash), "manifest_hash", htos(m.ManifestHash))

	c.treesMapMutex.Lock()
	mt := c.trees[m.ManifestHash].mt
	c.treesMapMutex.Unlock()
	if mt.Depth() != 0 {
		// In this case the tree had grown, so we received a merkle tree, so we need
		// to put the leaf into it to allow the request of the next hash

		tmp := *m
		tmp.ManifestHash = [32]byte{}
		e, err := tmp.Encode()
		if err != nil {
			return fmt.Errorf("encoding metadata packet as leaf: %w", err)
		}
		h := hashFunc()
		h.Reset()
		h.Write([]byte{0})
		h.Write(e)
		sum := h.Sum(nil)
		indexes, ok := mt.FindNodeIndexesFromHash(sum)
		if !ok {
			panic("received leaf for resized merkle tree but not in the tree")
		}
		for _, index := range indexes {
			mt.MarkNodeAsValidWithValidChilds(uint64(index))
		}
		c.scheduler.signalManifestComplete([32]byte(sum))
	}

	// Check if we have received all metadata packets for this advertisement

	tmp := make([]packet.Metadata, len(c.metadataBuff[m.ManifestHash]))
	for i, m := range c.metadataBuff[m.ManifestHash] {
		tmp[i] = *m
	}

	c.metadataBuffMutex.Unlock()

	slices.SortFunc(tmp, func(a, b packet.Metadata) int {
		return slices.Compare(a.DataHash[:], b.DataHash[:])
	})
	for i := range tmp {
		tmp[i].ManifestHash = [32]byte{} // Clear manifest hash
	}

	leafs := merkletree.NextPowerOfTwo(uint32(len(tmp)))
	h := hashFunc()

	leafHash := func(leaf uint32) ([]byte, error) {
		h.Reset()
		h.Write([]byte{0})
		if leaf >= uint32(len(tmp)) {
			return h.Sum(nil), nil
		}

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

	mtsToRequest := make([]*merkletree.MerkleTree, 0)

	// Prepare all the files
	c.metadataBuffMutex.Lock()
	c.filesMapMutex.Lock()
	for _, m := range c.metadataBuff[m.ManifestHash] {
		_, ok := c.filesMap[m.DataHash]
		if ok {
			// TODO: Probably if we have two server with a different manifest but with
			// some common files?
			panic("file already exists in filesMap, this should not happen")
		}
		err := c.prepareFile(m.DataHash)
		if err != nil {
			c.metadataBuffMutex.Unlock()
			c.filesMapMutex.Unlock()
			return fmt.Errorf("error preparing file: %w", err)
		}

		mtsToRequest = append(mtsToRequest, c.filesMap[m.DataHash].mt)
	}
	c.metadataBuffMutex.Unlock()
	c.filesMapMutex.Unlock()

	c.treesMapMutex.Lock()
	c.trees[m.ManifestHash].mt.Copy(mt)
	c.treesMapMutex.Unlock()

	c.scheduler.signalManifestComplete(m.ManifestHash)

	// Enqueue file requests

	c.filesMapMutex.Lock()
	for _, mt := range mtsToRequest {
		c.scheduler.requestFile([32]byte(mt.Root()))
	}
	c.filesMapMutex.Unlock()

	return nil
}
