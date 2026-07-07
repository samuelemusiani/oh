package merkletree

import (
	"fmt"
	"hash"
	"math/bits"
)

type Node struct {
	hash []byte
}

type MerkleTree struct {
	t []Node
}

func NextPowerOfTwo(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// NewMerkleTree constructs a Merkle tree from the given number of leafs, a
// function to compute the hash of each leaf and a hash function for the rest
// of the tree. The leafHash function should
// take a leaf index and return the corresponding hash. The number of leafs should
// be a power of two for the tree to be complete.
func NewMerkleTree(leafs uint32, leafHash func(leaf uint32) ([]byte, error), h hash.Hash) (*MerkleTree, error) {
	mt := &MerkleTree{}
	mt.t = make([]Node, 2*leafs-1)

	// Set leaf nodes
	for i := range leafs {
		hashBytes, err := leafHash(i)
		if err != nil {
			return nil, fmt.Errorf("error computing leaf hash for leaf %d: %w", i, err)
		}
		mt.t[uint32(len(mt.t))-leafs+i] = Node{hash: hashBytes}
	}

	// Compute the internal nodes
	depth := mt.Depth()
	if depth < 2 {
		return mt, nil
	}

	for d := int16(depth - 2); d >= 0; d-- {
		nodesAtLevel := 1 << d
		for n := nodesAtLevel - 1; n >= 0; n-- {
			pos := nodesAtLevel - 1 + n
			lChildHash := mt.t[2*pos+1].hash
			rChildHash := mt.t[2*pos+2].hash
			h.Reset()
			h.Write([]byte{uint8(int16(depth) - 1 - d)})
			h.Write(lChildHash[:])
			h.Write(rChildHash[:])
			mt.t[pos] = Node{hash: h.Sum(nil)}
		}
	}

	return mt, nil
}

func (mt *MerkleTree) Root() []byte {
	return mt.t[0].hash
}

func (mt *MerkleTree) GetNodes(subtree uint16, depth uint8) []Node {
	startIndex := subtree
	for range depth {
		startIndex = 2*startIndex + 1
	}
	return mt.t[startIndex : startIndex+(1<<depth)]
}

func (n *Node) Hash() []byte {
	return n.hash
}

func (mt *MerkleTree) Depth() uint8 {
	leafs := (len(mt.t) + 1) / 2
	return uint8(bits.Len(uint(leafs)))
}

func (mt *MerkleTree) LeafHash(leaf uint32) []byte {
	leafs := (len(mt.t) + 1) / 2
	if leaf >= uint32(leafs) {
		panic(fmt.Sprintf("leaf index %d out of bounds for Merkle tree with %d leafs", leaf, leafs))
	}
	return mt.t[uint32(len(mt.t))-uint32(leafs)+leaf].hash
}

func LeafsFromDepth(depth uint8) uint32 {
	if depth == 0 {
		return 0
	}
	return 1 << (depth - 1)
}
