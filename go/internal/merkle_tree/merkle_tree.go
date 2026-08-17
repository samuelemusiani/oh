package merkletree

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"hash"
	"math/bits"
	"slices"
	"sync"
)

type Node struct {
	hash []byte

	// Valid indicates whether the hash is valid.
	valid bool

	// Indicates if the node has valid childs
	validChilds bool
}

type MerkleTree struct {
	m sync.Mutex

	t []Node

	hashFunc hash.Hash

	// Merkle tree can grow with PutNodes
	growable bool
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
	mt := &MerkleTree{
		hashFunc: h,
		growable: false,
	}
	mt.t = make([]Node, 2*leafs-1)

	// Set leaf nodes
	for i := range leafs {
		hashBytes, err := leafHash(i)
		if err != nil {
			return nil, fmt.Errorf("error computing leaf hash for leaf %d: %w", i, err)
		}
		mt.t[uint32(len(mt.t))-leafs+i] = Node{hash: hashBytes, valid: true, validChilds: true}
	}

	// Compute the internal nodes
	depth := mt.depth()
	if depth < 1 {
		return mt, nil
	}

	for d := int16(depth - 1); d >= 0; d-- {
		nodesAtLevel := 1 << d
		for n := nodesAtLevel - 1; n >= 0; n-- {
			pos := nodesAtLevel - 1 + n
			lChildHash := mt.t[2*pos+1].hash
			rChildHash := mt.t[2*pos+2].hash
			h.Reset()
			// fmt.Println("depth:", depth, "d:", d, "pos:", pos, "lChildHash:", hex.EncodeToString(lChildHash), "rChildHash:", hex.EncodeToString(rChildHash))
			h.Write([]byte{uint8(int16(depth) - d)})
			h.Write(lChildHash[:])
			h.Write(rChildHash[:])
			mt.t[pos] = Node{hash: h.Sum(nil), valid: true, validChilds: true}
		}
	}

	return mt, nil
}

func (mt *MerkleTree) Root() []byte {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.t[0].hash
}

func (mt *MerkleTree) GetNodes(subtree uint32, depth uint8) []Node {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.getNodes(subtree, depth)
}

func (mt *MerkleTree) getNodes(subtree uint32, depth uint8) []Node {
	startIndex := subtree
	for range depth {
		startIndex = 2*startIndex + 1
	}
	return mt.t[startIndex : startIndex+(1<<depth)]
}

func (mt *MerkleTree) GetAllNodes() []Node {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.t
}

// Because this function is used to then retrieve the nodes hashes/valid or not
// we don't need the hash to be unique becuase the nodes should be the same
func (mt *MerkleTree) GetNodesBasedOnRootHash(h []byte, depth uint8) ([]Node, error) {
	mt.m.Lock()
	defer mt.m.Unlock()
	indexes, ok := mt.findNodeIndexesFromHash(h)
	if !ok {
		return nil, fmt.Errorf("hash not found in the tree: %s", hex.EncodeToString(h))
	}

	index := indexes[0]

	depthOfIndex := mt.depth() - uint8(31-bits.LeadingZeros32(uint32(index+1)))
	// fmt.Println("GetNodesBasedOnRootHash", "index", index, "depth", depth, "depthOfIndex", depthOfIndex, "min", min(depth, depthOfIndex))
	return mt.getNodes(uint32(index), min(depth, depthOfIndex)), nil
}

func (n *Node) Hash() []byte {
	return n.hash
}

func (mt *MerkleTree) Depth() uint8 {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.depth()
}

func (mt *MerkleTree) depth() uint8 {
	leafs := (len(mt.t) + 1) / 2
	return uint8(bits.Len(uint(leafs))) - 1
}

func (mt *MerkleTree) LeafHash(leaf uint32) []byte {
	mt.m.Lock()
	defer mt.m.Unlock()
	leafs := (len(mt.t) + 1) / 2
	if leaf >= uint32(leafs) {
		panic(fmt.Sprintf("leaf index %d out of bounds for Merkle tree with %d leafs", leaf, leafs))
	}
	return mt.t[uint32(len(mt.t))-uint32(leafs)+leaf].hash
}

func LeafsFromDepth(depth uint8) uint32 {
	return 1 << depth
}

func NewPartialMerkleTree(leafs uint32, rootHash []byte, hashFunc hash.Hash) *MerkleTree {
	mt := &MerkleTree{
		t:        make([]Node, 2*leafs-1),
		hashFunc: hashFunc,
	}
	rtCopy := make([]byte, len(rootHash))
	copy(rtCopy, rootHash)
	mt.t[0] = Node{hash: rtCopy, valid: true, validChilds: false}
	return mt
}

func (mt *MerkleTree) NextHashToQuery() ([]byte, bool) {
	mt.m.Lock()
	defer mt.m.Unlock()
	stack := []uint16{0}
	for len(stack) > 0 {
		// fmt.Println("stack", stack)
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if int(node) >= len(mt.t) {
			continue
		}
		n := mt.t[node]
		if !n.valid {
			// fmt.Println("node is invalid")
			continue
		}
		if !n.validChilds {
			// fmt.Println("node has no valid Childs")
			return n.hash, true
		}

		// fmt.Println("len(mt.t)", len(mt.t))
		// fmt.Println("int(2*node+2)", int(2*node+2))
		if int(2*node+2) < len(mt.t) {
			stack = append(stack, 2*node+2)
			stack = append(stack, 2*node+1)
		}
	}

	return nil, false
}

func (mt *MerkleTree) FindNodeIndexesFromHash(h []byte) ([]uint64, bool) {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.findNodeIndexesFromHash(h)
}

func (mt *MerkleTree) findNodeIndexesFromHash(h []byte) ([]uint64, bool) {
	indexes := make([]uint64, 0)
	for i := range mt.t {
		if bytes.Equal(mt.t[i].hash, h) {
			indexes = append(indexes, uint64(i))
		}
	}

	if len(indexes) > 0 {
		return indexes, true
	}

	return nil, false
}

func (mt *MerkleTree) PutNodes(subtreeHash []byte, depth uint8, nodes []byte) error {
	mt.m.Lock()
	defer mt.m.Unlock()
	indexes, ok := mt.findNodeIndexesFromHash(subtreeHash)
	if !ok {
		return fmt.Errorf("subtree hash not found in partial merkle tree: %s", hex.EncodeToString(subtreeHash))
	}
	for _, nIndx := range indexes {
		// fmt.Println("nIndex", nIndx)
		h := mt.hashFunc
		if 1<<depth != len(nodes)/h.Size() {
			return fmt.Errorf("nodes bytes (%d) do not match declared depth (%d)", len(nodes), depth)
		}

		depthOfIndex := 31 - bits.LeadingZeros32(uint32(nIndx+1))
		// fmt.Println("depthOfIndex", depthOfIndex)
		if depthOfIndex+int(depth) > int(mt.depth()) {
			if !mt.growable {
				panic("merkle tree is not growable and depth for PutNodes is too large")
			}
			// Growing merkle tree
			// fmt.Println("------------------ before ----------")
			mt.print()
			mt.grow(1 << (depthOfIndex + int(depth)))
			// fmt.Println("------------------ after ----------")
			mt.print()
		}

		leavesCount := uint64(1 << depth)
		// fmt.Println("leavesCount", leavesCount)
		nHashes := make([][]byte, 1<<leavesCount)
		for i := range leavesCount {
			nHashes[i] = make([]byte, h.Size())
			// fmt.Println("coping indexes", i*h.Size(), (i+1)*h.Size())
			// fmt.Println("coping", hex.EncodeToString(nodes[i*h.Size():(i+1)*h.Size()]))
			copy(nHashes[i], nodes[i*uint64(h.Size()):(i+1)*uint64(h.Size())])
			// fmt.Println("nHashes[i]", hex.EncodeToString(nHashes[i]))
		}

		startIndex := nIndx
		for range depth {
			startIndex = 2*startIndex + 1
		}
		// fmt.Println("startIndex", startIndex)
		for i := range leavesCount {
			if !mt.t[startIndex+i].valid {
				// fmt.Println("storing nodes", "index", startIndex+i, "hash", hex.EncodeToString(nHashes[i]))
				mt.t[startIndex+i].hash = nHashes[i]
				mt.t[startIndex+i].valid = true
			} else {
				if !bytes.Equal(mt.t[startIndex+i].hash, nHashes[i]) {
					return fmt.Errorf("inserting node into a valid node with different hash. new hash: %s old hash %s", hex.EncodeToString(nHashes[i]), hex.EncodeToString(mt.t[startIndex+i].hash))
				}
			}
		}

		if startIndex%2 == 0 {
			startIndex--
		}

		for i := uint64(0); i < leavesCount; i += 2 {
			err := mt.computeTreeUpwards(uint32(startIndex + i))
			if err != nil {
				return fmt.Errorf("computing tree upwards for index %d: %w", startIndex+i, err)
			}
		}
	}

	return nil
}

// This function is used to put a single leaf only
func (mt *MerkleTree) PutLeaf(h []byte, index uint32) error {
	mt.m.Lock()
	defer mt.m.Unlock()
	leaves := LeafsFromDepth(mt.depth())
	if index >= uint32(leaves) {
		return fmt.Errorf("leaf index too large. got %d where max is %d", index, leaves)
	}

	if len(h) != mt.hashFunc.Size() {
		return fmt.Errorf("hash len and hashfunc size missmatch")
	}

	absIndex := uint32(len(mt.t)) - leaves + index

	if mt.t[absIndex].valid {
		if slices.Equal(mt.t[absIndex].hash, h) {
			mt.t[absIndex].validChilds = true
			return nil
		} else {
			return fmt.Errorf("leaf already valid but with a different hash. old %s new %s", hex.EncodeToString(mt.t[absIndex].hash), hex.EncodeToString(h))
		}
	}

	mt.t[absIndex].valid = true
	mt.t[absIndex].hash = make([]byte, len(h))
	copy(mt.t[absIndex].hash, h)
	mt.t[absIndex].validChilds = true // TODO: Should i put this here?

	var err error
	if absIndex%2 == 1 { // Left leaf
		// fmt.Println("putted leaft leaf", hex.EncodeToString(mt.t[absIndex].hash))
		err = mt.computeTreeUpwards(absIndex)
	} else { // Right leaf
		// fmt.Println("putted right leaf", hex.EncodeToString(mt.t[absIndex].hash))
		err = mt.computeTreeUpwards(absIndex - 1)
	}
	if err != nil {
		return fmt.Errorf("error computing tree: %w", err)
	}

	return nil
}

func (mt *MerkleTree) computeTreeUpwards(leftIndex uint32) error {
	// fmt.Println("computing for index", leftIndex)
	if leftIndex == 0 {
		return nil
	}
	if !mt.t[leftIndex+1].valid {
		// fmt.Println("nothign todo")
		return nil // nothing to do
	}

	h := mt.hashFunc
	h.Reset()
	depthOfIndex := 31 - bits.LeadingZeros32(uint32(leftIndex+1))
	// fmt.Println("depth of index:", depthOfIndex)
	// fmt.Println("depth:", uint8(mt.depth()-uint8(depthOfIndex))+1)
	h.Write([]byte{uint8(mt.depth()-uint8(depthOfIndex)) + 1})
	// fmt.Println("left hash", hex.EncodeToString(mt.t[leftIndex].hash))
	// fmt.Println("right hash", hex.EncodeToString(mt.t[leftIndex+1].hash))
	h.Write(mt.t[leftIndex].hash)
	h.Write(mt.t[leftIndex+1].hash)
	parentHash := h.Sum(nil)
	// fmt.Println("parent hash", hex.EncodeToString(parentHash))

	parent := (leftIndex - 1) / 2
	if mt.t[parent].valid {
		if slices.Equal(mt.t[parent].hash, parentHash) {
			mt.t[parent].validChilds = true
			return nil // nothing to do
		} else {
			return fmt.Errorf("parent hash does not match. index %d old hash %s new hash %s", parent, hex.EncodeToString(mt.t[parent].hash), hex.EncodeToString(parentHash))
		}
	}

	mt.t[parent].valid = true
	mt.t[parent].validChilds = true
	mt.t[parent].hash = parentHash

	if parent%2 == 1 {
		return mt.computeTreeUpwards(parent)
	} else {
		return mt.computeTreeUpwards(parent - 1)
	}
}

func (mt *MerkleTree) Print() {
	mt.m.Lock()
	defer mt.m.Unlock()
	mt.print()
}

func (mt *MerkleTree) print() {
	for i, n := range mt.t {
		fmt.Println(i, hex.EncodeToString(n.hash))
	}
}

func (mt *MerkleTree) NumberOfLeavesFromRootHash(h []byte) (uint32, error) {
	mt.m.Lock()
	defer mt.m.Unlock()
	return mt.numberOfLeavesFromRootHash(h)
}

func (mt *MerkleTree) numberOfLeavesFromRootHash(h []byte) (uint32, error) {
	indexes, ok := mt.findNodeIndexesFromHash(h)
	if !ok {
		return 0, fmt.Errorf("hash not found in the tree: %s", hex.EncodeToString(h))
	}

	// Because same root means same nodes and same number of leaves
	index := indexes[0]

	depthOfIndex := 31 - bits.LeadingZeros32(uint32(index+1))
	depth := mt.depth() - uint8(depthOfIndex)

	return LeafsFromDepth(depth), nil
}

// Because this function is used to then retrieve the data from the leaves,
// we don't need the hash to be unique becuase the content will be the same
func (mt *MerkleTree) GetAbsoluteLevesIndexesFromRootHash(h []byte) ([]uint32, error) {
	mt.m.Lock()
	defer mt.m.Unlock()
	indexes, ok := mt.findNodeIndexesFromHash(h)
	if !ok {
		return nil, fmt.Errorf("hash not found in the tree: %s", hex.EncodeToString(h))
	}

	index := indexes[0]

	depthOfIndex := 31 - bits.LeadingZeros32(uint32(index+1))
	depth := mt.depth() - uint8(depthOfIndex)

	nleaves, err := mt.numberOfLeavesFromRootHash(h)
	if err != nil {
		return nil, fmt.Errorf("cannot get number of leaves for root hash: %w", err)
	}

	firstLeafIndex := (len(mt.t)+1)/2 - 1

	for range depth {
		index = 2*index + 1
	}

	leavesIndexes := make([]uint32, nleaves)
	for i := range nleaves {
		leavesIndexes[i] = uint32(index) + i - uint32(firstLeafIndex)
	}

	return leavesIndexes, nil
}

func (mt *MerkleTree) AllowGrow() {
	mt.m.Lock()
	defer mt.m.Unlock()
	mt.growable = true
}

func (mt *MerkleTree) DisableGrow() {
	mt.m.Lock()
	defer mt.m.Unlock()
	mt.growable = false
}

func (mt *MerkleTree) Grow(leaves uint32) {
	mt.m.Lock()
	defer mt.m.Unlock()
	mt.grow(leaves)
}

func (mt *MerkleTree) grow(leaves uint32) {
	total := 2*leaves - 1
	if len(mt.t) >= int(total) {
		panic(fmt.Sprintf("called grow with a lower number of current leaves. new %d old %d\n", total, len(mt.t)))
	}

	newSlice := make([]Node, total)
	copy(newSlice, mt.t)
	mt.t = newSlice
}

func FindMTHeight(root []byte, nleaves uint32, leaves []byte, h hash.Hash) (uint8, error) {
	if len(leaves) != int(nleaves)*h.Size() {
		return 0, fmt.Errorf("leaves bytes (%d) do not match number of leaves (%d)", len(leaves), nleaves)
	}

	base := 31 - bits.LeadingZeros32(nleaves)

	for i := base; i <= 1<<8; i++ {
		// fmt.Println("searcing for h", i)
		mt := NewPartialMerkleTree(1<<i, root, h)
		err := mt.PutNodes(root, uint8(base), leaves)
		if err != nil {
			continue
		}
		return uint8(i), nil
	}

	return 0, fmt.Errorf("could not find hight for merkle tree")
}

func (mt *MerkleTree) MarkNodeAsValidWithValidChilds(index uint64) {
	mt.m.Lock()
	defer mt.m.Unlock()
	mt.t[index].valid = true
	mt.t[index].validChilds = true
}

func (mt *MerkleTree) Copy(m *MerkleTree) {
	mt.m.Lock()
	defer mt.m.Unlock()

	m.m.Lock()
	defer m.m.Unlock()

	newNodes := make([]Node, len(m.t))
	for i := range newNodes {
		newNodes[i].hash = make([]byte, len(m.t[i].hash))
		copy(newNodes[i].hash, m.t[i].hash)
		newNodes[i].valid = m.t[i].valid
		newNodes[i].validChilds = m.t[i].validChilds
	}

	mt.t = newNodes
	mt.hashFunc = m.hashFunc
	mt.growable = m.growable
}
