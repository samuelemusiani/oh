// AI WARNING: THESE TESTS ARE VIBECODED!!
package merkletree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"testing"
)

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

func buildFullTree(t *testing.T, leafs uint32) *MerkleTree {
	t.Helper()
	mt, err := NewMerkleTree(leafs, func(leaf uint32) ([]byte, error) {
		h := sha256.New()
		h.Write(binary.BigEndian.AppendUint16(nil, uint16(leaf)))
		return h.Sum(nil), nil
	}, sha256.New())
	if err != nil {
		t.Fatalf("cannot build full tree with %d leaves: %v", leafs, err)
	}
	return mt
}

// ---------------------------------------------------------------------
// Full Merkle Tree tests
// ---------------------------------------------------------------------

func TestNextPowerOfTwo(t *testing.T) {
	tests := []struct{ in, want uint32 }{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {4, 4}, {5, 8}, {1023, 1024}, {1024, 1024},
	}
	for _, tt := range tests {
		got := NextPowerOfTwo(tt.in)
		if got != tt.want {
			t.Errorf("NextPowerOfTwo(%d) = %d; want %d", tt.in, got, tt.want)
		}
	}
}

func TestNewMerkleTreeSizes(t *testing.T) {
	for _, n := range []uint32{1, 2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			mt := buildFullTree(t, n)
			if mt == nil {
				t.Fatal("expected non‑nil tree")
			}
			expectedDepth := uint8(bits.Len(uint(n))) - 1
			if mt.Depth() != expectedDepth {
				t.Errorf("depth = %d; want %d", mt.Depth(), expectedDepth)
			}
			leaves := (len(mt.t) + 1) / 2
			if uint32(leaves) != n {
				t.Errorf("leaf count %d; want %d", leaves, n)
			}
		})
	}
}

func TestRootAndLeafAccess(t *testing.T) {
	mt := buildFullTree(t, 4)
	r := mt.Root()
	if len(r) != sha256.New().Size() {
		t.Errorf("root size %d; want %d", len(r), sha256.New().Size())
	}

	// Verify leaves can be retrieved
	for i := range uint32(4) {
		leaf := mt.LeafHash(i)
		if len(leaf) != sha256.New().Size() {
			t.Errorf("leaf %d size %d", i, len(leaf))
		}
	}
}

func TestGetNodesAndGetNodesBasedOnRootHash(t *testing.T) {
	mt := buildFullTree(t, 8)
	// depth := mt.Depth() // 4

	// Root subtree at depth 2 should return 4 leaves
	nodes, err := mt.GetNodesBasedOnRootHash(mt.Root(), 2)
	if err != nil {
		t.Fatalf("GetNodesBasedOnRootHash failed: %v", err)
	}
	if len(nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(nodes))
	}

	// Internal subtree at index 1 (left child of root) depth 1 -> 2 leaves
	subHash := mt.t[1].hash
	subNodes, err := mt.GetNodesBasedOnRootHash(subHash, 1)
	if err != nil {
		t.Fatalf("subtree query failed: %v", err)
	}
	if len(subNodes) != 2 {
		t.Errorf("expected 2 nodes for subtree 1 depth 1, got %d", len(subNodes))
	}
}

func TestLeafHashBounds(t *testing.T) {
	mt := buildFullTree(t, 4)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for out‑of‑range leaf")
		}
	}()
	_ = mt.LeafHash(99)
}

func TestLeafsFromDepth(t *testing.T) {
	tests := []struct {
		depth uint8
		want  uint32
	}{
		{0, 1}, {1, 2}, {2, 4}, {3, 8}, {4, 16},
	}
	for _, tt := range tests {
		got := LeafsFromDepth(tt.depth)
		if got != tt.want {
			t.Errorf("LeafsFromDepth(%d) = %d; want %d", tt.depth, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------
// Partial Tree tests
// ---------------------------------------------------------------------

func TestNewPartialMerkleTree(t *testing.T) {
	h := sha256.New()
	h.Write([]byte("root_hash_12345"))
	rh := h.Sum(nil)

	tests := []uint32{1, 2, 4, 8, 16}
	for _, n := range tests {
		t.Run(fmt.Sprintf("leaves=%d", n), func(t *testing.T) {
			pmt := NewPartialMerkleTree(n, rh, sha256.New())
			if pmt == nil {
				t.Fatal("expected non‑nil partial tree")
			}
			expectedDepth := uint8(bits.Len(uint(n))) - 1
			if pmt.Depth() != expectedDepth {
				t.Errorf("depth %d; want %d", pmt.Depth(), expectedDepth)
			}
			if !bytes.Equal(pmt.t[0].hash, rh) {
				t.Errorf("root hash mismatch")
			}
			if !pmt.t[0].valid {
				t.Error("root should be valid")
			}
			if pmt.t[0].validChilds {
				t.Error("root should not have validChilds initially")
			}
		})
	}
}

func TestFindNodeIndexFromHash(t *testing.T) {
	h := sha256.New()
	h.Write([]byte("special"))
	special := h.Sum(nil)

	pmt := NewPartialMerkleTree(4, special, sha256.New())
	idx, ok := pmt.FindNodeIndexFromHash(special)
	if !ok || idx != 0 {
		t.Errorf("expected index 0, got %d ok=%v", idx, ok)
	}

	_, ok = pmt.FindNodeIndexFromHash([]byte("missing"))
	if ok {
		t.Error("expected false for missing hash")
	}
}

func TestNextHashToQueryInitial(t *testing.T) {
	pmt := NewPartialMerkleTree(4, []byte("root"), sha256.New())
	nh, ok := pmt.NextHashToQuery()
	if !ok {
		t.Fatal("expected a hash to query")
	}
	if string(nh) != "root" {
		t.Errorf("expected root hash, got %s", hex.EncodeToString(nh))
	}
}

func TestNextHashToQueryAfterInsert(t *testing.T) {
	// Build a full tree
	full := buildFullTree(t, 4)

	// Extract root's children (depth=1)
	children, err := full.GetNodesBasedOnRootHash(full.Root(), 1)
	if err != nil {
		t.Fatalf("GetNodesBasedOnRootHash: %v", err)
	}
	flat := make([]byte, len(children)*sha256.New().Size())
	for i, n := range children {
		copy(flat[i*sha256.New().Size():], n.hash)
	}

	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())
	if err := pmt.PutNodes(full.Root(), 1, flat); err != nil {
		t.Fatalf("PutNodes root depth 1: %v", err)
	}

	// After inserting root's children, NextHashToQuery should return
	// one of the children (they don't have children yet).
	nh, ok := pmt.NextHashToQuery()
	if !ok {
		t.Fatal("expected hash to query after insert")
	}
	found := false
	for _, c := range children {
		if bytes.Equal(nh, c.hash) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("queried hash %s not one of the inserted children", hex.EncodeToString(nh))
	}
}

func TestPutNodesRootDepth1(t *testing.T) {
	full := buildFullTree(t, 4)
	children, _ := full.GetNodesBasedOnRootHash(full.Root(), 1)
	flat := concatHashes(children)

	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())
	if err := pmt.PutNodes(full.Root(), 1, flat); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Root should now have validChilds = true and a matching hash
	if !pmt.t[0].validChilds {
		t.Error("expected root validChilds = true after inserting children")
	}
	if !bytes.Equal(pmt.t[0].hash, full.Root()) {
		t.Errorf("root hash changed after PutNodes")
	}
}

func TestPutNodesRootDepth2FullLeaves(t *testing.T) {
	full := buildFullTree(t, 4)
	leaves, _ := full.GetNodesBasedOnRootHash(full.Root(), 2)
	flat := concatHashes(leaves)

	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())
	if err := pmt.PutNodes(full.Root(), 2, flat); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// After full insertion the tree should be fully expanded.
	// All internal nodes should have validChilds = true.
	for i := 0; i < len(pmt.t)-4; i++ { // internal nodes only
		if !pmt.t[i].validChilds {
			t.Errorf("internal node %d expected validChilds = true", i)
		}
	}
}

func TestPutNodesSubTreeLeftChild(t *testing.T) {
	full := buildFullTree(t, 4)
	// Subtree rooted at index 1 (left internal node), depth 1
	subHash := full.t[1].hash
	subNodes, _ := full.GetNodesBasedOnRootHash(subHash, 1)
	flat := concatHashes(subNodes)

	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())

	// PutNodes requires the subtree hash to already exist in the partial tree.
	// Since only the root was initialized, subHash (index 1) is unknown here.
	// Therefore this call MUST fail.
	err := pmt.PutNodes(subHash, 1, flat)
	if err == nil {
		t.Fatal("expected PutNodes to fail because subtree hash is not present in partial tree")
	}
	t.Logf("correctly rejected unknown subtree hash: %v", err)

	// Verify the tree remains untouched (only root valid)
	if pmt.t[0].valid && pmt.t[0].validChilds {
		// root should still have no children
		t.Error("root should not have validChilds after failed insertion")
	}
}

func TestPutNodesInvalidLength(t *testing.T) {
	pmt := NewPartialMerkleTree(4, make([]byte, 32), sha256.New())
	badLength := make([]byte, 3) // not a multiple of 32
	if err := pmt.PutNodes(pmt.t[0].hash, 1, badLength); err == nil {
		t.Error("expected error for wrong node length")
	}
}

func TestPutNodesUnknownSubtreeHash(t *testing.T) {
	pmt := NewPartialMerkleTree(4, make([]byte, 32), sha256.New())
	badHash := make([]byte, 32)
	badHash[0] = 0xFF
	nodes := make([]byte, 2*sha256.New().Size())
	if err := pmt.PutNodes(badHash, 1, nodes); err == nil {
		t.Error("expected error for unknown subtree hash")
	}
}

func TestPutNodesHashMismatch(t *testing.T) {
	full := buildFullTree(t, 4)
	children, _ := full.GetNodesBasedOnRootHash(full.Root(), 1)
	flat := concatHashes(children)
	// Corrupt one byte in the second child
	flat[sha256.New().Size()+10] ^= 0xFF

	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())
	// Because PutNodes writes into the partial tree, the first insertion
	// should succeed; we test conflict by inserting the *same* subtree
	// with different data.
	_ = pmt.PutNodes(full.Root(), 1, flat) // first insert (corrupted)
	// Second insert with correct data should conflict
	goodFlat := concatHashes(children)
	err := pmt.PutNodes(full.Root(), 1, goodFlat)
	if err == nil {
		t.Error("expected hash mismatch error")
	}
}

func TestPartialDepth(t *testing.T) {
	for _, n := range []uint32{1, 2, 4, 8, 16} {
		t.Run(fmt.Sprintf("leaves=%d", n), func(t *testing.T) {
			pmt := NewPartialMerkleTree(n, make([]byte, 32), sha256.New())
			want := uint8(bits.Len(uint(n))) - 1
			if pmt.Depth() != want {
				t.Errorf("Depth() = %d; want %d", pmt.Depth(), want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------

func TestIntegrationRootToFullExpansion(t *testing.T) {
	full := buildFullTree(t, 8)
	pmt := NewPartialMerkleTree(8, full.Root(), sha256.New())

	// Expand level by level using NextHashToQuery + PutNodes
	// (Simplified: we know the structure, so we just insert full leaves)
	leaves, _ := full.GetNodesBasedOnRootHash(full.Root(), uint8(full.Depth()-1))
	flat := concatHashes(leaves)

	if err := pmt.PutNodes(full.Root(), uint8(full.Depth()-1), flat); err != nil {
		t.Fatalf("full expansion failed: %v", err)
	}

	// After full expansion, root hash must match
	if !bytes.Equal(pmt.t[0].hash, full.Root()) {
		t.Errorf("expanded root hash mismatch")
	}

	// NextHashToQuery should now traverse to a leaf (because leaves never get validChilds)
	_, ok := pmt.NextHashToQuery()
	if !ok {
		t.Error("expected a query result even for fully expanded tree (leaves have no children)")
	}
}

// ---------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------

func concatHashes(nodes []Node) []byte {
	h := sha256.New()
	out := make([]byte, 0, len(nodes)*h.Size())
	for _, n := range nodes {
		out = append(out, n.hash...)
	}
	return out
}

func Test(t *testing.T) {
	full := buildFullTree(t, 4)
	full.Print()
	pmt := NewPartialMerkleTree(4, full.Root(), sha256.New())
	pmt.Print()
	h, ok := pmt.NextHashToQuery()
	if !ok {
		t.Error("no hash to query")
	}
	fmt.Println("hash to query: ", hex.EncodeToString(h))
	nodes := full.GetNodes(0, 2)
	flat := concatHashes(nodes)
	err := pmt.PutNodes(full.Root(), 2, flat)
	if err != nil {
		t.Error("cannot put nodes", err)
	}
	pmt.Print()
	h, ok = pmt.NextHashToQuery()
	if !ok {
		t.Error("no hash to query")
	}
	fmt.Println("hash to query: ", hex.EncodeToString(h))

	nodes = full.GetNodes(1, 1)
	flat = concatHashes(nodes)
	err = pmt.PutNodes(full.t[1].hash, 1, flat)
	if err != nil {
		t.Error("cannot put nodes", err)
	}

	h, ok = pmt.NextHashToQuery()
	if !ok {
		t.Error("no hash to query")
	}
	fmt.Println("hash to query: ", hex.EncodeToString(h))
}

func TestGrowable(t *testing.T) {
	realH := 7
	full := buildFullTree(t, 1<<realH)
	full.Print()
	nodes := full.GetNodes(0, uint8(realH-3))
	flat := concatHashes(nodes)

	h, err := FindMTHeight(full.Root(), 1<<(realH-3), flat, sha256.New())
	if err != nil {
		t.Error("error", err)
	}
	if h != uint8(realH) {
		t.Error("wrong height", h)
	}

	// pmt := NewPartialMerkleTree(1, full.Root(), sha256.New())
	// pmt.Print()
	// flat := concatHashes(nodes)
	// pmt.AllowGrow()
	// err := pmt.PutNodes(full.Root(), 1, flat)
	// if err != nil {
	// 	t.Error("cannot put nodes", err)
	// }
	// pmt.Print()
}
