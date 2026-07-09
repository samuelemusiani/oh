package server

import (
	"fmt"
	"slices"
	"sync"

	"codeberg.org/go-mmap/mmap"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"golang.org/x/sync/errgroup"
)

// The file struct represents a file that has to be sent (or is already sent)
// by the server. It includes also some metadata about the file
type file struct {
	// ID uint32
	// Merkle tree ?
	size uint64

	path string
	name string

	mmapFile *mmap.File

	chunks []*chunker.Chunk
	mt     *merkletree.MerkleTree
}

func (s *Server) openFiles(paths []string) error {
	var mutex sync.Mutex
	files := make([]*file, 0, len(paths))
	g := new(errgroup.Group)
	for _, path := range paths {
		g.Go(func() error {
			f, err := s.openFile(path)
			if err != nil {
				return fmt.Errorf("error opening file %s: %w", path, err)
			}
			mutex.Lock()
			files = append(files, f)
			mutex.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Sort files by their Merkle tree root hash to ensure a deterministic order
	slices.SortFunc(files, func(a, b *file) int {
		return slices.Compare(a.mt.Root(), b.mt.Root())
	})

	s.files = files
	for i, f := range s.files {
		if _, ok := s.hashLookup[[32]byte(f.mt.Root())]; ok {
			panic(fmt.Sprintf("duplicate file hash %x for file %s", f.mt.Root(), f.path))
		}

		s.hashLookup[[32]byte(f.mt.Root())] = hashLookupEntry{
			t:         hashTypeFile,
			fileIndex: i,
		}

		for j, c := range f.chunks {
			lh := f.mt.LeafHash(uint32(j))
			if e, ok := s.hashLookup[[32]byte(lh)]; ok {
				panic(fmt.Sprintf("duplicate chunk hash %x for file %s type %d", c.Hash(), f.path, e.t))
			}

			s.hashLookup[[32]byte(lh)] = hashLookupEntry{
				t:          hashTypeChunk,
				fileIndex:  i,
				chunkIndex: j,
			}
		}
	}

	return nil
}

func (s *Server) openFile(path string) (*file, error) {
	mmapFile, err := mmap.OpenFile(path, mmap.Read)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %w", err)
	}

	stats, err := mmapFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("could not stat file: %w", err)
	}

	if stats.IsDir() {
		return nil, fmt.Errorf("path is a directory: %s", path)
	}

	chunks, err := s.chunker.ChunkFile(mmapFile)
	if err != nil {
		return nil, fmt.Errorf("could not chunk file: %w", err)
	}

	leafs := merkletree.NextPowerOfTwo(uint32(len(chunks)))
	mt, err := merkletree.NewMerkleTree(leafs, func(leaf uint32) ([]byte, error) {
		h := hashFunc()
		h.Reset()
		h.Write([]byte{0})
		if leaf >= uint32(len(chunks)) {
			// If the leaf index is greater than the number of chunks, we return the
			// hash of an empty chunk
			return h.Sum(nil), nil
		}
		// We cannot use the Hash() func for the chunk because we need to appen 0
		// to the beginning of it.
		d, err := chunks[leaf].Data(mmapFile)
		if err != nil {
			return nil, fmt.Errorf("could not read chunk data: %w", err)
		}
		h.Write(d)
		return h.Sum(nil), nil
	}, hashFunc())
	if err != nil {
		return nil, fmt.Errorf("could not create Merkle tree: %w", err)
	}

	return &file{
		size:     uint64(stats.Size()),
		path:     path,
		name:     stats.Name(),
		mmapFile: mmapFile,
		chunks:   chunks,
		mt:       mt,
	}, nil
}

func (s *Server) closeFiles() {
	for _, f := range s.files {
		_ = f.mmapFile.Close()
	}
}
