package server

import (
	"fmt"

	"codeberg.org/go-mmap/mmap"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
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
}

func openFiles(paths []string, chunker *chunker.Chunker) ([]*file, error) {
	files := make([]*file, 0, len(paths))
	for _, path := range paths {
		f, err := openFile(path, chunker)
		if err != nil {
			return nil, fmt.Errorf("error opening file %s: %w", path, err)
		}
		files = append(files, f)
	}

	return files, nil
}

func openFile(path string, chunker *chunker.Chunker) (*file, error) {
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

	chunks, err := chunker.ChunkFile(mmapFile)
	if err != nil {
		return nil, fmt.Errorf("could not chunk file: %w", err)
	}

	return &file{
		size:     uint64(stats.Size()),
		path:     path,
		name:     stats.Name(),
		mmapFile: mmapFile,
		chunks:   chunks,
	}, nil
}
