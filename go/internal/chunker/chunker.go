package chunker

import (
	"hash"
	"io"
)

const (
	MaxChunkSize = 1024 * 16
)

// A Chunk represents a chunk of data that has an hash associated with it
type Chunk struct {
	hash   []byte
	offset uint64
	length uint64
}

// Chunker is responsible for splitting data into chunker.Chunks
type Chunker struct {
	hashFunc func() hash.Hash
}

func NewChunker(hashFunc func() hash.Hash) *Chunker {
	h := hashFunc()
	if h.Size() != 32 {
		panic("unsupported hash function: only 32-byte hashes are supported")
	}
	return &Chunker{
		hashFunc: hashFunc,
	}
}

func (c *Chunker) ChunkFile(r io.ReaderAt) ([]*Chunk, error) {
	// TODO: This function should use a Content Defined Chunking algorithm to split
	// the file into chunks. For now, we just split the file into fixed-size chunks
	// of 1024 bytes and hash each chunk.

	h := c.hashFunc()

	size := MaxChunkSize
	chunks := make([]*Chunk, 0)

	for {
		buf := make([]byte, size) // TODO: make this configurable
		n, err := r.ReadAt(buf, int64(len(chunks)*size))
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}

		h.Reset()
		h.Write(buf[:n])
		chunks = append(chunks, &Chunk{
			hash:   h.Sum(nil),
			offset: uint64(len(chunks) * size),
			length: uint64(n),
		})

		if n < size {
			break
		}
	}

	return chunks, nil
}

func (c *Chunk) Hash() []byte {
	return c.hash
}

func (c *Chunk) Offset() uint64 {
	return c.offset
}

func (c *Chunk) Length() uint64 {
	return c.length
}

func (c *Chunk) Data(r io.ReaderAt) ([]byte, error) {
	buf := make([]byte, c.length)
	_, err := r.ReadAt(buf, int64(c.offset))
	if err != nil {
		return nil, err
	}
	return buf, nil
}
