package client

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"codeberg.org/go-mmap/mmap"
	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

var (
	ErrFileExists = errors.New("file already exists")
)

type chunk struct {
	// We don't need to store the file hash here, because we will have a separate channel for each file

	chunkHash   [32]byte
	chunkID     uint32
	chunkOffset uint64
	chunkLen    uint32
	packetIndex uint32

	data []byte
}

type file struct {
	// Hash of manifest where metadata for this file is contained
	metadataHash [32]byte

	// Channel to receive data packets for this file
	chunkChan chan chunk

	finished chan struct{}

	mmapFile *mmap.File
}

// Gorouting that will request on file at a time from the filesToRequest channel
func (c *Client) requestFiles(finish chan struct{}) {
	for {
		select {
		case f := <-c.filesToRequest:
			err := c.requestFile(f)
			if err != nil {
				c.l["request"].Error("error requesting file", "hash", f, "error", err)
			}
		case <-time.After(5 * time.Second):
			finish <- struct{}{}
			return
		}
	}

}

func (c *Client) requestFile(hf [32]byte) error {
	c.metadataBuffMutex.Lock()
	c.filesMapMutex.Lock()

	err := c.prepareFile(hf)
	if err != nil {
		c.metadataBuffMutex.Unlock()
		c.filesMapMutex.Unlock()
		return fmt.Errorf("error preparing file: %w", err)
	}

	c.metadataBuffMutex.Unlock()
	c.filesMapMutex.Unlock()

	c.sendCh <- c.RequestPacket(hf)

	<-c.filesMap[hf].finished

	return nil
}

// prepareFile expects to have mutual exclusion on filesMapMutex and metadataBuffMutex.
// It will create the file on disk and memory-map it, and start a goroutine to handle chunks for this file.
// It will also create an entry in the filesMap for this file
func (c *Client) prepareFile(hf [32]byte) error {
	// To find the hash of the manifest
	fileStruct := file{
		chunkChan: make(chan chunk),
		finished:  make(chan struct{}),
	}
	var metadata *packet.Metadata

mloop:
	for k, m := range c.metadataBuff {
		for _, md := range m {
			if md.Hash == hf {
				c.l["file"].Debug("found metadata for file", "file_hash", htos(hf), "manifest_hash", htos(k))
				fileStruct.metadataHash = k
				metadata = md
				break mloop
			}
		}
	}

	// With manifest hash we also have metadata
	if metadata == nil {
		panic("metadata for file not found, this should never happen")
	}

	fName := fileNameFromMetadata(metadata)
	fPath := path.Join(c.dir, fName)

	// Create file with the size from metadata
	var err error
	fileStruct.mmapFile, err = c.createFile(fPath, metadata.Size)
	if err != nil {
		if errors.Is(err, ErrFileExists) {
			c.l["file"].Debug("file already exists, skipping", "file_hash", hf, "path", fPath)
			return nil
		}
		return fmt.Errorf("error creating file: %w", err)
	}

	c.filesMap[hf] = &fileStruct
	go c.chunkHandlerForFile(hf)
	return nil
}

func (*Client) createFile(path string, size uint64) (*mmap.File, error) {
	// TODO: avoid truncating the file if it already exists, we have to nack only missing parts
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrFileExists
		}
		return nil, fmt.Errorf("error creating file: %w", err)
	}

	err = f.Truncate(int64(size))
	if err != nil {
		return nil, fmt.Errorf("error truncating file: %w", err)
	}

	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("error closing file: %w", err)
	}

	mmapFile, err := mmap.OpenFile(path, mmap.Read|mmap.Write)
	if err != nil {
		return nil, fmt.Errorf("error memory-mapping file: %w", err)
	}

	return mmapFile, nil
}

func fileNameFromMetadata(m *packet.Metadata) string {
	fm, ok := m.Data.(*packet.FileMetadata)
	if !ok {
		panic("metadata is not of type FileMetadata, this should never happen")
	}

	return fm.Name
}

func (c *Client) chunkHandlerForFile(hf [32]byte) {
	c.l["file"].Debug("starting chunk handler for file", "file_hash", htos(hf))
	timer := time.NewTimer(30 * time.Second)

	type metaChunk struct {
		hash     [32]byte
		id       uint32
		offset   uint64
		len      uint32
		complete bool
	}

	chunkMeta := make(map[uint32]*metaChunk)
	chunkMap := make(map[uint32][]*chunk)

	// Verify if we have all the packets for a chunk
	verifyComplete := func(id, length uint32, hash [32]byte) (bool, error) {
		possibleComplete := true
		lowerNil := -1
		highestChunk := -1
		// [ 0, nil, nil, 3, 4, nil, 6]
		for i, c := range chunkMap[id] {
			if c == nil {
				lowerNil = min(lowerNil, i)
				continue
			} else if lowerNil != -1 {
				possibleComplete = false
				break
			}
			if uint32(i) != c.packetIndex {
				possibleComplete = false
				break
			}
			highestChunk = max(highestChunk, i)
		}

		if !possibleComplete {
			return false, nil
		}

		// if complete, check if we have the complete chunk. Total len and hash should match
		totLen := uint32(0)
		h := hashFunc()
		for i := range highestChunk + 1 {
			c := chunkMap[id][i]
			if c == nil {
				panic(fmt.Sprintf("chunk %d is complete but packet %d is missing", id, i))
			}
			totLen += uint32(len(c.data))
			h.Write(c.data)
		}

		if totLen != length {
			c.l["file"].Debug("chunk length mismatch", "file_hash", htos(hf), "chunk_id", id, "expected_len", length, "actual_len", totLen)
			return false, nil
		}

		if !bytes.Equal(h.Sum(nil), hash[:]) {
			c.l["file"].Debug("chunk hash mismatch", "file_hash", htos(hf), "chunk_id", id, "expected_hash", htos(hash), "actual_hash", htos([32]byte(h.Sum(nil))))
			return false, nil
		}

		chunkMap[id] = chunkMap[id][:highestChunk+1] // Trim the slice to the actual size

		return true, nil
	}

	// Write a complete chunk to the file
	writeChunk := func(id uint32) error {
		mc, ok := chunkMeta[id]
		if !ok {
			return fmt.Errorf("chunk metadata not found for chunk %d", id)
		}
		if !mc.complete {
			return fmt.Errorf("chunk %d is not complete, cannot write to file", id)
		}

		var mmapFile *mmap.File

		c.filesMapMutex.Lock()
		f, ok := c.filesMap[hf]
		if !ok {
			c.filesMapMutex.Unlock()
			return fmt.Errorf("file not found for hash %x", hf)
		}
		mmapFile = f.mmapFile
		c.filesMapMutex.Unlock()

		offset := int64(mc.offset)
		for i, chunk := range chunkMap[id] {
			if chunk == nil {
				return fmt.Errorf("chunk %d is complete but packet %d is missing", id, i)
			}
			c.l["file"].Debug("writing chunk to file", "file_hash", htos(hf), "chunk_id", id, "packet_index", i, "offset", offset, "len", len(chunk.data))
			n, err := mmapFile.WriteAt(chunk.data, offset)
			if err != nil {
				return fmt.Errorf("error writing chunk %d packet %d to file: %w", id, i, err)
			}
			if n != len(chunk.data) {
				return fmt.Errorf("error writing chunk %d packet %d to file: wrote %d bytes, expected %d", id, i, n, len(chunk.data))
			}
			offset += int64(len(chunk.data))
		}

		return nil
	}

	// Add a chunk to the chunkMap, verify if we have all the packets for it, if
	// so, write it to the file and mark it as complete
	addChunk := func(ch chunk) error {
		mc, ok := chunkMeta[ch.chunkID]
		if !ok {
			mc = &metaChunk{
				hash:   ch.chunkHash,
				id:     ch.chunkID,
				offset: ch.chunkOffset,
				len:    ch.chunkLen,
			}
			chunkMeta[ch.chunkID] = mc
		} else {
			if mc.hash != ch.chunkHash || mc.len != ch.chunkLen || mc.id != ch.chunkID || mc.offset != ch.chunkOffset {
				return fmt.Errorf("chunk metadata mismatch for chunk %d: expected hash %x and len %d, got hash %x and len %d", ch.chunkID, mc.hash, mc.len, ch.chunkHash, ch.chunkLen)
			}
		}

		if _, ok := chunkMap[ch.chunkID]; !ok {
			chunkMap[ch.chunkID] = make([]*chunk, 1024) // TODO: too small?
		}
		if ch.packetIndex >= uint32(len(chunkMap[ch.chunkID])) {
			// TODO: We should expand the slice and keep same order. For the future
			panic(fmt.Sprintf("packet index %d out of bounds for chunk %d", ch.packetIndex, ch.chunkID))
		}

		chunkMap[ch.chunkID][ch.packetIndex] = &ch

		complete, err := verifyComplete(ch.chunkID, mc.len, mc.hash)
		if err != nil {
			return fmt.Errorf("error verifying chunk %d: %w", ch.chunkID, err)
		}

		if !complete {
			return nil
		}

		c.l["file"].Debug("chunk complete", "file_hash", htos(hf), "chunk_id", ch.chunkID, "chunk_hash", htos(ch.chunkHash))
		chunkMeta[ch.chunkID].complete = true

		// Write chunk to file
		err = writeChunk(ch.chunkID)
		if err != nil {
			return fmt.Errorf("error writing chunk %d to file: %w", ch.chunkID, err)
		}

		c.l["file"].Debug("chunk written to file", "file_hash", htos(hf), "chunk_id", ch.chunkID, "chunk_hash", htos(ch.chunkHash))

		return nil
	}

	for {
		select {
		case ch := <-c.filesMap[hf].chunkChan:
			c.l["file"].Debug("received data packet for file", "file_hash", htos(hf))
			c.l["file"].Debug("data info", "chunk_hash", htos(ch.chunkHash), "chunk_id", ch.chunkID, "chunk_len", ch.chunkLen, "packet_index", ch.packetIndex)

			timer.Reset(30 * time.Second)

			err := addChunk(ch)
			if err != nil {
				c.l["file"].Error("error adding chunk for file", "file_hash", htos(hf), "error", err)
			}

			mt, err := c.merkleTreeForFile(hf)
			if err != nil {
				c.l["file"].Error("error creating Merkle tree for file", "file_hash", htos(hf), "error", err)
				continue
			}
			if mt == nil {
				c.l["file"].Debug("Merkle tree for file is not complete yet", "file_hash", htos(hf))
				continue
			}

			root := mt.Root()
			if !bytes.Equal(root, hf[:]) {
				// c.l["file"].Debug("Merkle tree root does not match file hash", "file_hash", htos(hf), "merkle_root", htos([32]byte(root)))
				continue
			}

			// If we reach here, we have received all chunks for the file and the Merkle tree is complete
			c.l["file"].Info("file download complete", "file_hash", htos(hf))
			close(c.filesMap[hf].finished)
			return
		case <-timer.C:
			c.l["file"].Debug("timeout waiting for chunks for file", "file_hash", hf)
			// TODO: request missing chunks
			timer.Reset(30 * time.Second)
		}
	}
}

func (c *Client) merkleTreeForFile(hf [32]byte) (*merkletree.MerkleTree, error) {
	var mmapFile *mmap.File
	var mtDepth uint8
	c.filesMapMutex.Lock()
	c.metadataBuffMutex.Lock()
	f, ok := c.filesMap[hf]
	if !ok {
		c.filesMapMutex.Unlock()
		c.metadataBuffMutex.Unlock()
		return nil, fmt.Errorf("file not found for hash %x", hf)
	}
	mmapFile = f.mmapFile

	for _, m := range c.metadataBuff[f.metadataHash] {
		if m.Hash == hf {
			mtDepth = m.MTDepth
			break
		}
	}
	if mtDepth == 0 {
		panic("metadata for file not found, this should never happen")
	}

	c.filesMapMutex.Unlock()
	c.metadataBuffMutex.Unlock()

	chunks, err := c.chunker.ChunkFile(mmapFile)
	if err != nil {
		return nil, fmt.Errorf("could not chunk file: %w", err)
	}

	expectedLeafs := uint32(1) << mtDepth
	calculatedLeafs := merkletree.NextPowerOfTwo(uint32(len(chunks)))
	if expectedLeafs != calculatedLeafs {
		return nil, nil
	}

	mt, err := merkletree.NewMerkleTree(calculatedLeafs, func(leaf uint32) ([]byte, error) {
		if leaf >= uint32(len(chunks)) {
			// If the leaf index is greater than the number of chunks, we return the
			// hash of an empty chunk
			h := hashFunc()
			h.Reset()
			return h.Sum(nil), nil
		}
		return chunks[leaf].Hash(), nil
	}, hashFunc())
	if err != nil {
		return nil, fmt.Errorf("could not create Merkle tree: %w", err)
	}

	return mt, nil
}
