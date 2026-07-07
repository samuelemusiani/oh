package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"os"
	"path"
	"time"

	"codeberg.org/go-mmap/mmap"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

var (
	ErrFileExists = errors.New("file already exists")
)

type dataInChunk struct {
	// We don't need to store the file hash here, because we will have a separate channel for each file

	chunkHash   [32]byte
	chunkID     uint32
	packetIndex uint32

	data []byte
}

type file struct {
	// Hash of manifest where metadata for this file is contained
	metadataHash [32]byte

	// Channel to receive data packets for this file
	dataChan chan dataInChunk

	finished chan struct{}

	mmapFile *mmap.File
}

// Gorouting that will request on file at a time from the filesToRequest channel
func (c *Client) requestFiles(finish chan struct{}) {
	atLeastOneFile := false
	for {
		select {
		case f := <-c.filesToRequest:
			err := c.requestFile(f)
			if err != nil {
				c.l["request"].Error("error requesting file", "hash", f, "error", err)
			}
			atLeastOneFile = true
		case <-time.After(5 * time.Second):
			if !atLeastOneFile {
				continue
			}
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
		dataChan: make(chan dataInChunk),
		finished: make(chan struct{}),
	}
	var metadata *packet.Metadata

mloop:
	for k, m := range c.metadataBuff {
		for _, md := range m {
			if md.DataHash == hf {
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

	fileData := fileDataFromMetata(metadata)
	fPath := path.Join(c.dir, fileData.Name)

	// Create file with the size from metadata
	var err error
	fileStruct.mmapFile, err = c.createFile(fPath, binary.NativeEndian.Uint64(fileData.Size))
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

func fileDataFromMetata(m *packet.Metadata) *packet.FileMetadata {
	fm, ok := m.Data.(*packet.FileMetadata)
	if !ok {
		panic("metadata is not of type FileMetadata, this should never happen")
	}

	return fm
}

type chunkInfo struct {
	hash               [32]byte
	id                 uint32
	complete           bool
	currentRollingHash hash.Hash
	lastPacketIndex    uint32
}

type chunkHandler struct {
	// Logger for this file
	l *slog.Logger

	// Hash of the file for which this chunkHandler is responsible
	fileHash [32]byte

	// Timer to reset on each received chunk, to detect when we have not
	// received any chunks for a while and request missing chunks
	timer *time.Timer

	// Total number of bytes written to the file so far
	totalWritten uint64
	// Total size of the file in which the chunks belongs.
	// This is used to check against the totalWritten as a precondition to check
	// if the file is complete.
	totalSize uint64

	chunksInfo map[uint32]*chunkInfo
	dataMap    map[uint32][]*dataInChunk

	// Because we don't know the size of each chunk, we need to keep track of
	// the offset of each chunk in the file.
	chunksOffset map[uint32]uint64

	// Slice of chunk IDs that we need to write to the file but we don't
	// have the offset for yet. This can happen if we receive a chunk out of
	// order, and we need to wait for the previous chunks to be written first
	// or re-request missing chunks
	chunksToWrite []uint32
}

func (ch *chunkHandler) findTotalSize(c *Client) {
	c.filesMapMutex.Lock()
	f, ok := c.filesMap[ch.fileHash]
	if !ok {
		panic(fmt.Sprintf("file not found for hash %x, this should never happen", ch.fileHash))
	}
	c.filesMapMutex.Unlock()

	c.metadataBuffMutex.Lock()
	for _, m := range c.metadataBuff[f.metadataHash] {
		if m.DataHash == ch.fileHash {
			ch.totalSize = binary.NativeEndian.Uint64(fileDataFromMetata(m).Size)
			break
		}
	}
	c.metadataBuffMutex.Unlock()
}

// Writes a complete chunk to the file. It expects the chunk to be complet
// and verified before call
func (ch *chunkHandler) writeChunk(c *Client, id uint32) error {
	mc, ok := ch.chunksInfo[id]
	if !ok {
		return fmt.Errorf("chunk metadata not found for chunk %d", id)
	}
	if !mc.complete {
		return fmt.Errorf("chunk %d is not complete, cannot write to file", id)
	}

	var mmapFile *mmap.File

	c.filesMapMutex.Lock()
	f, ok := c.filesMap[ch.fileHash]
	if !ok {
		c.filesMapMutex.Unlock()
		return fmt.Errorf("file not found for hash %x", ch.fileHash)
	}
	mmapFile = f.mmapFile
	c.filesMapMutex.Unlock()

	offset, ok := ch.chunksOffset[id]
	if !ok {
		ch.chunksToWrite = append(ch.chunksToWrite, id)
		return nil
		// If we don't have an offset for this chunk, we need to calculate it
		// TODO: We cannot panic, we have to delay the writing of this chunk until
		// we have all the previous chunks and can calculate the offset.
		panic(fmt.Sprintf("not implemented yet: calculate offset for chunk %d", id))
	}
	for i, chunk := range ch.dataMap[id] {
		if chunk == nil {
			return fmt.Errorf("chunk %d is complete but packet %d is missing", id, i)
		}
		c.l["file"].Debug("writing chunk to file", "file_hash", htos(ch.fileHash), "chunk_id", id, "packet_index", i, "offset", offset, "len", len(chunk.data))
		n, err := mmapFile.WriteAt(chunk.data, int64(offset))
		if err != nil {
			return fmt.Errorf("error writing chunk %d packet %d to file: %w", id, i, err)
		}
		if n != len(chunk.data) {
			return fmt.Errorf("error writing chunk %d packet %d to file: wrote %d bytes, expected %d", id, i, n, len(chunk.data))
		}
		offset += uint64(len(chunk.data))
		ch.totalWritten += uint64(len(chunk.data))
	}

	// Offset for the next chunk
	ch.chunksOffset[id+1] = offset

	delete(ch.dataMap, id)
	delete(ch.chunksInfo, id)

	return nil
}

// Add a chunk to the chunkMap, verify if we have all the packets for it, if
// so, write it to the file and mark it as complete
func (ch *chunkHandler) addChunk(c *Client, data dataInChunk) error {
	mc, ok := ch.chunksInfo[data.chunkID]
	if !ok {
		mc = &chunkInfo{
			hash:               data.chunkHash,
			id:                 data.chunkID,
			currentRollingHash: hashFunc(),
			lastPacketIndex:    0,
		}
		// Because the hash that we are receiving is a leaf hash, it has the
		// depth (0) prepended to it
		mc.currentRollingHash.Write([]byte{0})
		ch.chunksInfo[data.chunkID] = mc
	} else {
		if mc.hash != data.chunkHash || mc.id != data.chunkID {
			return fmt.Errorf("chunk metadata mismatch for chunk %d: expected hash %x, got hash %x", data.chunkID, mc.hash, data.chunkHash)
		}
	}

	if _, ok := ch.dataMap[data.chunkID]; !ok {
		ch.dataMap[data.chunkID] = make([]*dataInChunk, chunker.MaxChunkSize)
	}
	if data.packetIndex >= uint32(len(ch.dataMap[data.chunkID])) {
		// TODO: We should expand the slice and keep same order. For the future
		panic(fmt.Sprintf("packet index %d out of bounds for chunk %d", data.packetIndex, data.chunkID))
	}

	ch.dataMap[data.chunkID][data.packetIndex] = &data

	for i := mc.lastPacketIndex; i < uint32(len(ch.dataMap[data.chunkID])); i++ {
		if ch.dataMap[data.chunkID][i] == nil {
			mc.lastPacketIndex = uint32(i)
			break
		}
		mc.currentRollingHash.Write(ch.dataMap[data.chunkID][i].data)
		mc.lastPacketIndex = uint32(i + 1)
	}

	// Verify if chunk is complete.
	if !bytes.Equal(mc.currentRollingHash.Sum(nil), mc.hash[:]) {
		ch.l.Debug("chunk hash mismatch", "file_hash", htos(ch.fileHash), "chunk_id", data.chunkID, "expected_hash", htos(mc.hash), "actual_hash", htos([32]byte(mc.currentRollingHash.Sum(nil))))
		return nil
	}

	ch.dataMap[data.chunkID] = ch.dataMap[data.chunkID][:mc.lastPacketIndex] // Trim the slice to the actual size

	ch.l.Debug("chunk complete", "file_hash", htos(ch.fileHash), "chunk_id", data.chunkID, "chunk_hash", htos(data.chunkHash))
	ch.chunksInfo[data.chunkID].complete = true

	// Write chunk to file
	err := ch.writeChunk(c, data.chunkID)
	if err != nil {
		return fmt.Errorf("error writing chunk %d to file: %w", data.chunkID, err)
	}

	c.l["file"].Debug("chunk written to file", "file_hash", htos(ch.fileHash), "chunk_id", data.chunkID, "chunk_hash", htos(data.chunkHash))

	return nil
}

func (c *Client) chunkHandlerForFile(hf [32]byte) {
	c.l["file"].Debug("starting chunk handler for file", "file_hash", htos(hf))
	cHandler := &chunkHandler{
		l:            c.l["file"],
		fileHash:     hf,
		timer:        time.NewTimer(30 * time.Second),
		chunksInfo:   make(map[uint32]*chunkInfo),
		dataMap:      make(map[uint32][]*dataInChunk),
		chunksOffset: make(map[uint32]uint64),
	}

	cHandler.chunksOffset[0] = 0 // First chunk always starts at offset 0

	cHandler.findTotalSize(c)

	for {
		select {
		case dataChunk := <-c.filesMap[hf].dataChan:
			c.l["file"].Debug("received data packet for file", "file_hash", htos(hf))
			c.l["file"].Debug("data info", "chunk_hash", htos(dataChunk.chunkHash), "chunk_id", dataChunk.chunkID, "packet_index", dataChunk.packetIndex)

			cHandler.timer.Reset(5 * time.Second)

			err := cHandler.addChunk(c, dataChunk)
			if err != nil {
				c.l["file"].Error("error adding chunk for file", "file_hash", htos(hf), "error", err)
			}

			if cHandler.totalWritten < cHandler.totalSize {
				c.l["file"].Debug("file not complete yet", "file_hash", htos(hf), "total_written", cHandler.totalWritten, "total_size", cHandler.totalSize)
				continue
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
		case <-cHandler.timer.C:
			c.l["file"].Debug("timeout waiting for chunks for file", "file_hash", htos(hf))
			fmt.Println("chunks to write:", cHandler.chunksToWrite)
			panic("not implemented yet: request missing chunks")
			// TODO: request missing chunks
			cHandler.timer.Reset(5 * time.Second)
		}
	}
}

func (c *Client) merkleTreeForFile(hf [32]byte) (*merkletree.MerkleTree, error) {
	var mmapFile *mmap.File
	var mtDepth uint8
	c.metadataBuffMutex.Lock()
	c.filesMapMutex.Lock()
	f, ok := c.filesMap[hf]
	if !ok {
		c.metadataBuffMutex.Unlock()
		c.filesMapMutex.Unlock()
		return nil, fmt.Errorf("file not found for hash %x", hf)
	}
	mmapFile = f.mmapFile

	for _, m := range c.metadataBuff[f.metadataHash] {
		if m.DataHash == hf {
			mtDepth = fileDataFromMetata(m).MerkleTreeDepth
			break
		}
	}
	if mtDepth == 0 {
		panic(fmt.Sprintf("Merkle tree depth not found for file %x, this should never happen", hf))
	}

	c.metadataBuffMutex.Unlock()
	c.filesMapMutex.Unlock()

	chunks, err := c.chunker.ChunkFile(mmapFile)
	if err != nil {
		return nil, fmt.Errorf("could not chunk file: %w", err)
	}

	expectedLeafs := merkletree.LeafsFromDepth(mtDepth)
	calculatedLeafs := merkletree.NextPowerOfTwo(uint32(len(chunks)))
	if expectedLeafs != calculatedLeafs {
		c.l["file"].Debug("Merkle tree depth does not match number of chunks", "file_hash", htos(hf), "expected_leafs", expectedLeafs, "calculated_leafs", calculatedLeafs)
		return nil, nil
	}

	mt, err := merkletree.NewMerkleTree(calculatedLeafs, func(leaf uint32) ([]byte, error) {
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
		d, err := chunks[leaf].Data(f.mmapFile)
		if err != nil {
			return nil, fmt.Errorf("could not read chunk data: %w", err)
		}
		h.Write(d)
		return h.Sum(nil), nil
	}, hashFunc())
	if err != nil {
		return nil, fmt.Errorf("could not create Merkle tree: %w", err)
	}

	return mt, nil
}
