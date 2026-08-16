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
	"slices"
	"sync"
	"time"

	"codeberg.org/go-mmap/mmap"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

var (
	ErrFileExists = errors.New("file already exists")
)

const (
	timeoutPartialChunkRequest = 1000 * time.Millisecond
	timeoutMissingChunkRequest = 1000 * time.Millisecond
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
	metadata     *packet.Metadata

	// Channel to receive data packets for this file
	dataChan chan *dataInChunk

	// Received at least one data packet in the dataChan that was valid
	// If this is true we can be sure that the chunkHandler is active and will
	// re-request missing chunks. This is used in the scheduler
	receivedAtLeastOne bool

	// Because we cannot check if channel is closed :(
	isFinished bool
	finished   chan struct{}

	mmapFile *mmap.File

	mt *merkletree.MerkleTree
}

// prepareFile expects to have mutual exclusion on filesMapMutex and metadataBuffMutex.
// It will create the file on disk and memory-map it, and start a goroutine to handle chunks for this file.
// It will also create an entry in the filesMap for this file
func (c *Client) prepareFile(hf [32]byte) error {
	// To find the hash of the manifest
	fileStruct := file{
		dataChan: make(chan *dataInChunk, 1024*1024),
		finished: make(chan struct{}),
	}
mloop:
	for k, m := range c.metadataBuff {
		for _, md := range m {
			if md.DataHash == hf {
				c.l["file"].Debug("found metadata for file", "file_hash", htos(hf), "manifest_hash", htos(k))
				fileStruct.metadataHash = k
				fileStruct.metadata = md
				break mloop
			}
		}
	}

	// With manifest hash we also have metadata
	if fileStruct.metadata == nil {
		panic("metadata for file not found, this should never happen")
	}

	fileData := fileDataFromMetadata(fileStruct.metadata)
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

	leaves := merkletree.LeafsFromDepth(fileData.MerkleTreeDepth)
	fileStruct.mt = merkletree.NewPartialMerkleTree(leaves, fileStruct.metadata.DataHash[:], hashFunc())
	c.treesMapMutex.Lock()
	c.trees[fileStruct.metadata.DataHash] = mtType{mt: fileStruct.mt, t: merkleTreeFile}
	c.treesMapMutex.Unlock()

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

func fileDataFromMetadata(m *packet.Metadata) *packet.FileMetadata {
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

	// Timer to re-request missing packets for this chunk
	reRequest *time.Timer
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

	rwChunksInfoLock sync.RWMutex
	chunksInfo       map[uint32]*chunkInfo
	dataMap          map[uint32][]*dataInChunk

	// Because we don't know the size of each chunk, we need to keep track of
	// the offset of each chunk in the file.
	chunksOffset map[uint32]uint64

	// Map that stores all the written chunks
	writtenChunks map[uint32]struct{}

	// Slice of chunk IDs that we need to write to the file but we don't
	// have the offset for them yet. This can happen if we receive a chunk out of
	// order, and we need to wait for the previous chunks to be written first
	// or re-request missing chunks
	chunksToWrite []uint32

	// When a chunk is not complete, we need to request it again. This channel
	// receives the chunk IDs that we need to request again.
	reRequest chan uint32
}

func (ch *chunkHandler) findTotalSize(c *Client) {
	c.filesMapMutex.Lock()
	f, ok := c.filesMap[ch.fileHash]
	if !ok {
		panic(fmt.Sprintf("file not found for hash %x, this should never happen", ch.fileHash))
	}
	c.filesMapMutex.Unlock()
	ch.totalSize = binary.NativeEndian.Uint64(fileDataFromMetadata(f.metadata).Size)
}

// Writes a complete chunk to the file. It expects the chunk to be complet
// and verified before call
func (ch *chunkHandler) writeChunk(c *Client, id uint32) error {
	ch.rwChunksInfoLock.RLock()
	mc, ok := ch.chunksInfo[id]
	ch.rwChunksInfoLock.RUnlock()
	if !ok {
		panic(fmt.Sprintf("chunk metadata not found for chunk %d, this should never happen", id))
	}
	if !mc.complete {
		panic(fmt.Sprintf("chunk %d is not complete, this should never happen", id))
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
		c.l["file"].Debug("chunk offset not found, adding to chunksToWrite", "file_hash", htos(ch.fileHash), "chunk_id", id)

		// if len(ch.chunksToWrite) == 0 || len(ch.chunksToWrite) > 0 && ch.chunksToWrite[len(ch.chunksToWrite)-1] != id-1 {
		// 	// We try to be fast by requesting the chunk again because we should
		// 	// have received them in order
		// 	if id == 0 {
		// 		panic("chunk id is 0, this should never happen")
		// 	}
		// 	go func() {
		// 		c.sendCh <- c.RequestLeafPacket(id-1, ch.fileHash)
		// 		c.l["file"].Debug("requesting previous chunk for file", "file_hash", htos(ch.fileHash), "chunk_id", id-1)
		// 	}()
		// }
		// TODO: A sorted set would be much better here probably
		if slices.Contains(ch.chunksToWrite, id) {
			// We already have this chunk in the chunksToWrite slice, so we don't
			// need to add it again
			return nil
		}
		ch.chunksToWrite = append(ch.chunksToWrite, id)
		slices.Sort(ch.chunksToWrite)
		return nil
	}

	leafHash := hashFunc()
	leafHash.Reset()
	leafHash.Write([]byte{0})

	for i, chunk := range ch.dataMap[id] {
		if chunk == nil {
			panic(fmt.Sprintf("chunk %d is complete but packet %d is missing, this should never happen", id, i))
		}
		c.l["file"].Debug("writing packet of chunk to file", "file_hash", htos(ch.fileHash), "chunk_id", id, "packet_index", i, "offset", offset, "len", len(chunk.data))
		n, err := mmapFile.WriteAt(chunk.data, int64(offset))
		if err != nil {
			return fmt.Errorf("error writing chunk %d packet %d to file: %w", id, i, err)
		}
		if n != len(chunk.data) {
			return fmt.Errorf("error writing chunk %d packet %d to file: wrote %d bytes, expected %d", id, i, n, len(chunk.data))
		}
		offset += uint64(len(chunk.data))
		ch.totalWritten += uint64(len(chunk.data))
		leafHash.Write(chunk.data)
	}

	c.l["file"].Debug("chunk written to file", "file_hash", htos(ch.fileHash), "chunk_id", id, "offset", offset, "total_written", ch.totalWritten)

	c.treesMapMutex.Lock()
	err := c.trees[ch.fileHash].mt.PutLeaf(leafHash.Sum(nil), id)
	if err != nil {
		return fmt.Errorf("putting leaf into merkle tree: %w", err)
	}
	c.treesMapMutex.Unlock()

	// Offset for the next chunk
	ch.chunksOffset[id+1] = offset

	ch.writtenChunks[id] = struct{}{}
	delete(ch.dataMap, id)

	return nil
}

// Add a chunk to the chunkMap, verify if we have all the packets for it, if
// so, write it to the file and mark it as complete
func (ch *chunkHandler) addData(c *Client, data *dataInChunk) error {
	if _, ok := ch.writtenChunks[data.chunkID]; ok {
		c.l["file"].Debug("chunk already written to file", "file_hash", htos(ch.fileHash), "chunk_id", data.chunkID)
		return nil
	}

	ch.rwChunksInfoLock.RLock()
	mc, ok := ch.chunksInfo[data.chunkID]
	ch.rwChunksInfoLock.RUnlock()
	if !ok {
		mc = &chunkInfo{
			hash:               data.chunkHash,
			id:                 data.chunkID,
			currentRollingHash: hashFunc(),
			lastPacketIndex:    0,
			reRequest: time.AfterFunc(timeoutPartialChunkRequest, func() {
				ch.reRequest <- data.chunkID
			}),
		}
		// Because the hash that we are receiving is a leaf hash, it has the
		// depth (0) prepended to it
		mc.currentRollingHash.Write([]byte{0})
		ch.rwChunksInfoLock.Lock()
		ch.chunksInfo[data.chunkID] = mc
		ch.rwChunksInfoLock.Unlock()
	} else {
		if mc.hash != data.chunkHash || mc.id != data.chunkID {
			return fmt.Errorf("chunk metadata mismatch for chunk %d: expected hash %x, got hash %x", data.chunkID, mc.hash, data.chunkHash)
		}
	}

	mc.reRequest.Reset(timeoutPartialChunkRequest)

	if _, ok := ch.dataMap[data.chunkID]; !ok {
		ch.dataMap[data.chunkID] = make([]*dataInChunk, chunker.MaxChunkSize/packet.MaxDataPacketPayload+1)
	}
	if data.packetIndex >= uint32(len(ch.dataMap[data.chunkID])) {
		// TODO: We should expand the slice and keep same order. For the future
		panic(fmt.Sprintf("packet index %d out of bounds for chunk %d. len %d", data.packetIndex, data.chunkID, len(ch.dataMap[data.chunkID])))
	}

	ch.dataMap[data.chunkID][data.packetIndex] = data

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
	ch.rwChunksInfoLock.RLock()
	ch.chunksInfo[data.chunkID].complete = true

	ch.chunksInfo[data.chunkID].reRequest.Stop()
	ch.rwChunksInfoLock.RUnlock()

	// Write chunk to file
	err := ch.writeChunk(c, data.chunkID)
	if err != nil {
		return fmt.Errorf("error writing chunk %d to file: %w", data.chunkID, err)
	}

	if len(ch.chunksToWrite) > 0 {
		// We need to check if we can write any of the chunks that we have in the
		// chunksToWrite slice.
		slices.Sort(ch.chunksToWrite)

		id := data.chunkID
		if _, found := slices.BinarySearch(ch.chunksToWrite, id); found {
			return nil
		}
		// We are now expecting this to be written
		id++

		for len(ch.chunksToWrite) > 0 && ch.chunksToWrite[0] == id {
			if ch.chunksToWrite[0] == id {
				nextChunkID := ch.chunksToWrite[0]
				ch.chunksToWrite = ch.chunksToWrite[1:]
				err := ch.writeChunk(c, nextChunkID)
				if err != nil {
					return fmt.Errorf("error writing chunk %d to file: %w", nextChunkID, err)
				}
			}
			id++
		}
	}

	return nil
}

func (c *Client) chunkHandlerForFile(hf [32]byte) {
	c.l["file"].Debug("starting chunk handler for file", "file_hash", htos(hf))
	cHandler := &chunkHandler{
		l:             c.l["file"],
		fileHash:      hf,
		timer:         time.NewTimer(30 * time.Second),
		chunksInfo:    make(map[uint32]*chunkInfo),
		dataMap:       make(map[uint32][]*dataInChunk),
		chunksOffset:  make(map[uint32]uint64),
		writtenChunks: make(map[uint32]struct{}),
		reRequest:     make(chan uint32),
	}

	cHandler.timer.Stop() // We will start the timer when we receive the first chunk

	cHandler.chunksOffset[0] = 0 // First chunk always starts at offset 0

	cHandler.findTotalSize(c)

	// This can go in a separate routing
	go func() {
		for {
			chunkID := <-cHandler.reRequest
			// We need to re-request a particular chunk
			cHandler.rwChunksInfoLock.RLock()
			chInfo, ok := cHandler.chunksInfo[chunkID]
			cHandler.rwChunksInfoLock.RUnlock()
			if !ok {
				// TODO: check this. Maybe if time fires but in the meantime we receive
				// the last chunk block and we finish it (?).
				panic("if timer has fired we must have the chunksInfo")
			}
			chInfo.reRequest.Reset(timeoutPartialChunkRequest)
			// We can request the chunk again, by hash :)

			c.l["file"].Debug("timeout waiting for finishing chunk for file", "file_hash", htos(hf), "chunk_hash", htos(chInfo.hash))
			go c.scheduler.reRequest(chInfo.hash)
		}
	}()

	file := c.filesMap[hf]

	go func() {
		for {
			time.Sleep(1 * time.Second)
			var firstNeededChunk uint64
			if len(cHandler.chunksToWrite) > 0 {
				firstNeededChunk = uint64(cHandler.chunksToWrite[0]) - 1
			}
			fmt.Println("progress", float64(cHandler.totalWritten)/float64(cHandler.totalSize)*100, len(cHandler.chunksToWrite), firstNeededChunk)

			if cHandler.totalWritten >= cHandler.totalSize {
				return
			}
		}
	}()

	go func() {
		for {
			<-cHandler.timer.C
			// This timer should fire when we receive up to k chunks but we need n chunks
			// (where k < n) to complete the file. So basically it's used to request
			// again the chunks that we completly didn't receive are at the end of
			// the file. We should have another way to request missing chunks that
			// are between received chunks

			// TODO: This probably requires only a sweep in the number of leafs (tree
			// is balanced) and request all of them
			c.l["file"].Debug("timeout waiting for chunks for file", "file_hash", htos(hf))

			leavesToRequest := 0

			// TODO: Try to be smart. We can probably the number of last leaves based
			// on file size and chunk size. But for now we will just request all
			// missing chunks up to the depth of the Merkle tree.
			for i := range merkletree.LeafsFromDepth(fileDataFromMetadata(file.metadata).MerkleTreeDepth) {
				cHandler.rwChunksInfoLock.RLock()
				_, ok := cHandler.chunksInfo[uint32(i)]
				cHandler.rwChunksInfoLock.RUnlock()
				if ok {
					continue
				}
				_, ok = cHandler.writtenChunks[uint32(i)]
				if ok {
					continue
				}

				if slices.Contains(cHandler.chunksToWrite, uint32(i)) {
					continue
				}

				if len(cHandler.chunksToWrite) > 0 && cHandler.chunksToWrite[len(cHandler.chunksToWrite)-1] < uint32(i) {
					break
				}

				c.l["file"].Debug("requesting missing chunk for file", "file_hash", htos(hf), "chunk_id", i)
				go c.scheduler.requestLeafByIndex(hf, uint32(i))
				leavesToRequest++
				if leavesToRequest >= 8 {
					break
				}
			}

			cHandler.timer.Reset(timeoutMissingChunkRequest)
		}
	}()

	for {
		dataChunk := <-file.dataChan
		file.receivedAtLeastOne = true
		c.l["file"].Debug("received data packet for file", "file_hash", htos(hf))
		c.l["file"].Debug("data info", "chunk_hash", htos(dataChunk.chunkHash), "chunk_id", dataChunk.chunkID, "packet_index", dataChunk.packetIndex)

		// TODO: Does it make sense as a number?
		if len(cHandler.chunksToWrite) < 10 {
			cHandler.timer.Reset(timeoutMissingChunkRequest)
		}

		err := cHandler.addData(c, dataChunk)
		if err != nil {
			c.l["file"].Error("error adding chunk for file", "file_hash", htos(hf), "error", err)
		}

		if cHandler.totalWritten < cHandler.totalSize {
			c.l["file"].Debug("file not complete yet", "file_hash", htos(hf), "total_written", cHandler.totalWritten, "total_size", cHandler.totalSize, "chunksToWrite", cHandler.chunksToWrite)
			continue
		}

		mt, err := cHandler.merkleTreeForFileFromChunks()
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

		cHandler.timer.Stop()

		// We need to mark the tree as completed
		c.treesMapMutex.Lock()
		cHandler.rwChunksInfoLock.RLock()
		totChunks := uint32(len(cHandler.chunksInfo))
		cHandler.rwChunksInfoLock.RUnlock()
		calculatedLeafs := merkletree.NextPowerOfTwo(totChunks)
		h := hashFunc()
		h.Write([]byte{0})
		hash := h.Sum(nil)
		for i := totChunks; i < calculatedLeafs; i++ {
			err := c.trees[hf].mt.PutLeaf(hash, i)
			if err != nil {
				panic("putting leaf")
			}
		}
		c.treesMapMutex.Unlock()

		// If we reach here, we have received all chunks for the file and the Merkle tree is complete
		c.l["file"].Info("file download complete", "file_hash", htos(hf))

		go c.scheduler.signalFileComplete(hf)

		file.isFinished = true
		close(file.finished)
		select {
		case <-file.dataChan:
		// Drain the channel to avoid blocking the sender
		default:
		}
		close(file.dataChan)
		return
	}
}

// merkleTreeForFileFromChunks creates a Merkle tree for a file from the chunks
// that we have received. This is faster becauase we already calculated the
// hashes of the chunks (leaves)
func (ch *chunkHandler) merkleTreeForFileFromChunks() (*merkletree.MerkleTree, error) {
	ch.rwChunksInfoLock.RLock()
	nChunks := len(ch.chunksInfo)
	ch.rwChunksInfoLock.RUnlock()
	calculatedLeafs := merkletree.NextPowerOfTwo(uint32(nChunks))
	mt, err := merkletree.NewMerkleTree(calculatedLeafs, func(leaf uint32) ([]byte, error) {
		if leaf >= uint32(nChunks) {
			h := hashFunc()
			h.Write([]byte{0})
			return h.Sum(nil), nil
		}
		ch.rwChunksInfoLock.RLock()
		hash := ch.chunksInfo[leaf].hash[:]
		ch.rwChunksInfoLock.RUnlock()
		return hash, nil
	}, hashFunc())
	if err != nil {
		return nil, fmt.Errorf("could not create Merkle tree: %w", err)
	}

	return mt, nil
}

// MerkleTreeForFileFromFile creates a Merkle tree for a file from the file
// itself. This is slow and should be done separately from the chunk handler
func (c *Client) merkleTreeForFileFromFile(hf [32]byte) (*merkletree.MerkleTree, error) {
	var mmapFile *mmap.File
	var mtDepth uint8
	c.filesMapMutex.Lock()
	f, ok := c.filesMap[hf]
	if !ok {
		c.filesMapMutex.Unlock()
		return nil, fmt.Errorf("file not found for hash %x", hf)
	}
	mmapFile = f.mmapFile

	mtDepth = fileDataFromMetadata(f.metadata).MerkleTreeDepth
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
