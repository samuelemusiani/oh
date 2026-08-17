package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type blobType uint8

const (
	BlobTypeFile blobType = iota
	BlobTypeDirectory
	BlobTypeStream
)

type Metadata struct {
	// Hash of the manifest that contains this metadata
	ManifestHash [32]byte

	// Root hash of the Merkle tree of the data
	DataHash [32]byte

	// Root hash of the Merkle tree of extra metadata
	ExtraMetadataHash [32]byte

	// SHADOW: BlobType blobType

	// SHADOW: Reserved uint8

	// Type dependent metadata (file name, stream ID, ...)
	Data BlobData
}

type BlobData interface {
	EncodeBlobData() ([]byte, error)
	DecodeBlobData([]byte) error
	Type() blobType
}

type FileMetadata struct {
	// Depth of the Merkle tree for the file data
	MerkleTreeDepth uint8

	// SHADOW: Reserved uint6
	// SHADOW: Size bits

	// Size of the file. This can be 64, 128, 256 and 320 bits.
	// Len is encoded in Size bits
	Size []uint8

	// SHADOW: Length of the name in bytes

	// Name of the file
	Name string
}

type DirectoryMetadata struct {
	// SHADOW: Length of the name in bytes

	// Name of the directory
	Name string
}

type StreamMetadata struct {
	// ID of the stream
	ID [256]byte
}

func (m *Metadata) Type() messageType {
	return MessageTypeMetadata
}

func (f *FileMetadata) Type() blobType {
	return BlobTypeFile
}

func (d *DirectoryMetadata) Type() blobType {
	return BlobTypeDirectory
}

func (s *StreamMetadata) Type() blobType {
	return BlobTypeStream
}

func (m *Metadata) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, m.ManifestHash); err != nil {
		return nil, fmt.Errorf("error encoding manifest hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.DataHash); err != nil {
		return nil, fmt.Errorf("error encoding data hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.ExtraMetadataHash); err != nil {
		return nil, fmt.Errorf("error encoding extra metadata hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.Data.Type()); err != nil {
		return nil, fmt.Errorf("error encoding blob type: %w", err)
	}
	// Reserved byte is always 0
	if err := binary.Write(buff, binary.BigEndian, uint8(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}

	blobData, err := m.Data.EncodeBlobData()
	if err != nil {
		return nil, fmt.Errorf("error encoding blob data: %w", err)
	}
	if _, err := buff.Write(blobData); err != nil {
		return nil, fmt.Errorf("error writing blob data: %w", err)
	}

	return buff.Bytes(), nil
}

func (m *Metadata) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &m.ManifestHash); err != nil {
		return fmt.Errorf("error decoding manifest hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &m.DataHash); err != nil {
		return fmt.Errorf("error decoding hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &m.ExtraMetadataHash); err != nil {
		return fmt.Errorf("error decoding hash extra: %w", err)
	}
	var blobType blobType
	if err := binary.Read(r, binary.BigEndian, &blobType); err != nil {
		return fmt.Errorf("error decoding blob type: %w", err)
	}
	var reserved uint8
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}

	switch blobType {
	case BlobTypeFile:
		m.Data = &FileMetadata{}
	case BlobTypeDirectory:
		m.Data = &DirectoryMetadata{}
	case BlobTypeStream:
		m.Data = &StreamMetadata{}
	default:
		return fmt.Errorf("unknown blob type: %d", blobType)
	}

	data, err := io.ReadAll(r) // Read the remaining bytes for the blob data
	if err != nil {
		return fmt.Errorf("error reading blob data: %w", err)
	}

	if err := m.Data.DecodeBlobData(data); err != nil {
		return fmt.Errorf("error decoding blob data: %w", err)
	}

	return nil
}

func (f *FileMetadata) EncodeBlobData() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, f.MerkleTreeDepth); err != nil {
		return nil, fmt.Errorf("error encoding Merkle tree depth: %w", err)
	}

	var reservedAndSize uint8
	switch len(f.Size) {
	case 64 / 8:
		reservedAndSize = 0
	case 128 / 8:
		reservedAndSize = 1
	case 256 / 8:
		reservedAndSize = 2
	case 320 / 8:
		reservedAndSize = 3
	default:
		return nil, fmt.Errorf("invalid size length: %d bytes", len(f.Size))
	}

	if err := binary.Write(buff, binary.BigEndian, reservedAndSize); err != nil {
		return nil, fmt.Errorf("error encoding reserved and size: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.Size); err != nil {
		return nil, fmt.Errorf("error encoding size: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, uint8(len(f.Name))); err != nil {
		return nil, fmt.Errorf("error encoding file name length: %w", err)
	}
	if _, err := buff.Write([]byte(f.Name)); err != nil {
		return nil, fmt.Errorf("error encoding file name: %w", err)
	}
	return buff.Bytes(), nil
}

func (f *FileMetadata) DecodeBlobData(buff []byte) error {
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &f.MerkleTreeDepth); err != nil {
		return fmt.Errorf("error decoding Merkle tree depth: %w", err)
	}
	var reservedAndSize uint8
	if err := binary.Read(r, binary.BigEndian, &reservedAndSize); err != nil {
		return fmt.Errorf("error decoding reserved and size: %w", err)
	}

	switch reservedAndSize & 0x03 {
	case 0:
		f.Size = make([]uint8, 64/8)
	case 1:
		f.Size = make([]uint8, 128/8)
	case 2:
		f.Size = make([]uint8, 256/8)
	case 3:
		f.Size = make([]uint8, 320/8)
	default:
		return fmt.Errorf("invalid size length: %d bytes", reservedAndSize&0x03)
	}

	if err := binary.Read(r, binary.BigEndian, &f.Size); err != nil {
		return fmt.Errorf("error decoding size: %w", err)
	}

	var nameLen uint8
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return fmt.Errorf("error decoding file name length: %w", err)
	}
	nameBytes := make([]byte, nameLen)
	if _, err := r.Read(nameBytes); err != nil {
		return fmt.Errorf("error decoding file name: %w", err)
	}
	f.Name = string(nameBytes)

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}

func (d *DirectoryMetadata) EncodeBlobData() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, uint8(len(d.Name))); err != nil {
		return nil, fmt.Errorf("error encoding directory name length: %w", err)
	}
	if _, err := buff.Write([]byte(d.Name)); err != nil {
		return nil, fmt.Errorf("error encoding directory name: %w", err)
	}
	return buff.Bytes(), nil
}

func (d *DirectoryMetadata) DecodeBlobData(buff []byte) error {
	r := bytes.NewReader(buff)
	var nameLen uint8
	if err := binary.Read(r, binary.BigEndian, &nameLen); err != nil {
		return fmt.Errorf("error decoding directory name length: %w", err)
	}
	nameBytes := make([]byte, nameLen)
	if _, err := r.Read(nameBytes); err != nil {
		return fmt.Errorf("error decoding directory name: %w", err)
	}
	d.Name = string(nameBytes)

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}

func (s *StreamMetadata) EncodeBlobData() ([]byte, error) {
	b := bytes.NewBuffer(nil)
	if _, err := b.Write(s.ID[:]); err != nil {
		return nil, fmt.Errorf("error encoding stream ID: %w", err)
	}
	return b.Bytes(), nil
}

func (s *StreamMetadata) DecodeBlobData(buff []byte) error {
	if len(buff) != 256 {
		return fmt.Errorf("invalid stream ID length: %d", len(buff))
	}
	r := bytes.NewReader(buff)
	if _, err := r.Read(s.ID[:]); err != nil {
		return fmt.Errorf("error decoding stream ID: %w", err)
	}
	return nil
}

var (
	_ BlobData = (*FileMetadata)(nil)
	_ BlobData = (*DirectoryMetadata)(nil)
	_ BlobData = (*StreamMetadata)(nil)
)
