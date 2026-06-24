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
	// Root hash of the Merkle tree of the blob
	Hash [32]byte

	// Root hash of the Merkle tree of extra metadata
	HashExtra [32]byte

	// Size of the blob in bytes
	Size uint64

	// Depth of the Merkle tree of the blob
	MTDepth uint8

	// SHADOW: BlobType blobType

	// Type dependent metadata (file name, stream ID, ...)
	Data BlobData
}

type BlobData interface {
	EncodeBlobData() ([]byte, error)
	DecodeBlobData([]byte) error
	Type() blobType
}

type FileMetadata struct {
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
	// SHADOW: Length of the ID in bytes

	// ID of the stream
	ID []byte
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
	if err := binary.Write(buff, binary.BigEndian, m.Hash); err != nil {
		return nil, fmt.Errorf("error encoding hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.HashExtra); err != nil {
		return nil, fmt.Errorf("error encoding hash extra: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.Size); err != nil {
		return nil, fmt.Errorf("error encoding size: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.MTDepth); err != nil {
		return nil, fmt.Errorf("error encoding MT depth: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, m.Data.Type()); err != nil {
		return nil, fmt.Errorf("error encoding blob type: %w", err)
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
	if err := binary.Read(r, binary.BigEndian, &m.Hash); err != nil {
		return fmt.Errorf("error decoding hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &m.HashExtra); err != nil {
		return fmt.Errorf("error decoding hash extra: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &m.Size); err != nil {
		return fmt.Errorf("error decoding size: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &m.MTDepth); err != nil {
		return fmt.Errorf("error decoding MT depth: %w", err)
	}
	var blobType blobType
	if err := binary.Read(r, binary.BigEndian, &blobType); err != nil {
		return fmt.Errorf("error decoding blob type: %w", err)
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
	if err := binary.Write(buff, binary.BigEndian, uint16(len(f.Name))); err != nil {
		return nil, fmt.Errorf("error encoding file name length: %w", err)
	}
	if _, err := buff.Write([]byte(f.Name)); err != nil {
		return nil, fmt.Errorf("error encoding file name: %w", err)
	}
	return buff.Bytes(), nil
}

func (f *FileMetadata) DecodeBlobData(buff []byte) error {
	r := bytes.NewReader(buff)
	var nameLen uint16
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
	if err := binary.Write(buff, binary.BigEndian, uint16(len(d.Name))); err != nil {
		return nil, fmt.Errorf("error encoding directory name length: %w", err)
	}
	if _, err := buff.Write([]byte(d.Name)); err != nil {
		return nil, fmt.Errorf("error encoding directory name: %w", err)
	}
	return buff.Bytes(), nil
}

func (d *DirectoryMetadata) DecodeBlobData(buff []byte) error {
	r := bytes.NewReader(buff)
	var nameLen uint16
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
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, uint16(len(s.ID))); err != nil {
		return nil, fmt.Errorf("error encoding stream ID length: %w", err)
	}
	if _, err := buff.Write(s.ID); err != nil {
		return nil, fmt.Errorf("error encoding stream ID: %w", err)
	}
	return buff.Bytes(), nil
}

func (s *StreamMetadata) DecodeBlobData(buff []byte) error {
	r := bytes.NewReader(buff)
	var idLen uint16
	if err := binary.Read(r, binary.BigEndian, &idLen); err != nil {
		return fmt.Errorf("error decoding stream ID length: %w", err)
	}
	s.ID = make([]byte, idLen)
	if _, err := r.Read(s.ID); err != nil {
		return fmt.Errorf("error decoding stream ID: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}

var (
	_ BlobData = (*FileMetadata)(nil)
	_ BlobData = (*DirectoryMetadata)(nil)
	_ BlobData = (*StreamMetadata)(nil)
)
