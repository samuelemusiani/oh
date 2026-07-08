package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type RequestLeaves struct {
	// SHADOW: Reserved uint8
	Hash [32]byte
}

func (a *RequestLeaves) Type() messageType {
	return MessageTypeRequestLeaves
}

func (a *RequestLeaves) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, uint8(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, a.Hash); err != nil {
		return nil, fmt.Errorf("error encoding hash: %w", err)
	}

	return buff.Bytes(), nil
}

func (a *RequestLeaves) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	var reserved uint8
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &a.Hash); err != nil {
		return fmt.Errorf("error decoding hash: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}

type RequestLeaf struct {
	// SHADOW: Reserved uint16

	// Index of the leaf to request
	Index uint32

	// Root hash of the tree to request the leaf from
	RootHash [32]byte
}

func (a *RequestLeaf) Type() messageType {
	return MessageTypeRequestLeaf
}

func (a *RequestLeaf) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved bytes: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, a.Index); err != nil {
		return nil, fmt.Errorf("error encoding index: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, a.RootHash); err != nil {
		return nil, fmt.Errorf("error encoding root hash: %w", err)
	}

	return buff.Bytes(), nil
}

func (a *RequestLeaf) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	var reserved uint16
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved bytes: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &a.Index); err != nil {
		return fmt.Errorf("error decoding index: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &a.RootHash); err != nil {
		return fmt.Errorf("error decoding root hash: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}
