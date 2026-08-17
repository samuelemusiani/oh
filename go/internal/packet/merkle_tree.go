package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type MerkleTree struct {
	RootHash [32]byte
	Reserved uint8
	Depth    uint8
	Nodes    []byte
}

func (a *MerkleTree) Type() messageType {
	return MessageTypeMerkleTree
}

func (a *MerkleTree) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, a.RootHash); err != nil {
		return nil, fmt.Errorf("error encoding root hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, a.Reserved); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, a.Depth); err != nil {
		return nil, fmt.Errorf("error encoding depth: %w", err)
	}
	if _, err := buff.Write(a.Nodes); err != nil {
		return nil, fmt.Errorf("error encoding nodes: %w", err)
	}

	return buff.Bytes(), nil
}

func (a *MerkleTree) Decode(buff []byte) error {
	copy(a.RootHash[:], buff[:32])
	a.Depth = buff[33]
	length := int(1<<a.Depth) * 32
	if len(buff) < 34+length {
		return fmt.Errorf("buffer too small for nodes: expected at least %d bytes, got %d", 34+length, len(buff))
	} else if len(buff) > 34+length {
		return fmt.Errorf("buffer too large for nodes: expected at most %d bytes, got %d", 34+length, len(buff))
	}
	if len(a.Nodes) != length {
		a.Nodes = make([]byte, length)
	}
	copy(a.Nodes[:], buff[34:])
	return nil
}
