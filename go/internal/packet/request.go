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

// TODO: Request Leaf
