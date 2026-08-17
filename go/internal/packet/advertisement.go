package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Advertisement struct {
	// SHADOW: Reserved uint8
	Hash [32]byte
}

func (a *Advertisement) Type() messageType {
	return MessageTypeAdvertisement
}

func (a *Advertisement) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	// Reserved byte is always 0
	if err := binary.Write(buff, binary.BigEndian, uint8(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}

	if err := binary.Write(buff, binary.BigEndian, a.Hash); err != nil {
		return nil, fmt.Errorf("error encoding advertisement hash: %w", err)
	}

	return buff.Bytes(), nil
}

func (a *Advertisement) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	var reserved uint8
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}

	if err := binary.Read(r, binary.BigEndian, &a.Hash); err != nil {
		return fmt.Errorf("error decoding advertisement hash: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}
