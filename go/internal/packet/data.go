package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Data struct {
	// RootHash is the root of the Merkle tree containing the leaf
	RootHash [32]byte

	// LeafHash is the hash of the data
	LeafHash [32]byte
	// BlobIndex is the index of the blob in the file
	LeafIndex uint32

	// PacketIndex is the index of the packet in the leaf that we are sending
	PacketIndex uint32

	// SHADOW: Reserved uint16

	// SHADOW: Length uint16

	Data []byte
}

func (d *Data) Type() messageType {
	return MessageTypeData
}

func (d *Data) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, d.RootHash); err != nil {
		return nil, fmt.Errorf("error encoding file hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.LeafHash); err != nil {
		return nil, fmt.Errorf("error encoding blob hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.LeafIndex); err != nil {
		return nil, fmt.Errorf("error encoding blob id: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.PacketIndex); err != nil {
		return nil, fmt.Errorf("error encoding packet id: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved: %w", err)
	}

	if len(d.Data) > 1<<16-1 {
		return nil, fmt.Errorf("data length %d exceeds maximum of %d", len(d.Data), 1<<16-1)
	}
	if err := binary.Write(buff, binary.BigEndian, uint16(len(d.Data))); err != nil {
		return nil, fmt.Errorf("error encoding data length: %w", err)
	}
	if _, err := buff.Write(d.Data); err != nil {
		return nil, fmt.Errorf("error encoding data: %w", err)
	}

	return buff.Bytes(), nil
}

func (d *Data) Decode(buff []byte) error {
	copy(d.RootHash[:], buff[0:32])
	copy(d.LeafHash[:], buff[32:64])
	d.LeafIndex = binary.BigEndian.Uint32(buff[64:68])
	d.PacketIndex = binary.BigEndian.Uint32(buff[68:72])
	// Skip reserved
	length := binary.BigEndian.Uint16(buff[74:76])
	if len(buff) < 76+int(length) {
		return fmt.Errorf("error decoding data: expected %d bytes, got %d bytes", length, len(buff)-77)
	}
	d.Data = make([]byte, length)
	copy(d.Data, buff[76:76+length])
	if len(buff) > 76+int(length) {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}
	return nil
}
