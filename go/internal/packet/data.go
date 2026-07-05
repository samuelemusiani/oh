package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Data struct {
	// RootHash is the root of the Merkle tree containing the leaf
	RootHash [32]byte

	// LeafHash is the hash of the data
	LeafHash [32]byte
	// BlobIndex is the index of the blob in the file
	LeafIndex uint32

	// BlobOffset is the offset of the blob in the file
	// NOTE: We don't acctually need this, we could calculate it if we had received
	// all the previous blobs. But since this would mean a bit of buffering on the
	// client side, we will just send it with the data packet.
	// TODO: REMOVE THIS
	BlobOffset uint64

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
	if err := binary.Write(buff, binary.BigEndian, d.BlobOffset); err != nil {
		return nil, fmt.Errorf("error encoding blob offset: %w", err)
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
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &d.RootHash); err != nil {
		return fmt.Errorf("error decoding file hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.LeafHash); err != nil {
		return fmt.Errorf("error decoding blob hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.LeafIndex); err != nil {
		return fmt.Errorf("error decoding blob id: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.BlobOffset); err != nil {
		return fmt.Errorf("error decoding blob offset: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.PacketIndex); err != nil {
		return fmt.Errorf("error decoding packet id: %w", err)
	}
	var reserved uint16
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved: %w", err)
	}
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("error decoding data length: %w", err)
	}

	if r.Len() != int(length) {
		return fmt.Errorf("error decoding data: expected %d bytes, got %d bytes", length, r.Len())
	}

	d.Data = make([]byte, length)
	if _, err := r.Read(d.Data); err != nil {
		return fmt.Errorf("error decoding data: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}
