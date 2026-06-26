package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Data struct {
	// FileHash is the hash of the file that this data belongs to
	FileHash [32]byte

	// BlobHash is the hash of the blob that this data belongs to
	BlobHash [32]byte
	// BlobIndex is the index of the blob in the file
	BlobID uint32
	// BlobOffset is the offset of the blob in the file
	// NOTE: We don't acctually need this, we could calculate it if we had received
	// all the previous blobs. But since this would mean a bit of buffering on the
	// client side, we will just send it with the data packet.
	BlobOffset uint64
	// BlobLen is the length of the blob in bytes
	BlobLen uint32
	// PacketID is the index of the packet in the blob
	PacketID uint32

	Data []byte
}

func (d *Data) Type() messageType {
	return MessageTypeData
}

func (d *Data) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, d.FileHash); err != nil {
		return nil, fmt.Errorf("error encoding file hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.BlobHash); err != nil {
		return nil, fmt.Errorf("error encoding blob hash: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.BlobID); err != nil {
		return nil, fmt.Errorf("error encoding blob id: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.BlobOffset); err != nil {
		return nil, fmt.Errorf("error encoding blob offset: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.BlobLen); err != nil {
		return nil, fmt.Errorf("error encoding blob len: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, d.PacketID); err != nil {
		return nil, fmt.Errorf("error encoding packet id: %w", err)
	}
	if _, err := buff.Write(d.Data); err != nil {
		return nil, fmt.Errorf("error encoding data: %w", err)
	}

	return buff.Bytes(), nil
}

func (d *Data) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &d.FileHash); err != nil {
		return fmt.Errorf("error decoding file hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.BlobHash); err != nil {
		return fmt.Errorf("error decoding blob hash: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.BlobID); err != nil {
		return fmt.Errorf("error decoding blob id: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.BlobOffset); err != nil {
		return fmt.Errorf("error decoding blob offset: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.BlobLen); err != nil {
		return fmt.Errorf("error decoding blob len: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &d.PacketID); err != nil {
		return fmt.Errorf("error decoding packet id: %w", err)
	}

	d.Data = make([]byte, r.Len())
	if _, err := r.Read(d.Data); err != nil {
		return fmt.Errorf("error decoding data: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}
