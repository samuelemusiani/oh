package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Request struct {
	Hash [32]byte
}

func (a *Request) Type() messageType {
	return MessageTypeRequest
}

func (a *Request) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, a.Hash); err != nil {
		return nil, fmt.Errorf("error encoding hash: %w", err)
	}

	return buff.Bytes(), nil
}

func (a *Request) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &a.Hash); err != nil {
		return fmt.Errorf("error decoding hash: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}

	return nil
}
