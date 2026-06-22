package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type FECHeader struct {
	// SHADOW: Length uint16
	Data []byte
}

type FECMessage struct {
	// SHADOW: Length uint16
	Data []byte
}

func (m *FECMessage) Type() messageType {
	return MessageTypeFEC
}

func (m *FECMessage) Encode() ([]byte, error) {
	w := bytes.NewBuffer(nil)
	if err := binary.Write(w, binary.BigEndian, uint16(len(m.Data))); err != nil {
		return nil, fmt.Errorf("error writing FEC message length: %w", err)
	}
	if _, err := w.Write(m.Data); err != nil {
		return nil, fmt.Errorf("error writing FEC message data: %w", err)
	}
	return w.Bytes(), nil
}

func (m *FECMessage) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return fmt.Errorf("error reading FEC message length: %w", err)
	}
	if length != uint16(len(buff)-2) {
		return fmt.Errorf("FEC message length mismatch: expected %d bytes, got %d bytes", length, len(buff)-2)
	}

	if length == 0 {
		m.Data = nil
		return nil
	}

	m.Data = make([]byte, length)
	if _, err := r.Read(m.Data); err != nil {
		return fmt.Errorf("error reading FEC message data: %w", err)
	}
	return nil
}
