package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type messageType uint8

const (
	MessageTypeAdvertisement messageType = iota
	MessageTypeMetadata
	MessageTypeRequest
	MessageTypeFEC

	FlagFEC = 1 << 0

	MaxPayloadSize = 1024
)

var (
	sizeOfGlobalHeader          = binary.Size(GlobalHeader{})
	ErrExtraAfterReadingMessage = fmt.Errorf("extra data after reading message")
)

type GlobalHeader struct {
	// Version is the version of the protocol. For now it exists only version 1
	Version uint8
	// Type is the type of the message
	Type messageType

	Flags uint8

	// Length is the length of the payload in bytes
	Length uint16
}

type MessageWithFEC struct {
	MSG Message
	FEC *FECHeader
}

type Message interface {
	Type() messageType
	Encode() ([]byte, error)
	Decode([]byte) error
}

func WritePacket(msg Message, fec *FECHeader) ([]byte, error) {
	buff, err := msg.Encode()
	if err != nil {
		return nil, fmt.Errorf("error encoding message: %w", err)
	}

	if len(buff) > 1<<16 {
		return nil, fmt.Errorf("payload too large: %d bytes (max %d bytes)", len(buff), MaxPayloadSize)
	}

	var flags uint8
	if fec != nil {
		flags |= FlagFEC
	}

	hdr := GlobalHeader{
		Version: 1,
		Type:    msg.Type(),
		Flags:   flags,
		Length:  uint16(len(buff)),
	}

	w := bytes.NewBuffer(nil)
	if err := binary.Write(w, binary.BigEndian, hdr); err != nil {
		return nil, fmt.Errorf("error writing global header: %w", err)
	}

	if fec != nil {
		if err := binary.Write(w, binary.BigEndian, uint16(len(fec.Data))); err != nil {
			return nil, fmt.Errorf("error writing FEC header length: %w", err)
		}
		if _, err := w.Write(fec.Data); err != nil {
			return nil, fmt.Errorf("error writing FEC header data: %w", err)
		}
	}

	if _, err := w.Write(buff); err != nil {
		return nil, fmt.Errorf("error writing message payload: %w", err)
	}

	return w.Bytes(), nil
}

func ReadPacket(buff []byte) (MessageWithFEC, error) {
	r := bytes.NewReader(buff)
	var hdr GlobalHeader
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return MessageWithFEC{}, fmt.Errorf("error reading global header: %w", err)
	}

	if hdr.Version != 1 {
		return MessageWithFEC{}, fmt.Errorf("unsupported protocol version: %d", hdr.Version)
	}

	var fec *FECHeader
	if hdr.Flags&FlagFEC != 0 {
		var fecLength uint16
		if err := binary.Read(r, binary.BigEndian, &fecLength); err != nil {
			return MessageWithFEC{}, fmt.Errorf("error reading FEC header length: %w", err)
		}

		fecData := make([]byte, fecLength)
		if _, err := r.Read(fecData); err != nil {
			return MessageWithFEC{}, fmt.Errorf("error reading FEC header data: %w", err)
		}

		fec = &FECHeader{
			Data: fecData,
		}
	}

	constructor, ok := messageRegistry[messageType(hdr.Type)]
	if !ok {
		return MessageWithFEC{}, fmt.Errorf("unknown message type: %d", hdr.Type)
	}

	msg := constructor()
	remainingBuff := buff[sizeOfGlobalHeader:max(sizeOfGlobalHeader+int(hdr.Length), len(buff))]
	if err := msg.Decode(remainingBuff); err != nil {
		return MessageWithFEC{}, fmt.Errorf("error decoding message type %d: %w", msg.Type(), err)
	}

	return MessageWithFEC{
		MSG: msg,
		FEC: fec,
	}, nil
}

var messageRegistry = map[messageType]func() Message{
	MessageTypeAdvertisement: func() Message { return &Advertisement{} },
	MessageTypeMetadata:      func() Message { return &Metadata{} },
	MessageTypeRequest:       func() Message { return &Request{} },
	MessageTypeFEC:           func() Message { return &FECMessage{} },
}
