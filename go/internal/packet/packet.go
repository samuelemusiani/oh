package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

type messageType uint8

const (
	MessageTypeAdvertisement messageType = iota
	MessageTypeMetadata
	MessageTypeRequest
	MessageTypeData

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

	// Length is the length of the payload in bytes
	Length uint16
}

type MessageWithSrc struct {
	MSG Message
	Src net.IP
}

type Message interface {
	Type() messageType
	Encode() ([]byte, error)
	Decode([]byte) error
}

func WritePacket(msg Message) ([]byte, error) {
	buff, err := msg.Encode()
	if err != nil {
		return nil, fmt.Errorf("error encoding message: %w", err)
	}

	if len(buff) > 1<<16 {
		return nil, fmt.Errorf("payload too large: %d bytes (max %d bytes)", len(buff), MaxPayloadSize)
	}

	hdr := GlobalHeader{
		Version: 1,
		Type:    msg.Type(),
		Length:  uint16(len(buff)),
	}

	w := bytes.NewBuffer(nil)
	if err := binary.Write(w, binary.BigEndian, hdr); err != nil {
		return nil, fmt.Errorf("error writing global header: %w", err)
	}

	if _, err := w.Write(buff); err != nil {
		return nil, fmt.Errorf("error writing message payload: %w", err)
	}

	return w.Bytes(), nil
}

func ReadPacket(buff []byte) (Message, error) {
	r := bytes.NewReader(buff)
	var hdr GlobalHeader
	if err := binary.Read(r, binary.BigEndian, &hdr); err != nil {
		return nil, fmt.Errorf("error reading global header: %w", err)
	}

	if hdr.Version != 1 {
		return nil, fmt.Errorf("unsupported protocol version: %d", hdr.Version)
	}

	constructor, ok := messageRegistry[messageType(hdr.Type)]
	if !ok {
		return nil, fmt.Errorf("unknown message type: %d", hdr.Type)
	}
	msg := constructor()
	remainingBuff := buff[sizeOfGlobalHeader:max(sizeOfGlobalHeader+int(hdr.Length), len(buff))]
	if err := msg.Decode(remainingBuff); err != nil {
		return nil, fmt.Errorf("error decoding message type %d: %w", msg.Type(), err)
	}

	return msg, nil
}

var messageRegistry = map[messageType]func() Message{
	MessageTypeAdvertisement: func() Message { return &Advertisement{} },
	MessageTypeMetadata:      func() Message { return &Metadata{} },
	MessageTypeRequest:       func() Message { return &Request{} },
	MessageTypeData:          func() Message { return &Data{} },
}
