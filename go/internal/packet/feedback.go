package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type FeedbackRequest struct {
	// SHADOW: Reserved uint8
	RequestID         uint16
	Timespan          uint16
	NumberOfFeedbacks uint16
}

type Feedback struct {
	RequestID uint16
	// SHADOW: Reserved          uint16
	PacketIndex   uint16
	NumberPackets uint16
	// SHADOW: Reserved          uint16
	ServerIP [128]byte
}

func (f *FeedbackRequest) Type() messageType {
	return MessageTypeFeedbackRequest
}

func (f *Feedback) Type() messageType {
	return MessageTypeFeedback
}

func (f *FeedbackRequest) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, uint8(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.RequestID); err != nil {
		return nil, fmt.Errorf("error encoding request ID: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.Timespan); err != nil {
		return nil, fmt.Errorf("error encoding timespan: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.NumberOfFeedbacks); err != nil {
		return nil, fmt.Errorf("error encoding number of feedbacks: %w", err)
	}

	return buff.Bytes(), nil
}

func (f *Feedback) Encode() ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	if err := binary.Write(buff, binary.BigEndian, f.RequestID); err != nil {
		return nil, fmt.Errorf("error encoding request ID: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.PacketIndex); err != nil {
		return nil, fmt.Errorf("error encoding packet index: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, f.NumberPackets); err != nil {
		return nil, fmt.Errorf("error encoding number of packets: %w", err)
	}
	if err := binary.Write(buff, binary.BigEndian, uint16(0)); err != nil {
		return nil, fmt.Errorf("error encoding reserved byte: %w", err)
	}
	if _, err := buff.Write(f.ServerIP[:]); err != nil {
		return nil, fmt.Errorf("error encoding server IP: %w", err)
	}

	return buff.Bytes(), nil
}

func (f *FeedbackRequest) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	var reserved uint8
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &f.RequestID); err != nil {
		return fmt.Errorf("error decoding request ID: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &f.Timespan); err != nil {
		return fmt.Errorf("error decoding timespan: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &f.NumberOfFeedbacks); err != nil {
		return fmt.Errorf("error decoding number of feedbacks: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}
	return nil
}

func (f *Feedback) Decode(buff []byte) error {
	r := bytes.NewReader(buff)
	if err := binary.Read(r, binary.BigEndian, &f.RequestID); err != nil {
		return fmt.Errorf("error decoding request ID: %w", err)
	}
	var reserved uint16
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &f.PacketIndex); err != nil {
		return fmt.Errorf("error decoding packet index: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &f.NumberPackets); err != nil {
		return fmt.Errorf("error decoding number of packets: %w", err)
	}
	if err := binary.Read(r, binary.BigEndian, &reserved); err != nil {
		return fmt.Errorf("error decoding reserved byte: %w", err)
	}
	if _, err := r.Read(f.ServerIP[:]); err != nil {
		return fmt.Errorf("error decoding server IP: %w", err)
	}

	if _, err := r.Read(make([]byte, 1)); err != io.EOF {
		return fmt.Errorf("error decoding: %w", ErrExtraAfterReadingMessage)
	}
	return nil
}
