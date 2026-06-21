package internal

import (
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

type FEC interface {
	// BlockSize returns how many data packets form one FEC block
	BlockSize() int

	// RepairCount returns how many repair packets are generated per block
	RepairCount() int

	// BlockID returns the ID of the FEC block, which is used to identify which
	// packets belong to the same block
	BlockID(msg packet.Message) uint64

	// IndexID returns the ID of the packet within the block, which is used to
	// identify duplicate packets. The value MUST be unique for each packet within
	// the same block, but can be reused across different blocks.
	IndexID(msg packet.Message) uint64

	// Encode takes data packets, returns RepairCount() repair packets.
	// If the number of data packets is less than BlockSize(), it pads with FEC
	// packets until BlockSize() is reached.
	// The padding can be zeros or have a meaningful content.
	Encode(block []packet.Message) ([]packet.MessageWithFEC, error)

	// Decode takes a mix of received data+repair packets (possibly with gaps),
	// returns the recovered BlockSize() data packets or error if unrecoverable
	Decode(received []packet.MessageWithFEC) ([]packet.Message, error)
}

type FECType uint8

const (
	FECTypeNull FECType = iota
)

func NewFEC(t FECType) FEC {
	switch t {
	case FECTypeNull:
		return &NullFEC{}
	default:
		panic("unknown FEC type")
	}
}

// NullFEC is a FEC implementation that does nothing
type NullFEC struct{}

func (f *NullFEC) BlockSize() int {
	return 1
}

func (f *NullFEC) RepairCount() int {
	return 0
}

func (f *NullFEC) BlockID(msg packet.Message) uint64 {
	return 0
}

func (f *NullFEC) IndexID(msg packet.Message) uint64 {
	return 0
}

func (f *NullFEC) Encode(msgs []packet.Message) ([]packet.MessageWithFEC, error) {
	result := make([]packet.MessageWithFEC, len(msgs)+f.RepairCount())
	for i, msg := range msgs {
		result[i] = packet.MessageWithFEC{
			MSG: msg,
			FEC: nil,
		}
	}
	return result, nil
}

func (f *NullFEC) Decode(msgs []packet.MessageWithFEC) ([]packet.Message, error) {
	result := make([]packet.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg.MSG.Type() == packet.MessageTypeFEC {
			panic("NullFEC should not receive FEC messages")
		}
		result = append(result, msg.MSG)
	}
	return result, nil
}
