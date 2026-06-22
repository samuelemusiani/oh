package internal

import (
	"encoding/binary"
	"fmt"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

type FEC interface {
	// BlockSize returns how many data packets form one FEC block
	BlockSize() int

	// RepairCount returns how many repair packets are generated per block
	RepairCount() int

	// BlockID returns the ID of the FEC block, which is used to identify which
	// packets belong to the same block
	BlockID(msg packet.MessageWithFEC) (uint64, error)

	// IndexID returns the ID of the packet within the block, which is used to
	// identify duplicate packets. The value MUST be unique for each packet within
	// the same block, but can be reused across different blocks.
	IndexID(msg packet.MessageWithFEC) (uint64, error)

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
	FECTypeSemiNull
)

func NewFEC(t FECType) FEC {
	switch t {
	case FECTypeNull:
		return &nullFEC{}
	case FECTypeSemiNull:
		return &semiNullFEC{
			BlockSizeValue:   4,
			RepairCountValue: 0,
		}
	default:
		panic("unknown FEC type")
	}
}

// nullFEC is a FEC implementation that does nothing
type nullFEC struct{}

func (f *nullFEC) BlockSize() int {
	return 1
}

func (f *nullFEC) RepairCount() int {
	return 0
}

func (f *nullFEC) BlockID(msg packet.MessageWithFEC) (uint64, error) {
	return 0, nil
}

func (f *nullFEC) IndexID(msg packet.MessageWithFEC) (uint64, error) {
	return 0, nil
}

func (f *nullFEC) Encode(msgs []packet.Message) ([]packet.MessageWithFEC, error) {
	result := make([]packet.MessageWithFEC, len(msgs)+f.RepairCount())
	for i, msg := range msgs {
		result[i] = packet.MessageWithFEC{
			MSG: msg,
			FEC: nil,
		}
	}
	return result, nil
}

func (f *nullFEC) Decode(msgs []packet.MessageWithFEC) ([]packet.Message, error) {
	result := make([]packet.Message, 0, len(msgs))
	for _, msg := range msgs {
		if msg.MSG.Type() == packet.MessageTypeFEC {
			panic("NullFEC should not receive FEC messages")
		}
		result = append(result, msg.MSG)
	}
	return result, nil
}

// semiNullFEC is a FEC implementation that does not generate any repair
// packets, but add FEC headers. This is used to test the simple possible FEC
// handling in the client and server, without actually implementing any error
// correction logic.
type semiNullFEC struct {
	BlockSizeValue   int
	RepairCountValue int

	counter uint64
}

type semiNullFECHeader struct {
	BlockID uint64
	IndexID uint64
}

func parseSemiNullFECHeader(fec *packet.FECHeader) (*semiNullFECHeader, error) {
	if len(fec.Data) != 16 {
		return nil, fmt.Errorf("invalid FEC header length: expected 16 bytes, got %d bytes", len(fec.Data))
	}
	return &semiNullFECHeader{
		BlockID: binary.BigEndian.Uint64(fec.Data[:8]),
		IndexID: binary.BigEndian.Uint64(fec.Data[8:]),
	}, nil
}

func encodeSemiNullFECHeader(blockID, indexID uint64) *packet.FECHeader {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[:8], blockID)
	binary.BigEndian.PutUint64(data[8:], indexID)
	return &packet.FECHeader{
		Data: data,
	}
}

func (f *semiNullFEC) BlockSize() int {
	return f.BlockSizeValue
}

func (f *semiNullFEC) RepairCount() int {
	return f.RepairCountValue
}

func (f *semiNullFEC) BlockID(msg packet.MessageWithFEC) (uint64, error) {
	h, err := parseSemiNullFECHeader(msg.FEC)
	if err != nil {
		return 0, fmt.Errorf("error parsing FEC header: %w", err)
	}
	return h.BlockID, nil
}

func (f *semiNullFEC) IndexID(msg packet.MessageWithFEC) (uint64, error) {
	h, err := parseSemiNullFECHeader(msg.FEC)
	if err != nil {
		return 0, fmt.Errorf("error parsing FEC header: %w", err)
	}
	return h.IndexID, nil
}

func (f *semiNullFEC) Encode(block []packet.Message) ([]packet.MessageWithFEC, error) {
	result := make([]packet.MessageWithFEC, f.BlockSize()+f.RepairCount())
	blockID := f.counter
	for i, msg := range block {
		result[i] = packet.MessageWithFEC{
			MSG: msg,
			FEC: encodeSemiNullFECHeader(blockID, uint64(i)),
		}
	}
	for i := len(block); i < f.BlockSize(); i++ {
		result[i] = packet.MessageWithFEC{
			MSG: &packet.FECMessage{},
			FEC: encodeSemiNullFECHeader(blockID, uint64(i)),
		}
	}
	f.counter++
	return result, nil
}

func (f *semiNullFEC) Decode(msgs []packet.MessageWithFEC) ([]packet.Message, error) {
	result := make([]packet.Message, 0, len(msgs))
	var blockID *uint64
	indexIDs := make(map[uint64]struct{})
	for _, msg := range msgs {
		h, err := parseSemiNullFECHeader(msg.FEC)
		if err != nil {
			return nil, fmt.Errorf("error parsing FEC header: %w", err)
		}

		if blockID == nil {
			blockID = &h.BlockID
		} else if *blockID != h.BlockID {
			return nil, fmt.Errorf("mismatched block ID: expected %d, got %d", *blockID, h.BlockID)
		}

		if msg.MSG.Type() == packet.MessageTypeFEC {
			m := msg.MSG.(*packet.FECMessage)
			if len(m.Data) == 0 {
				// This is a padding packet, ignore it
				indexIDs[h.IndexID] = struct{}{}
				continue
			}
			panic("SemiNullFEC should not receive FEC messages with non-empty data")
		}

		if _, ok := indexIDs[h.IndexID]; ok {
			return nil, fmt.Errorf("duplicate index ID: %d", h.IndexID)
		}
		result = append(result, msg.MSG)
		indexIDs[h.IndexID] = struct{}{}
	}

	for i := 0; i < f.BlockSize(); i++ {
		if _, ok := indexIDs[uint64(i)]; !ok {
			return nil, fmt.Errorf("missing packet with index ID: %d", i)
		}
	}
	return result, nil
}
