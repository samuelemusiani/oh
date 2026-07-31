package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	fecFlushTimeout = 100 * time.Millisecond
	fecWaitTimeout  = 500 * time.Millisecond
)

type Packet []byte

type PacketWithSrc struct {
	Packet Packet
	Src    net.IP
}

type FECEncoder struct {
	fec FEC
	l   *slog.Logger
}

func NewFECEncoder(fec FEC, l *slog.Logger) *FECEncoder {
	return &FECEncoder{
		fec: fec,
		l:   l,
	}
}

func (e *FECEncoder) Run(recv <-chan packet.Message, send chan<- Packet) error {
	fecBlock := make([]packet.Message, 0, e.fec.BlockSize())
	ticker := time.NewTicker(fecFlushTimeout)

	flushBlock := func() error {
		if len(fecBlock) == 0 {
			return nil
		}
		encodedBlock, err := e.fec.Encode(fecBlock)
		if err != nil {
			return fmt.Errorf("error encoding FEC block: %w", err)
		}
		for _, msg := range encodedBlock {
			send <- msg
		}
		return nil
	}

	for {
		select {
		case <-ticker.C:
			err := flushBlock()
			if err != nil {
				e.l.Error("error flushing FEC block", "error", err)
			}
			fecBlock = fecBlock[:0]

		case msg := <-recv:
			switch msg.Type() {
			case packet.MessageTypeAdvertisement, packet.MessageTypeFeedbackRequest:
				// Advertisement messages should not be included in FEC blocks
				e.l.Error("received advertisement message, encoding and sending immediately")
				pkt, err := e.fec.EncodeIgnore(msg)
				if err != nil {
					e.l.Error("error encoding advertisement message", "error", err)
					continue
				}
				send <- pkt
			default:
				fecBlock = append(fecBlock, msg)
				if len(fecBlock) >= e.fec.BlockSize() {
					err := flushBlock()
					if err != nil {
						e.l.Error("error flushing FEC block", "error", err)
					}
					fecBlock = fecBlock[:0]
				}
			}
		}
	}
}

type FECDecoder struct {
	fec FEC
	l   *slog.Logger
}

func NewFECDecoder(fec FEC, l *slog.Logger) *FECDecoder {
	return &FECDecoder{
		fec: fec,
		l:   l,
	}
}

func (d *FECDecoder) Run(recv <-chan PacketWithSrc, send chan<- packet.MessageWithSrc) error {
	type block struct {
		buff   []PacketWithSrc
		index  map[uint64]struct{}
		ctx    context.Context
		cancel context.CancelFunc
		timer  *time.Timer
	}

	blockMap := make(map[uint64]*block)
	// TODO: There is a small possiblity of deadlock if the blockChan is full
	// and the main loop does not select on it
	blockChan := make(chan uint64, 4096)

	notifyBlock := func(t <-chan time.Time, ctx context.Context, blockID uint64) {
		select {
		case <-t:
			d.l.Debug("FEC block wait timer expired, notifying block for decoding", "blockID", blockID, "random", ctx.Value("random"))
			blockChan <- blockID
		case <-ctx.Done():
			d.l.Debug("FEC block wait timer cancelled, not notifying block for decoding", "blockID", blockID, "random", ctx.Value("random"))
			return
		}
	}

	handleFECMessage := func(msg PacketWithSrc) error {
		blockID, err := d.fec.BlockID(msg.Packet)
		if err != nil {
			return fmt.Errorf("error getting block ID from FEC header: %w", err)
		}

		indexID, err := d.fec.IndexID(msg.Packet)
		if err != nil {
			return fmt.Errorf("error getting index ID from FEC header: %w", err)
		}
		d.l.Debug("received FEC message", "blockID", blockID, "indexID", indexID, "src", msg.Src.String())

		b, ok := blockMap[blockID]
		if !ok {
			d.l.Debug("received first packet for FEC block, creating new block", "blockID", blockID)
			cancelCtx, cancel := context.WithCancel(context.Background())
			cancelCtx = context.WithValue(cancelCtx, "random", rand.Uint64())
			b = &block{
				buff:   make([]PacketWithSrc, 0, d.fec.BlockSize()+d.fec.RepairCount()),
				index:  make(map[uint64]struct{}),
				timer:  time.NewTimer(fecWaitTimeout),
				ctx:    cancelCtx,
				cancel: cancel,
			}
			blockMap[blockID] = b
			go notifyBlock(b.timer.C, b.ctx, blockID)
		}

		if _, ok := b.index[indexID]; ok {
			d.l.Debug("duplicate packet received, ignoring", "blockID", blockID, "indexID", indexID)
			return nil
		}

		if len(b.buff) > 0 {
			// Check if src IP matches every other packet
			src := b.buff[0].Src
			for _, msg := range b.buff {
				if !msg.Src.Equal(src) {
					// TODO: This is highly likely to happen with multiple senders,
					// even one sender that changes its IP address during the transmission.
					// We should handle this differently
					panic(fmt.Sprintf("blockID %d has messages from different sources: %s and %s", blockID, src.String(), msg.Src.String()))
				}
			}
		}

		if !b.timer.Reset(fecWaitTimeout) {
			d.l.Debug("FEC block wait timer expired, discarding packet", "blockID", blockID)
			return nil
		}

		// We received a new packet for this block
		b.buff = append(b.buff, msg)
		b.index[indexID] = struct{}{}

		if len(b.buff) >= d.fec.BlockSize() {
			d.l.Debug("received enough packets to decode FEC block", "blockID", blockID)
			// We have received enough packets to decode the block, we can stop the timer
			if !b.timer.Stop() {
				// If the timer already expired, we need to drain the channel to prevent a leak
				select {
				case <-b.timer.C:
				default:
				}
				// We also need to cancel the context to prevent goroutines waiting on the timer from leaking
				b.cancel()
			}
			blockChan <- blockID
		}
		return nil
	}

	for {
		select {
		case pkt := <-recv:
			ignore, err := d.fec.Ignore(pkt.Packet)
			if err != nil {
				d.l.Error("error checking if FEC message should be ignored", "error", err)
				continue
			}
			if ignore {
				d.l.Debug("received FEC message that should be ignored, decoding and sending to output", "src", pkt.Src.String())
				msg, err := d.fec.DecodeIgnore(pkt.Packet)
				if err != nil {
					d.l.Error("error decoding ignored FEC message", "error", err)
					continue
				}
				send <- packet.MessageWithSrc{
					MSG: msg,
					Src: pkt.Src,
				}
				continue
			}
			err = handleFECMessage(pkt)
			if err != nil {
				d.l.Error("error handling FEC message", "error", err)
				continue
			}
		case blockID := <-blockChan:
			d.l.Debug("notified to decode FEC block", "blockID", blockID)
			b, ok := blockMap[blockID]
			if !ok {
				panic(fmt.Sprintf("blockID %d not found in blockMap", blockID))
			}
			if len(b.buff) == 0 {
				panic(fmt.Sprintf("blockID %d has empty buffer", blockID))
			}

			pkts := make([]Packet, len(b.buff))
			for i, pkt := range b.buff {
				d.l.Debug("preparing message for FEC decoding", "blockID", blockID, "indexID", i)
				pkts[i] = pkt.Packet
			}

			recovered, err := d.fec.Decode(pkts)
			if err != nil {
				d.l.Error("error decoding FEC block", "error", err, "blockID", blockID)
				delete(blockMap, blockID)
				continue
			}

			// Previous code ensures that all messages in the block have the same
			// source IP, so we can just take the source IP from the first message
			src := b.buff[0].Src

			for _, msg := range recovered {
				send <- packet.MessageWithSrc{
					MSG: msg,
					Src: src,
				}
			}
			d.l.Debug("FEC block decoded successfully", "blockID", blockID)
			delete(blockMap, blockID)
		}
	}
}

type FEC interface {
	// BlockSize returns how many data packets form one FEC block
	BlockSize() int

	// RepairCount returns how many repair packets are generated per block
	RepairCount() int

	// BlockID returns the ID of the FEC block, which is used to identify which
	// packets belong to the same block
	BlockID(msg Packet) (uint64, error)

	// IndexID returns the ID of the packet within the block, which is used to
	// identify duplicate packets. The value MUST be unique for each packet within
	// the same block, but can be reused across different blocks.
	IndexID(msg Packet) (uint64, error)

	// Ignore returns true if the packet should be ignored by the FEC decoder.
	Ignore(msg Packet) (bool, error)

	// EncodeIgnore returns a FEC packet that is marked to be ignored by the FEC decoder.
	EncodeIgnore(msg packet.Message) (Packet, error)

	// DecodeIgnore returns a FEC packet that is marked to be ignored by the FEC decoder.
	DecodeIgnore(msg Packet) (packet.Message, error)

	// Encode takes data packets, returns RepairCount() repair packets.
	// If the number of data packets is less than BlockSize(), it pads with FEC
	// packets until BlockSize() is reached.
	// The padding can be zeros or have a meaningful content.
	Encode(block []packet.Message) ([]Packet, error)

	// Decode takes a mix of received data+repair packets (possibly with gaps),
	// returns the recovered BlockSize() data packets or error if unrecoverable
	Decode(received []Packet) ([]packet.Message, error)
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
			BlockSizeValue:   10,
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

func (f *nullFEC) BlockID(msg Packet) (uint64, error) {
	return 0, nil
}

func (f *nullFEC) IndexID(msg Packet) (uint64, error) {
	return 0, nil
}

func (f *nullFEC) Ignore(msg Packet) (bool, error) {
	return true, nil
}

func (f *nullFEC) EncodeIgnore(msg packet.Message) (Packet, error) {
	pkt, err := packet.WritePacket(msg)
	if err != nil {
		return nil, fmt.Errorf("error encoding message: %w", err)
	}
	return pkt, nil
}

func (f *nullFEC) DecodeIgnore(msg Packet) (packet.Message, error) {
	msgDecoded, err := packet.ReadPacket(msg)
	if err != nil {
		return nil, fmt.Errorf("error decoding message: %w", err)
	}
	return msgDecoded, nil
}

func (f *nullFEC) Encode(msgs []packet.Message) ([]Packet, error) {
	result := make([]Packet, len(msgs)+f.RepairCount())
	for i, msg := range msgs {
		var err error
		result[i], err = packet.WritePacket(msg)
		if err != nil {
			return nil, fmt.Errorf("error encoding message: %w", err)
		}
	}
	return result, nil
}

func (f *nullFEC) Decode(pkts []Packet) ([]packet.Message, error) {
	result := make([]packet.Message, 0, len(pkts))
	for _, pkt := range pkts {
		msg, err := packet.ReadPacket(pkt)
		if err != nil {
			return nil, fmt.Errorf("error decoding packet: %w", err)
		}
		result = append(result, msg)
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
	BlockID   uint64
	IndexID   uint64
	FECPacket bool
	Ignore    bool
}

func (e *semiNullFEC) parseHeader(pkt Packet) (semiNullFECHeader, error) {
	if len(pkt) < 16 {
		return semiNullFECHeader{}, fmt.Errorf("packet too short to contain FEC header: expected at least 16 bytes, got %d bytes", len(pkt))
	}

	buff := bytes.NewReader(pkt[:binary.Size(semiNullFECHeader{})])
	var header semiNullFECHeader
	if err := binary.Read(buff, binary.BigEndian, &header); err != nil {
		return semiNullFECHeader{}, fmt.Errorf("error decoding FEC header: %w", err)
	}
	return header, nil
}

func (e *semiNullFEC) encodeHeader(h semiNullFECHeader) ([]byte, error) {
	var buff bytes.Buffer
	if err := binary.Write(&buff, binary.BigEndian, h); err != nil {
		return nil, fmt.Errorf("error encoding FEC header: %w", err)
	}
	return buff.Bytes(), nil
}

func (f *semiNullFEC) BlockSize() int {
	return f.BlockSizeValue
}

func (f *semiNullFEC) RepairCount() int {
	return f.RepairCountValue
}

func (f *semiNullFEC) BlockID(pkt Packet) (uint64, error) {
	h, err := f.parseHeader(pkt)
	if err != nil {
		return 0, fmt.Errorf("error parsing FEC header: %w", err)
	}
	return h.BlockID, nil
}

func (f *semiNullFEC) IndexID(pkt Packet) (uint64, error) {
	h, err := f.parseHeader(pkt)
	if err != nil {
		return 0, fmt.Errorf("error parsing FEC header: %w", err)
	}
	return h.IndexID, nil
}

func (f *semiNullFEC) Ignore(pkt Packet) (bool, error) {
	h, err := f.parseHeader(pkt)
	if err != nil {
		return false, fmt.Errorf("error parsing FEC header: %w", err)
	}
	return h.Ignore, nil
}

func (f *semiNullFEC) EncodeIgnore(msg packet.Message) (Packet, error) {
	pkt, err := packet.WritePacket(msg)
	if err != nil {
		return nil, fmt.Errorf("error encoding message: %w", err)
	}
	header, err := f.encodeHeader(semiNullFECHeader{
		BlockID:   0,
		IndexID:   0,
		FECPacket: false,
		Ignore:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("error encoding FEC header: %w", err)
	}
	return append(header, pkt...), nil
}

func (f *semiNullFEC) DecodeIgnore(pkt Packet) (packet.Message, error) {
	h, err := f.parseHeader(pkt)
	if err != nil {
		return nil, fmt.Errorf("error parsing FEC header: %w", err)
	}
	if !h.Ignore {
		return nil, fmt.Errorf("packet is not marked to be ignored")
	}
	msg, err := packet.ReadPacket(pkt[binary.Size(semiNullFECHeader{}):])
	if err != nil {
		return nil, fmt.Errorf("error decoding message: %w", err)
	}
	return msg, nil
}

func (f *semiNullFEC) Encode(block []packet.Message) ([]Packet, error) {
	result := make([]Packet, f.BlockSize()+f.RepairCount())
	blockID := f.counter
	for i, msg := range block {
		var err error
		result[i], err = f.encodeHeader(semiNullFECHeader{
			BlockID:   blockID,
			IndexID:   uint64(i),
			FECPacket: false,
		})
		if err != nil {
			return nil, fmt.Errorf("error encoding FEC header: %w", err)
		}

		pkt, err := packet.WritePacket(msg)
		if err != nil {
			return nil, fmt.Errorf("error encoding message: %w", err)
		}
		result[i] = append(result[i], pkt...)
	}
	for i := len(block); i < f.BlockSize(); i++ {
		var err error
		result[i], err = f.encodeHeader(semiNullFECHeader{
			BlockID:   blockID,
			IndexID:   uint64(i),
			FECPacket: true,
		})
		if err != nil {
			return nil, fmt.Errorf("error encoding FEC header: %w", err)
		}
	}
	f.counter++
	return result, nil
}

func (f *semiNullFEC) Decode(pkts []Packet) ([]packet.Message, error) {
	result := make([]packet.Message, 0, len(pkts))
	var blockID *uint64
	indexIDs := make(map[uint64]struct{})
	for _, pkt := range pkts {
		h, err := f.parseHeader(pkt)
		if err != nil {
			return nil, fmt.Errorf("error parsing FEC header: %w", err)
		}

		if blockID == nil {
			blockID = &h.BlockID
		} else if *blockID != h.BlockID {
			return nil, fmt.Errorf("mismatched block ID: expected %d, got %d", *blockID, h.BlockID)
		}

		if h.FECPacket {
			// This is a padding packet, ignore it
			indexIDs[h.IndexID] = struct{}{}
			continue
		}

		if _, ok := indexIDs[h.IndexID]; ok {
			return nil, fmt.Errorf("duplicate index ID: %d", h.IndexID)
		}
		msg, err := packet.ReadPacket(pkt[binary.Size(semiNullFECHeader{}):])
		if err != nil {
			return nil, fmt.Errorf("error decoding packet: %w", err)
		}
		result = append(result, msg)
		indexIDs[h.IndexID] = struct{}{}
	}

	for i := 0; i < f.BlockSize(); i++ {
		if _, ok := indexIDs[uint64(i)]; !ok {
			return nil, fmt.Errorf("missing packet with index ID: %d", i)
		}
	}
	return result, nil
}
