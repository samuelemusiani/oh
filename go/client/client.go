package client

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	fecType = internal.FECTypeSemiNull

	fecWaitTimeout = 500 * time.Millisecond
)

type MessageWithSrc struct {
	MSG packet.Message
	Src net.IP
}

type Client struct {
	config        oh.Config
	multicastConn *internal.Multicast
	fec           internal.FEC

	// Directory to save files to
	dir string

	sendCh    chan packet.Message
	receiveCh chan MessageWithSrc

	l map[string]*slog.Logger
}

func New(c oh.Config) (*Client, error) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	m, err := internal.OpenMulticastConnection(c.Address(), c.Port(), c.Interface())
	if err != nil {
		return nil, fmt.Errorf("error opening multicast connection: %w", err)
	}

	return &Client{
		config:        c,
		multicastConn: m,
		fec:           internal.NewFEC(fecType),

		sendCh:    make(chan packet.Message, 1024),
		receiveCh: make(chan MessageWithSrc, 1024),

		l: makeLoggers(),
	}, nil
}

func (c *Client) ReceiveFiles(dir string) error {
	stat, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", dir)
	} else if err != nil {
		return fmt.Errorf("unable to access directory %s: %w", dir, err)
	}

	if !stat.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dir)
	}

	c.dir = dir

	if err := c.run(); err != nil {
		return fmt.Errorf("error running client: %w", err)
	}

	return nil
}

func (s *Client) sendLoop() {
	// Clients are not expected to send any FEC blocks, we can ignore the construction
	for {
		// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
		msg := <-s.sendCh
		data, err := packet.WritePacket(msg, nil)
		if err != nil {
			s.l["send"].Error("error encoding message", "error", err)
			continue
		}

		if err := s.multicastConn.Send(data); err != nil {
			s.l["send"].Error("error sending message", "error", err)
			continue
		}
	}
}

func (s *Client) receiveLoop() {
	type ReceivedMessageWithSrc struct {
		MSG packet.MessageWithFEC
		Src net.IP
	}

	// Because Receive is a blocking call, we need to run it in a separate
	// goroutine and send the received messages to the main loop via a channel
	tmpPacketBuff := make(chan ReceivedMessageWithSrc)
	go func() {
		buff := make([]byte, 65536)
		for {
			n, src, err := s.multicastConn.Receive(buff)
			if err != nil {
				s.l["receive"].Error("error receiving message", "error", err)
				continue
			}

			msg, err := packet.ReadPacket(buff[:n])
			if err != nil {
				s.l["receive"].Error("error decoding message", "error", err)
				continue
			}

			tmpPacketBuff <- ReceivedMessageWithSrc{
				MSG: msg,
				Src: src,
			}
		}
	}()

	type block struct {
		buff   []ReceivedMessageWithSrc
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
			s.l["fec"].Debug("FEC block wait timer expired, notifying block for decoding", "blockID", blockID, "random", ctx.Value("random"))
			blockChan <- blockID
		case <-ctx.Done():
			s.l["fec"].Debug("FEC block wait timer cancelled, not notifying block for decoding", "blockID", blockID, "random", ctx.Value("random"))
			return
		}
	}

	handleFECMessage := func(msg ReceivedMessageWithSrc) error {
		blockID, err := s.fec.BlockID(msg.MSG)
		if err != nil {
			return fmt.Errorf("error getting block ID from FEC header: %w", err)
		}

		indexID, err := s.fec.IndexID(msg.MSG)
		if err != nil {
			return fmt.Errorf("error getting index ID from FEC header: %w", err)
		}
		s.l["fec"].Debug("received FEC message", "blockID", blockID, "indexID", indexID, "src", msg.Src.String())

		b, ok := blockMap[blockID]
		if !ok {
			s.l["fec"].Debug("received first packet for FEC block, creating new block", "blockID", blockID)
			cancelCtx, cancel := context.WithCancel(context.Background())
			cancelCtx = context.WithValue(cancelCtx, "random", rand.Uint64())
			b = &block{
				buff:   make([]ReceivedMessageWithSrc, 0, s.fec.BlockSize()+s.fec.RepairCount()),
				index:  make(map[uint64]struct{}),
				timer:  time.NewTimer(fecWaitTimeout),
				ctx:    cancelCtx,
				cancel: cancel,
			}
			blockMap[blockID] = b
			go notifyBlock(b.timer.C, b.ctx, blockID)
		}

		if _, ok := b.index[indexID]; ok {
			s.l["fec"].Debug("duplicate packet received, ignoring", "blockID", blockID, "indexID", indexID)
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
			s.l["fec"].Debug("FEC block wait timer expired, discarding packet", "blockID", blockID)
			return nil
		} else {
			// // Need to extend the timer
			// b.cancel()
			// select {
			// case <-b.timer.C:
			// default:
			// }
			// cancelCtx, cancel := context.WithCancel(context.Background())
			// cancelCtx = context.WithValue(cancelCtx, "random", rand.Uint64())
			// b.ctx = cancelCtx
			// b.cancel = cancel
			// go notifyBlock(b.timer.C, b.ctx, blockID)
			// s.l.Debug("FEC block wait timer extended", "blockID", blockID)
		}

		// We received a new packet for this block
		b.buff = append(b.buff, msg)
		b.index[indexID] = struct{}{}

		if len(b.buff) >= s.fec.BlockSize() {
			s.l["fec"].Debug("received enough packets to decode FEC block", "blockID", blockID)
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
		case msg := <-tmpPacketBuff:
			// If no FEC just send the message on the main channel
			if msg.MSG.FEC == nil {
				s.receiveCh <- MessageWithSrc{
					MSG: msg.MSG.MSG,
					Src: msg.Src,
				}

				continue
			}
			err := handleFECMessage(msg)
			if err != nil {
				s.l["receive"].Error("error handling FEC message", "error", err)
				continue
			}
		case blockID := <-blockChan:
			s.l["fec"].Debug("notified to decode FEC block", "blockID", blockID)
			b, ok := blockMap[blockID]
			if !ok {
				panic(fmt.Sprintf("blockID %d not found in blockMap", blockID))
			}
			if len(b.buff) == 0 {
				panic(fmt.Sprintf("blockID %d has empty buffer", blockID))
			}

			msgs := make([]packet.MessageWithFEC, len(b.buff))
			for i, msg := range b.buff {
				s.l["fec"].Debug("preparing message for FEC decoding", "blockID", blockID, "indexID", i)
				msgs[i] = msg.MSG
			}

			recovered, err := s.fec.Decode(msgs)
			if err != nil {
				s.l["fec"].Error("error decoding FEC block", "error", err, "blockID", blockID)
				delete(blockMap, blockID)
				continue
			}

			// Previous code ensures that all messages in the block have the same
			// source IP, so we can just take the source IP from the first message
			src := b.buff[0].Src

			for _, msg := range recovered {
				s.receiveCh <- MessageWithSrc{
					MSG: msg,
					Src: src,
				}
			}
			s.l["fec"].Debug("FEC block decoded successfully", "blockID", blockID)
			delete(blockMap, blockID)
		}
	}
}

func (c *Client) run() error {
	go c.sendLoop()
	go c.receiveLoop()

	for {
		select {
		case msg := <-c.receiveCh:
			err := c.handleMessage(msg)
			if err != nil {
				c.l["global"].Error("error handling message", "error", err)
			}
		}
	}
}

func (s *Client) handleMessage(msg MessageWithSrc) error {
	switch msg.MSG.Type() {
	case packet.MessageTypeAdvertisement:
		adv := msg.MSG.(*packet.Advertisement)
		h := hex.EncodeToString(adv.Hash[:])
		s.l["mhandler"].Info("Received advertisement packet", "hash", h)

		s.sendCh <- s.RequestPacket(adv.Hash)

	case packet.MessageTypeMetadata:
		mtd := msg.MSG.(*packet.Metadata)
		h := hex.EncodeToString(mtd.Hash[:])
		s.l["mhandler"].Info("Received metadata packet", "hash", h, "size", mtd.Size)
	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}

	return nil
}

func makeLoggers() map[string]*slog.Logger {
	makeLogger := func(component string, level *slog.LevelVar) *slog.Logger {
		h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
			Level: level,
		})
		return slog.New(h).With("component", component)
	}

	levels := map[string]*slog.LevelVar{
		"global":   {},
		"fec":      {},
		"send":     {},
		"receive":  {},
		"mhandler": {},
	}

	loggers := map[string]*slog.Logger{
		"global":   makeLogger("global", levels["global"]),
		"fec":      makeLogger("fec", levels["fec"]),
		"send":     makeLogger("send", levels["send"]),
		"receive":  makeLogger("receive", levels["receive"]),
		"mhandler": makeLogger("mhandler", levels["mhandler"]),
	}

	// Change level at any time, takes effect immediately:
	levels["global"].Set(slog.LevelDebug)
	levels["fec"].Set(slog.LevelInfo)
	levels["send"].Set(slog.LevelDebug)
	levels["receive"].Set(slog.LevelDebug)
	levels["mhandler"].Set(slog.LevelInfo)

	return loggers
}
