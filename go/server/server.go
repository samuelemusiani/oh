package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"time"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	advertisementInterval = 1 * time.Second
)

type Server struct {
	config        oh.Config
	multicastConn *internal.Multicast
	chunker       *chunker.Chunker

	files []*file

	sendCh    chan packet.Message
	receiveCh chan packet.Message

	l *slog.Logger
}

var (
	hashFunc = sha256.New
)

func New(c oh.Config) (*Server, error) {
	m, err := internal.OpenMulticastConnection(c.Address(), c.Port(), c.Interface())
	if err != nil {
		return nil, fmt.Errorf("error opening multicast connection: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // drop it
			}
			return a
		},
		Level: slog.LevelDebug,
	}))

	return &Server{
		config:        c,
		multicastConn: m,
		chunker:       chunker.NewChunker(hashFunc()),

		sendCh:    make(chan packet.Message, 100),
		receiveCh: make(chan packet.Message, 100),

		l: logger,
	}, nil
}

func (s *Server) SendFiles(files []string) error {
	err := s.openFiles(files)
	if err != nil {
		return fmt.Errorf("error opening files: %w", err)
	}

	// Start server
	if err := s.run(); err != nil {
		return fmt.Errorf("error running server: %w", err)
	}

	s.closeFiles()

	return nil
}

func (s *Server) sendLoop() {
	for {
		// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
		msg := <-s.sendCh
		data, err := packet.WritePacket(msg)
		if err != nil {
			s.l.Error("error encoding message", "error", err)
			continue
		}

		if err := s.multicastConn.Send(data); err != nil {
			s.l.Error("error sending message", "error", err)
			continue
		}
	}
}

func (s *Server) receiveLoop() {
	buff := make([]byte, 65536)
	for {
		n, _, err := s.multicastConn.Receive(buff)
		if err != nil {
			s.l.Error("error receiving message", "error", err)
			continue
		}

		msg, err := packet.ReadPacket(buff[:n])
		if err != nil {
			s.l.Error("error decoding message", "error", err)
			continue
		}

		s.receiveCh <- msg
	}
}

func (s *Server) run() error {
	go s.sendLoop()
	go s.receiveLoop()

	advertisementTicker := time.NewTicker(advertisementInterval)
	for {
		select {
		case <-advertisementTicker.C:
			adv, err := s.AdvertisementPacket()
			if err != nil {
				s.l.Error("error creating advertisement packet", "error", err)

				continue
			}
			s.sendCh <- adv

		case msg := <-s.receiveCh:
			err := s.handleMessage(msg)
			if err != nil {
				s.l.Error("error handling message", "error", err)

				continue
			}
		}
	}
}

func (s *Server) handleMessage(msg packet.Message) error {
	switch msg.Type() {
	case packet.MessageTypeAdvertisement:
		adv := msg.(*packet.Advertisement)
		h := hex.EncodeToString(adv.Hash[:])
		s.l.Debug("Received advertisement packet", "hash", h)
	case packet.MessageTypeMetadata:
		s.l.Debug("Received metadata packet")
	case packet.MessageTypeRequest:
		req := msg.(*packet.Request)
		h := hex.EncodeToString(req.Hash[:])
		s.l.Debug("Received request packet", "hash", h)

		// TODO: Check hash if it's for metadata or blob. For now only metadata
		for _, f := range s.files {
			s.sendCh <- f.MetadataPacket()
		}

	default:
		return fmt.Errorf("unknown message type: %d", msg.Type())
	}

	return nil
}
