package client

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

type Client struct {
	config        oh.Config
	multicastConn *internal.Multicast

	// Directory to save files to
	dir string

	sendCh    chan packet.Message
	receiveCh chan packet.Message

	l *slog.Logger
}

func New(c oh.Config) (*Client, error) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

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

	return &Client{
		config:        c,
		multicastConn: m,

		sendCh:    make(chan packet.Message, 100),
		receiveCh: make(chan packet.Message, 100),

		l: logger,
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

func (s *Client) receiveLoop() {
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

func (c *Client) run() error {
	go c.sendLoop()
	go c.receiveLoop()

	for {
		select {
		case msg := <-c.receiveCh:
			err := c.handleMessage(msg)
			if err != nil {
				c.l.Error("error handling message", "error", err)
			}
		}
	}
}

func (s *Client) handleMessage(msg packet.Message) error {
	switch msg.Type() {
	case packet.MessageTypeAdvertisement:
		h := hex.EncodeToString((msg.(*packet.Advertisement)).Hash[:])
		s.l.Debug("Received advertisement packet", "hash", h)
	case packet.MessageTypeMetadata:
		s.l.Debug("Received metadata packet", "metadata", msg.(*packet.Metadata))
	default:
		return fmt.Errorf("unknown message type: %d", msg.Type())
	}

	return nil
}
