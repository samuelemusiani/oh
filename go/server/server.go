package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	advertisementInterval = 5 * time.Second
	fecType               = internal.FECTypeNull
)

type Server struct {
	config        oh.Config
	multicastConn *internal.Multicast
	chunker       *chunker.Chunker
	fecEncoder    *internal.FECEncoder

	files []*file

	hashLookup map[[32]byte]hashLookupEntry

	currentManifestHash *[32]byte

	sendCh    chan packet.Message
	receiveCh chan packet.MessageWithSrc

	l *slog.Logger
}

type hashType uint8

type hashLookupEntry struct {
	// Type of the hash (metadata, file, blob)
	t hashType

	// Set only for file and blob hashes, to identify which file/blob the hash belongs to
	fileIndex int

	// Set only for blob hashes, to identify which blob the hash belongs to
	blobIndex int
}

const (
	hashTypeManifest hashType = iota
	hashTypeFile
	hashTypeBlob
)

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
		Level: slog.LevelInfo,
	}))

	return &Server{
		config:        c,
		multicastConn: m,
		chunker:       chunker.NewChunker(hashFunc()),
		fecEncoder:    internal.NewFECEncoder(internal.NewFEC(fecType), logger),

		hashLookup: make(map[[32]byte]hashLookupEntry),

		sendCh:    make(chan packet.Message, 1024),
		receiveCh: make(chan packet.MessageWithSrc, 1024),

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
	fecSend := make(chan internal.Packet)
	go func() {
		// TODO: check for errors and handle them properly
		panic(s.fecEncoder.Run(s.sendCh, fecSend))
	}()

	// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
	for {
		data := <-fecSend
		err := s.multicastConn.Send(data)
		if err != nil {
			s.l.Error("error sending message", "error", err)
		}
	}
}

func (s *Server) receiveLoop() {
	// As a server we are not expecting to receive any FEC messages,
	// so we can just ignore the re-construction of FEC blocks.
	buff := make([]byte, 65536)
	for {
		n, src, err := s.multicastConn.Receive(buff)
		if err != nil {
			s.l.Error("error receiving message", "error", err)
			continue
		}

		msg, err := packet.ReadPacket(buff[:n])
		if err != nil {
			s.l.Error("error decoding message", "error", err)
			continue
		}

		s.receiveCh <- packet.MessageWithSrc{
			MSG: msg,
			Src: src,
		}
	}
}

func (s *Server) run() error {
	go s.sendLoop()
	go s.receiveLoop()

	advertisementTicker := time.NewTicker(advertisementInterval)

	// Send initial advertisement packet immediately
	adv, err := s.AdvertisementPacket()
	if err != nil {
		return fmt.Errorf("error creating advertisement packet: %w", err)
	}
	s.sendCh <- adv

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

func (s *Server) handleMessage(msg packet.MessageWithSrc) error {
	switch msg.MSG.Type() {
	case packet.MessageTypeAdvertisement:
		adv, ok := msg.MSG.(*packet.Advertisement)
		if !ok {
			panic("message type is advertisement but message is not of type *packet.Advertisement")
		}

		h := hex.EncodeToString(adv.Hash[:])
		s.l.Debug("Received advertisement packet", "src", msg.Src, "hash", h)
		return nil
	case packet.MessageTypeMetadata:
		s.l.Debug("Received metadata packet")
		return nil
	case packet.MessageTypeRequestLeaves:
		req, ok := msg.MSG.(*packet.RequestLeaves)
		if !ok {
			panic("message type is request but message is not of type *packet.Request")
		}
		err := s.handleRequest(req, msg.Src)
		if err != nil {
			return fmt.Errorf("error handling request packet: %w", err)
		}

		return nil

	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}
}

func (s *Server) handleRequest(req *packet.RequestLeaves, src net.IP) error {
	rh := hex.EncodeToString(req.Hash[:])
	s.l.Debug("Received request packet", "hash", rh)

	entry, ok := s.hashLookup[req.Hash]
	if !ok {
		s.l.Debug("Received request packet for unknown hash", "hash", rh)

		return nil
	}

	switch entry.t {
	case hashTypeManifest:
		s.l.Debug("Received request packet for manifest hash", "hash", rh)
		// We need to send the metadata packets for all files
		for _, f := range s.files {
			s.sendCh <- f.MetadataPacket(*s.currentManifestHash)
		}
	case hashTypeFile:
		s.l.Debug("Received request packet for file hash", "hash", rh)
		file := s.files[entry.fileIndex]
		for i := range file.chunks {
			s.l.Debug("Constructing blob packets for file", "file", file.name, "blob_index", i)
			pkts, err := file.BlobPacket(uint32(i))
			if err != nil {
				return fmt.Errorf("error creating blob packet for file %s: %w", file.name, err)
			}

			s.l.Debug("Sending blob packets for file", "file", file.name, "blob_index", i, "num_packets", len(pkts))
			for _, pkt := range pkts {
				s.sendCh <- &pkt
			}

			time.Sleep(1 * time.Millisecond) // Sleep a bit to avoid flooding the network
		}
	case hashTypeBlob:
		s.l.Debug("Received request packet for blob hash", "hash", rh)
		panic("not implemented yet")
	default:
		panic("unknown hash type")
	}

	return nil
}
