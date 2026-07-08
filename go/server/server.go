package server

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"

	// Profiling
	"net/http"
	_ "net/http/pprof"
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

	currentlySending *[32]byte
}

type hashType uint8

type hashLookupEntry struct {
	// Type of the hash (metadata, file, blob)
	t hashType

	// Set only for file and blob hashes, to identify which file/blob the hash belongs to
	fileIndex int

	// Set only for chunk hashes, to identify which chunk the hash belongs to
	chunkIndex int
}

const (
	hashTypeManifest hashType = iota
	hashTypeFile
	hashTypeChunk
)

var (
	hashFunc = sha256.New
)

func New(c oh.Config) (*Server, error) {
	go func() {
		panic(http.ListenAndServe("localhost:6060", nil))
	}()

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

	rateLimit := int64(400 / 8) // MB/s

	printDataRate := true
	var printBytesSent atomic.Uint64
	if printDataRate {
		intervalDatarate := 500 // ms
		lastReset := time.Now()
		go func() {
			for {
				if time.Since(lastReset) > time.Duration(intervalDatarate)*time.Millisecond {
					mbit := float64(printBytesSent.Load()) / (1024 * 1024 * (float64(intervalDatarate) / 1000)) * 8
					s.l.Info("data rate", "rate", fmt.Sprintf("%.2f Mbit/s", mbit), "rate2", fmt.Sprintf("%.2f MB/s", mbit/8))
					printBytesSent.Store(0)
					lastReset = time.Now()
					time.Sleep(time.Duration(intervalDatarate) * time.Millisecond)
				}
			}
		}()
	}

	bytesSent := int64(0)
	lastReset := time.Now()
	// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
	for {
		data := <-fecSend
		bytesSent += int64(len(data))
		printBytesSent.Add(uint64(len(data))) // For data rate printing
		mtPassed := time.Since(lastReset).Microseconds()
		// Becuase we are using microseconds, we should count the 1e6 factor. But also we are measuring in
		// Mega Bytes, so we should count the 1024*1024 factor. Becuase 1e6 ~= 1024*1024, we can just ignore
		// the factors and use everything as is. This will result in a slightly lower rate than the
		// specified rateLimit, but it is good enough for our purposes.
		if bytesSent > rateLimit*mtPassed {
			timeToWait := time.Duration((bytesSent-rateLimit*mtPassed)/rateLimit) * time.Microsecond
			time.Sleep(timeToWait)
		}
		if time.Since(lastReset) > 1*time.Second {
			bytesSent = 0
			lastReset = time.Now()
		}
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
		err := s.handleLeavesRequest(req, msg.Src)
		if err != nil {
			return fmt.Errorf("error handling request packet: %w", err)
		}

		return nil

	case packet.MessageTypeRequestLeaf:
		req, ok := msg.MSG.(*packet.RequestLeaf)
		if !ok {
			panic("message type is request but message is not of type *packet.Request")
		}
		err := s.handleLeafRequest(req, msg.Src)
		if err != nil {
			return fmt.Errorf("error handling request packet: %w", err)
		}

		return nil

	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}
}

func (s *Server) handleLeavesRequest(req *packet.RequestLeaves, src net.IP) error {
	rh := hex.EncodeToString(req.Hash[:])
	s.l.Debug("Received request packet", "hash", rh)

	if s.currentlySending != nil {
		if *s.currentlySending == req.Hash {
			s.l.Debug("Already sending requested hash, ignoring request", "hash", rh)

			return nil
		}
	}

	entry, ok := s.hashLookup[req.Hash]
	if !ok {
		s.l.Debug("Received request packet for unknown hash", "hash", rh)

		return nil
	}

	if s.currentlySending != nil && entry.t != hashTypeChunk {
		// For now we only allow concurrent requests for chunks
		// TODO: change later (?)
		return nil
	}

	switch entry.t {
	case hashTypeManifest:
		s.l.Debug("Received request packet for manifest hash", "hash", rh)
		s.currentlySending = &req.Hash
		// We need to send the metadata packets for all files
		go func() {
			for _, f := range s.files {
				s.sendCh <- f.MetadataPacket(*s.currentManifestHash)
			}

			s.currentlySending = nil
		}()
	case hashTypeFile:
		s.l.Debug("Received request packet for file hash", "hash", rh)
		s.currentlySending = &req.Hash
		sendFile := func(file *file) error {
			for i := range file.chunks {
				s.l.Debug("Constructing chunk packets for file", "file", file.name, "blob_index", i)
				pkts, err := file.ChunkPacket(uint32(i))
				if err != nil {
					return fmt.Errorf("error creating blob packet for file %s: %w", file.name, err)
				}

				s.l.Debug("Sending chunk packets for file", "file", file.name, "blob_index", i, "num_packets", len(pkts))
				for _, pkt := range pkts {
					s.sendCh <- &pkt
				}
			}

			return nil
		}

		file := s.files[entry.fileIndex]
		go func() {
			err := sendFile(file)
			if err != nil {
				s.l.Error("error sending file", "file", file.name, "error", err)
			}
			s.currentlySending = nil
		}()
	case hashTypeChunk:
		s.l.Debug("Received request packet for chunk hash", "hash", rh, "file_index", entry.fileIndex, "chunk_index", entry.chunkIndex)
		go func() {
			pkts, err := s.files[entry.fileIndex].ChunkPacket(uint32(entry.chunkIndex))
			if err != nil {
				s.l.Error("error creating chunk packet", "file_index", entry.fileIndex, "blob_index", entry.chunkIndex, "error", err)
				return
			}
			for _, pkt := range pkts {
				s.sendCh <- &pkt
			}
		}()
	default:
		panic(fmt.Sprintf("unknown hash type for request packet: %d %s", entry.t, rh))
	}

	return nil
}

func (s *Server) handleLeafRequest(req *packet.RequestLeaf, src net.IP) error {
	rh := hex.EncodeToString(req.RootHash[:])

	entry, ok := s.hashLookup[req.RootHash]
	if !ok {
		s.l.Debug("Received request packet for unknown hash", "hash", rh)

		return nil
	}

	// TODO: limit number of concurrent requests, the server goes OOM :)

	// TODO: for now we don't support sub-trees. So hash must be the manifest or
	// a file hash.
	switch entry.t {
	case hashTypeManifest:
		if uint32(len(s.files)) < req.Index {
			panic(fmt.Sprintf("requested index %d is out of bounds for manifest with %d files", req.Index, len(s.files)))
		}
		go func() {
			s.sendCh <- s.files[req.Index].MetadataPacket(*s.currentManifestHash)
		}()
	case hashTypeFile:
		file := s.files[entry.fileIndex]
		if uint32(len(file.chunks)) < req.Index {
			// Because we don't advertise the number of chunks in the file, but only
			// the depth of the Merkle tree, we can receive requests for chunks that
			// don't exist. In this case we just ignore the request.
			return nil
		}
		go func() {
			pkts, err := file.ChunkPacket(req.Index)
			if err != nil {
				s.l.Error("error creating chunk packet", "file", file.name, "chunk_index", req.Index, "error", err)
				return
			}
			for _, pkt := range pkts {
				s.sendCh <- &pkt
			}
		}()
	default:
		panic(fmt.Sprintf("unknown hash type for request packet: %d %s", entry.t, rh))
	}

	return nil
}
