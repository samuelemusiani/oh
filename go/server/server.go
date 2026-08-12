package server

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
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

	files       []*file
	filesHashes map[[32]byte]*file

	hashLookup map[[32]byte]*merkletree.MerkleTree

	currentManifest *merkletree.MerkleTree

	sendCh    chan packet.Message
	receiveCh chan packet.MessageWithSrc

	scheduler *scheduler

	l *slog.Logger
}

var (
	hashFunc = sha256.New
)

func New(c oh.Config) (*Server, error) {
	go func() {
		panic(http.ListenAndServe("127.0.0.1:6060", nil))
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

	server := Server{
		config:        c,
		multicastConn: m,
		chunker:       chunker.NewChunker(hashFunc),
		fecEncoder:    internal.NewFECEncoder(internal.NewFEC(fecType), logger),

		filesHashes: make(map[[32]byte]*file),
		hashLookup:  make(map[[32]byte]*merkletree.MerkleTree),

		sendCh:    make(chan packet.Message, 1024*1024),
		receiveCh: make(chan packet.MessageWithSrc, 1024*1024),

		l: logger,
	}
	server.scheduler = newScheduler(&server)

	return &server, nil
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
	fecSend := make(chan internal.Packet, 1024*1024)
	go func() {
		// TODO: check for errors and handle them properly
		panic(s.fecEncoder.Run(s.sendCh, fecSend))
	}()

	// TODO: This is limited because client cannot keep up
	rateLimit := int64(800)                      // MB/s
	rateLimit = int64(float64(rateLimit) * 1.06) // for approximation errors

	printDataRate := true
	var printBytesSent atomic.Uint64

	var batchSizeCount atomic.Int64
	var batchSent atomic.Int64

	if printDataRate {
		intervalDatarate := 500 // ms
		lastReset := time.Now()
		go func() {
			for {
				if time.Since(lastReset) > time.Duration(intervalDatarate)*time.Millisecond {
					mbit := float64(printBytesSent.Load()) / (1024 * 1024 * (float64(intervalDatarate) / 1000)) * 8
					s.l.Info("data rate", "rate", fmt.Sprintf("%.2f Mbit/s", mbit), "rate2", fmt.Sprintf("%.2f MB/s", mbit/8))
					s.l.Info("batch stats", "average batch size", float64(batchSizeCount.Load())/float64(batchSent.Load()), "batches sent", batchSent.Load())
					batchSizeCount.Store(0)
					batchSent.Store(0)
					printBytesSent.Store(0)
					lastReset = time.Now()
					time.Sleep(time.Duration(intervalDatarate) * time.Millisecond)
				}
			}
		}()
	}

	bytesToSend := int64(0)
	lastReset := time.Now()
	// TODO: add some sort of windowing mechanism to avoid sending too many messages at once

	batchSize := 1
	batch := make([][]byte, 0, batchSize)

	maxBatchesSent := 0

	sendBatch := func() {
		if len(batch) == 0 {
			return
		}
		batchSizeCount.Add(int64(len(batch)))
		batchSent.Add(1)

		for i := range batch {
			printBytesSent.Add(uint64(len(batch[i])))
		}

		bytesToSend = 0
		lastReset = time.Now()
		err := s.multicastConn.SendBatch(batch)
		if err != nil {
			s.l.Error("error sending batch", "error", err)
		}

		// TODO: remove when we'll have a congestion control
		if len(batch) >= batchSize {
			maxBatchesSent++
			if maxBatchesSent > 16 {
				maxBatchesSent = 0
				batchSize *= 2
				batch = make([][]byte, 0, batchSize)
			}
		}

		batch = batch[:0]
	}

	for {
		select {
		case data := <-fecSend:
			mtPassed := time.Since(lastReset).Microseconds()
			// Becuase we are using microseconds, we should count the 1e6 factor. But also we are measuring in
			// Mega Bytes, so we should count the 1024*1024 factor. Becuase 1e6 ~= 1024*1024, we can just ignore
			// the factors and use everything as is. This will result in a slightly lower rate than the
			// specified rateLimit, but it is good enough for our purposes.
			if (bytesToSend+int64(len(data)))*8 > rateLimit*mtPassed {
				sendBatch()
				timeToWait := time.Duration(((bytesToSend+int64(len(data)))*8-rateLimit*mtPassed)*8/rateLimit) * time.Microsecond
				time.Sleep(timeToWait)
			}

			bytesToSend += int64(len(data))

			batch = append(batch, data)
			if len(batch) >= batchSize {
				sendBatch()
			}
		case <-time.After(10 * time.Millisecond):
			sendBatch()
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
	go s.scheduler.run()
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

		h := htos(adv.Hash)
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
		s.scheduler.enqueueLeavesRequest(req, msg.Src)
		return nil

	case packet.MessageTypeRequestLeaf:
		req, ok := msg.MSG.(*packet.RequestLeaf)
		if !ok {
			panic("message type is request but message is not of type *packet.Request")
		}
		s.scheduler.enqueueLeafRequest(req, msg.Src)
		return nil

	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}
}
