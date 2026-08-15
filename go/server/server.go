package server

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sync"
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

	sendDataCh chan packet.Message
	sendNowCh  chan packet.Message
	receiveCh  chan packet.MessageWithSrc

	scheduler *scheduler

	// No mutex for sendingLimit as it's on the hotpath
	sendingLimit      atomic.Int64
	ccMutex           sync.Mutex
	sentPacketsNumber []uint64
	sentPackets       atomic.Uint64
	clientFeedbacks   map[[16]byte]*clientFeedback
	latestID          uint16
	lastLoss          time.Time
	lastLimitAtLoss   atomic.Int64
	limitedByRate     atomic.Bool

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

		sendDataCh: make(chan packet.Message, 8),
		sendNowCh:  make(chan packet.Message, 32),
		receiveCh:  make(chan packet.MessageWithSrc, 1024),

		clientFeedbacks: make(map[[16]byte]*clientFeedback),
		lastLoss:        time.Now(),

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
	fecSend := make(chan internal.Packet, 8)
	go func() {
		// TODO: check for errors and handle them properly
		panic(s.fecEncoder.Run(s.sendDataCh, fecSend))
	}()

	s.sendingLimit.Store(100) // Kbit/s

	printDataRate := true
	var printBytesSent atomic.Uint64

	var batchSizeCount atomic.Int64
	var batchSent atomic.Int64

	if printDataRate {
		intervalDatarate := 1000 // ms
		lastReset := time.Now()
		go func() {
			for {
				if time.Since(lastReset) > time.Duration(intervalDatarate)*time.Millisecond {
					kbit := float64(printBytesSent.Load()) / (1024 * (float64(intervalDatarate) / 1000)) * 8
					unit1 := "Kbit/s"
					// unit2 := "KB/s"
					if kbit > 1500 {
						kbit = kbit / 1024
						unit1 = "Mbit/s"
						// unit2 = "MB/s"
					}
					// s.l.Info("cc", "rate", fmt.Sprintf("%.2f %s", kbit, unit1), "rate2", fmt.Sprintf("%.2f %s", kbit/8, unit2), "avg batch size", float64(batchSizeCount.Load())/float64(batchSent.Load()), "batches sent", batchSent.Load())
					s.l.Info("cc", "rate", fmt.Sprintf("%.2f %s", kbit, unit1), "avg batch size", float64(batchSizeCount.Load())/float64(batchSent.Load()), "batches sent", batchSent.Load())
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

	// ------------- GRAPH -------------------
	graphChan := make(chan uint64, 1024*1024)
	go func() {
		path := "s_graph"
		f, err := os.Create(path)
		if err != nil {
			panic(fmt.Sprintf("could not open file %s: %s", path, err.Error()))
		}

		for {
			value := <-graphChan
			_, err := f.Write(fmt.Appendf(nil, "%d %d %d\n", time.Now().UnixNano(), value, s.sendingLimit.Load()))
			if err != nil {
				panic(fmt.Sprintf("could not write to file %s: %s", path, err.Error()))
			}
		}
	}()
	// ------------- GRAPH -------------------

	batchSize := 16
	batch := make([][]byte, 0, batchSize)

	sendBatch := func() {
		if len(batch) == 0 {
			return
		}
		s.sentPackets.Add(uint64(len(batch)))
		batchSizeCount.Add(int64(len(batch)))
		batchSent.Add(1)

		for i := range batch {
			printBytesSent.Add(uint64(len(batch[i])))
			graphChan <- uint64(len(batch[i]))
		}

		// for i := range batch {
		// 	if batch[i][1] == 4 {
		// 		fmt.Println("type: ", batch[i][1], "chunk_id", binary.BigEndian.Uint32(batch[i][66:70]))
		// 	}
		// }

		bytesToSend = 0
		lastReset = time.Now()
		// fmt.Println("sent packet", "time", time.Now().UnixMicro())
		err := s.multicastConn.SendBatch(batch)
		if err != nil {
			s.l.Error("error sending batch", "error", err)
		}

		batch = batch[:0]
	}

	for {
		select {
		case p := <-s.sendNowCh:
			pb, err := packet.WritePacket(p)
			if err != nil {
				s.l.Error("encoding packet", "error", err)

				continue
			}

			// Don't count the feedback request
			if p.Type() != packet.MessageTypeFeedbackRequest {
				sendBatch()
				s.sentPackets.Add(1)
			}
			err = s.multicastConn.Send(pb)
			if err != nil {
				s.l.Error("sending packet", "error", err)
			}
		case data := <-fecSend:
			mtPassed := time.Since(lastReset).Microseconds()
			sendingLimit := int64(math.Floor(float64(s.sendingLimit.Load()) * 1.2)) // For approx errors
			if (bytesToSend+int64(len(data)))*8*1e3 > sendingLimit*mtPassed {
				s.limitedByRate.Store(true)
				excessBits := (bytesToSend+int64(len(data)))*8*1e3 - sendingLimit*mtPassed
				sendBatch()
				timeToWait := time.Duration((excessBits)/sendingLimit) * time.Microsecond
				time.Sleep(timeToWait)
			} else {
				s.limitedByRate.Store(false)
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
	go s.congestionControl()

	advertisementTicker := time.NewTicker(advertisementInterval)

	// Send initial advertisement packet immediately
	adv, err := s.AdvertisementPacket()
	if err != nil {
		return fmt.Errorf("error creating advertisement packet: %w", err)
	}
	s.sendDataCh <- adv

	for {
		select {
		case <-advertisementTicker.C:
			adv, err := s.AdvertisementPacket()
			if err != nil {
				s.l.Error("error creating advertisement packet", "error", err)

				continue
			}
			s.sendNowCh <- adv

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
			panic("message type is not of type Advertisement")
		}

		h := htos(adv.Hash)
		s.l.Debug("Received advertisement packet", "src", msg.Src, "hash", h)
	case packet.MessageTypeMetadata:
		s.l.Debug("Received metadata packet")
	case packet.MessageTypeRequestLeaves:
		req, ok := msg.MSG.(*packet.RequestLeaves)
		if !ok {
			panic("message type is not of type RequestLeaves")
		}
		s.scheduler.enqueueLeavesRequest(req, msg.Src)

	case packet.MessageTypeRequestLeaf:
		req, ok := msg.MSG.(*packet.RequestLeaf)
		if !ok {
			panic("message type is not of type RequestLeaf")
		}
		s.scheduler.enqueueLeafRequest(req, msg.Src)

	case packet.MessageTypeFeedback:
		f, ok := msg.MSG.(*packet.Feedback)
		if !ok {
			panic("message is not of type Feedback")
		}

		s.handleFeedback(f, msg.Src)

	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}

	return nil
}
