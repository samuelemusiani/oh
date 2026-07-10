package server

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	maxConcurrentBigRequests   = 1
	maxConcurrentSmallRequests = 2

	oldRequestTimeout = 5 * time.Second
)

type leavesWithIP struct {
	r  *packet.RequestLeaves
	ip net.IP
}

type leafWithIP struct {
	r  *packet.RequestLeaf
	ip net.IP
}

type request struct {
	t time.Time
	f func()
}

// Request scheduler
type scheduler struct {
	// At the moment small requests are just 1 leaf, while big requests are
	// all the others. We might want to add medium requests that are leaves but
	// maybe more than 1 leaf. Like during merkle sub-tree requests

	// Number of big requests currently being sent
	nBigRequests atomic.Int32
	// Number of small requests currently being sent
	nSmallRequests atomic.Int32

	bigRequestsSemaphore   chan struct{}
	smallRequestsSemaphore chan struct{}

	bigRequestQueue   chan request
	smallRequestQueue chan request

	leavesQueue chan leavesWithIP
	leafQueue   chan leafWithIP

	l *slog.Logger

	// We probably don't need the full server pointer, but just some subfields.
	// For now, we keep it simple and use the full pointer.
	s *Server
}

func newScheduler(s *Server) *scheduler {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // drop it
			}
			return a
		},
		Level: slog.LevelDebug,
	}))

	return &scheduler{
		leavesQueue:            make(chan leavesWithIP, 64), // TODO: Do we want to make this buffered?
		leafQueue:              make(chan leafWithIP, 64),   // TODO: Do we want to make this buffered?
		bigRequestQueue:        make(chan request, 64),      // TODO: Do we want to make this buffered?
		smallRequestQueue:      make(chan request, 64),      // TODO: Do we want to make this buffered?
		bigRequestsSemaphore:   make(chan struct{}, maxConcurrentBigRequests),
		smallRequestsSemaphore: make(chan struct{}, maxConcurrentSmallRequests),

		l: logger.With("component", "scheduler"),
		s: s,
	}
}

func (s *scheduler) enqueueLeavesRequest(req *packet.RequestLeaves, src net.IP) {
	select {
	case s.leavesQueue <- leavesWithIP{r: req, ip: src}:
	default:
		s.l.Debug("Leaves request queue is full, dropping request", "hash", htos(req.Hash))
	}
}

func (s *scheduler) enqueueLeafRequest(req *packet.RequestLeaf, src net.IP) {
	select {
	case s.leafQueue <- leafWithIP{r: req, ip: src}:
	default:
		s.l.Debug("Leaf request queue is full, dropping request", "hash", htos(req.RootHash))
	}
}

func (s *scheduler) run() {
	go s.handleBigRequests()
	go s.handleSmallRequests()
	go s.sortLeavesRequests()
	go s.handleLeafRequests()
}

func (s *scheduler) sortLeavesRequests() {
	for {
		r := <-s.leavesQueue
		s.l.Debug("Received leaves request packet", "hash", htos(r.r.Hash))

		entry, ok := s.s.hashLookup[r.r.Hash]
		if !ok {
			s.s.l.Debug("Received request packet for unknown hash", "hash", htos(r.r.Hash))

			continue
		}

		switch entry.t {
		case hashTypeManifest:
			s.s.l.Debug("Received request packet for manifest hash", "hash", htos(r.r.Hash))
			// We need to send the metadata packets for all files

			s.bigRequestQueue <- request{
				t: time.Now(),
				f: func() {
					for _, f := range s.s.files {
						s.s.sendCh <- f.MetadataPacket(*s.s.currentManifestHash)
					}
					s.nBigRequests.Add(-1)
				},
			}
		case hashTypeFile:
			s.s.l.Debug("Received request packet for file hash", "hash", htos(r.r.Hash))
			file := s.s.files[entry.fileIndex]
			s.bigRequestQueue <- request{
				t: time.Now(),
				f: func() {
					for i := range file.chunks {
						s.s.l.Debug("Constructing chunk packets for file", "file", file.name, "blob_index", i)
						pkts, err := file.ChunkPacket(uint32(i))
						if err != nil {
							s.s.l.Error("error creating chunk packet", "file", file.name, "blob_index", i, "error", err)
							return
						}

						s.s.l.Debug("Sending chunk packets for file", "file", file.name, "blob_index", i, "num_packets", len(pkts))
						for _, pkt := range pkts {
							s.s.sendCh <- &pkt
						}
					}
				},
			}

		case hashTypeChunk:
			s.s.l.Debug("Received request packet for chunk hash", "hash", htos(r.r.Hash), "file_index", entry.fileIndex, "chunk_index", entry.chunkIndex)
			s.smallRequestQueue <- request{
				t: time.Now(),
				f: func() {
					fmt.Println("Sending chunk packets for file", "file_index", entry.fileIndex, "chunk_index", entry.chunkIndex)
					pkts, err := s.s.files[entry.fileIndex].ChunkPacket(uint32(entry.chunkIndex))
					if err != nil {
						s.s.l.Error("error creating chunk packet", "file_index", entry.fileIndex, "blob_index", entry.chunkIndex, "error", err)
						return
					}
					for _, pkt := range pkts {
						s.s.sendCh <- &pkt
					}
					fmt.Println("Finished sending chunk packets for file", "file_index", entry.fileIndex, "chunk_index", entry.chunkIndex)
				},
			}
		default:
			panic(fmt.Sprintf("unknown hash type for request packet: %d %v", entry.t, htos(r.r.Hash)))
		}
	}
}

func (s *scheduler) handleLeafRequests() {
	for {
		r := <-s.leafQueue
		s.l.Debug("Received leaf request packet", "hash", htos(r.r.RootHash), "index", r.r.Index)

		entry, ok := s.s.hashLookup[r.r.RootHash]
		if !ok {
			s.s.l.Debug("Received request packet for unknown hash", "hash", htos(r.r.RootHash))

			continue
		}

		switch entry.t {
		// TODO: for now we don't support sub-trees. So hash must be the manifest or
		// a file hash.
		case hashTypeManifest:
			if uint32(len(s.s.files)) < r.r.Index {
				panic(fmt.Sprintf("requested index %d is out of bounds for manifest with %d files", r.r.Index, len(s.s.files)))
			}
			s.smallRequestQueue <- request{
				t: time.Now(),
				f: func() {
					s.s.sendCh <- s.s.files[r.r.Index].MetadataPacket(*s.s.currentManifestHash)
				},
			}
		case hashTypeFile:
			file := s.s.files[entry.fileIndex]
			if r.r.Index >= uint32(len(file.chunks)) {
				// Because we don't advertise the number of chunks in the file, but only
				// the depth of the Merkle tree, we can receive requests for chunks that
				// don't exist. In this case we just ignore the request.
				continue
			}
			s.smallRequestQueue <- request{
				t: time.Now(),
				f: func() {
					pkts, err := file.ChunkPacket(r.r.Index)
					if err != nil {
						s.s.l.Error("error creating chunk packet", "file", file.name, "chunk_index", r.r.Index, "error", err)
						return
					}
					for _, pkt := range pkts {
						s.s.sendCh <- &pkt
					}
				},
			}
		default:
			panic(fmt.Sprintf("unknown hash type for request packet: %d %v", entry.t, htos(r.r.RootHash)))
		}
	}
}

func (s *scheduler) handleBigRequests() {
	for req := range s.bigRequestQueue {
		if time.Since(req.t) > oldRequestTimeout {
			s.l.Debug("Dropping old big request", "age", time.Since(req.t))
			continue
		}
		s.bigRequestsSemaphore <- struct{}{}
		s.nBigRequests.Add(1)
		go func() {
			defer func() {
				<-s.bigRequestsSemaphore
				s.nBigRequests.Add(-1)
			}()
			req.f()
		}()
	}
}

func (s *scheduler) handleSmallRequests() {
	for req := range s.smallRequestQueue {
		if time.Since(req.t) > oldRequestTimeout {
			s.l.Debug("Dropping old small request", "age", time.Since(req.t))
			continue
		}
		s.smallRequestsSemaphore <- struct{}{}
		s.nSmallRequests.Add(1)
		go func() {
			defer func() {
				<-s.smallRequestsSemaphore
				s.nSmallRequests.Add(-1)
			}()
			req.f()
		}()
	}
}
