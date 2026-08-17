package server

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	maxConcurrentBigRequests   = 1
	maxConcurrentSmallRequests = 10

	// These numbers should be adjusted based on the number of leaves.
	// But they can be lowered to test merkle trees :)
	maxLeavesForManifest = 1 << 31
	maxLeavesForFiles    = 1 << 31

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
	// TODO: At the moment small requests are just 1 leaf, while big requests are
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

	sentMTForManifest bool

	l *slog.Logger

	// TODO: We probably don't need the full server pointer, but just some subfields.
	// For now, we keep it simple and use the full pointer.
	s *Server

	// De-duplication of requests:
	dedupLeavesRWMutex sync.RWMutex
	dedupLeaves        map[packet.RequestLeaves]*schedulerDedup
	dedupLeafRWMutex   sync.RWMutex
	dedupLeaf          map[packet.RequestLeaf]*schedulerDedup
}

type schedulerDedup struct {
	enqueued        time.Time
	sending         bool
	finishedSending time.Time
}

func newScheduler(s *Server) *scheduler {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // drop it
			}
			return a
		},
		Level: slog.LevelInfo,
	}))

	return &scheduler{
		leavesQueue:            make(chan leavesWithIP, 64), // TODO: Do we want to make this buffered?
		leafQueue:              make(chan leafWithIP, 64),   // TODO: Do we want to make this buffered?
		bigRequestQueue:        make(chan request, 64),      // TODO: Do we want to make this buffered?
		smallRequestQueue:      make(chan request, 64),      // TODO: Do we want to make this buffered?
		bigRequestsSemaphore:   make(chan struct{}, maxConcurrentBigRequests),
		smallRequestsSemaphore: make(chan struct{}, maxConcurrentSmallRequests),

		dedupLeaves: make(map[packet.RequestLeaves]*schedulerDedup),
		dedupLeaf:   make(map[packet.RequestLeaf]*schedulerDedup),

		l: logger.With("component", "scheduler"),
		s: s,
	}
}

func (s *scheduler) enqueueLeavesRequest(req *packet.RequestLeaves, src net.IP) {
	s.dedupLeavesRWMutex.RLock()
	dedup, ok := s.dedupLeaves[*req]
	s.dedupLeavesRWMutex.RUnlock()
	if ok {
		if dedup.sending {
			return
		}
		if time.Since(dedup.finishedSending).Seconds() < 10 {
			return
		}
		if time.Since(dedup.enqueued).Seconds() < 15 {
			return
		}
	} else {
		dedup = &schedulerDedup{}
		s.dedupLeavesRWMutex.Lock()
		s.dedupLeaves[*req] = dedup
		s.dedupLeavesRWMutex.Unlock()
	}

	select {
	case s.leavesQueue <- leavesWithIP{r: req, ip: src}:
		dedup.enqueued = time.Now()
	default:
		s.l.Debug("Leaves request queue is full, dropping request", "hash", htos(req.Hash))
	}
}

func (s *scheduler) enqueueLeafRequest(req *packet.RequestLeaf, src net.IP) {
	s.dedupLeafRWMutex.RLock()
	dedup, ok := s.dedupLeaf[*req]
	s.dedupLeafRWMutex.RUnlock()
	if ok {
		if dedup.sending {
			return
		}
		if time.Since(dedup.finishedSending).Seconds() < 10 {
			return
		}
		if time.Since(dedup.enqueued).Seconds() < 15 {
			return
		}
	} else {
		dedup = &schedulerDedup{}
		s.dedupLeafRWMutex.Lock()
		s.dedupLeaf[*req] = dedup
		s.dedupLeafRWMutex.Unlock()
	}

	select {
	case s.leafQueue <- leafWithIP{r: req, ip: src}:
		dedup.enqueued = time.Now()
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

		mt, ok := s.s.hashLookup[r.r.Hash]
		if !ok {
			s.l.Debug("Received request packet for unknown hash", "hash", htos(r.r.Hash))

			continue
		}

		setDedupSendingLeaves := func() {
			s.dedupLeavesRWMutex.RLock()
			dedup, ok := s.dedupLeaves[*r.r]
			s.dedupLeavesRWMutex.RUnlock()
			if !ok {
				return
			}

			dedup.sending = true
		}

		setDedupStopSendingLeaves := func() {
			s.dedupLeavesRWMutex.RLock()
			dedup, ok := s.dedupLeaves[*r.r]
			s.dedupLeavesRWMutex.RUnlock()
			if !ok {
				return
			}

			dedup.sending = false
			dedup.finishedSending = time.Now()
		}

		switch {
		// TODO: Maybe just check the pointers? In theory we only have 1 object in
		// memeory for the merkle manifest tree
		case bytes.Equal(mt.Root(), s.s.currentManifest.Root()):
			s.l.Debug("Received request packet for manifest tree", "root", htos([32]byte(mt.Root())), "requested", htos(r.r.Hash))
			// We need to send the metadata packets

			// Get number of leaves
			// Check if number is greater than MAX (?)
			// If lower send leaves -> need to get leves abs indexes from root
			// If greater send merkle tree :)
			nleaves, err := mt.NumberOfLeavesFromRootHash(r.r.Hash[:])
			if err != nil {
				panic("cannot calculate number of leaves from root hash")
			}

			s.l.Debug("number of leaves", "leaves", nleaves)

			// For the server we send the merkle tree packet for the manifest if 2 cases:
			// 1. The first one is the same as for files: if the number of leaves is greater
			// 		than the maxLeavesForManifest constant.
			// 2. The second case is to help clients. Because for metadata we do not
			// 		send the index of them inside the tree (we do it for chunks in files),
			//		the clients have not reliable way to determine how to construct the
			//		tree beucase they don't know where to put the metadata.
			//		So if we sent a MT message and the client is trying to reconstruct
			//		the tree, we everything up to the leaves. This helps the client because
			//		after receiving one metadata, with the hash, it can determine the
			// 		location in the merkle tree
			if nleaves > maxLeavesForManifest || (nleaves != 1 && s.sentMTForManifest) {
				s.sentMTForManifest = true
				// number of leaves too large, we send a merkle tree

				s.smallRequestQueue <- request{
					t: time.Now(),
					f: func() {
						setDedupSendingLeaves()
						defer setDedupStopSendingLeaves()

						pkt, err := MerkleTreePacket(mt, r.r.Hash)
						if err != nil {
							s.l.Error("error creating merkle tree packet", "error", err)
							return
						}
						s.s.sendDataCh <- pkt
					},
				}

				continue
			}

			s.l.Debug("sending all the metadata packets for hash", "hash", htos(r.r.Hash))
			leavesIndexes, err := mt.GetAbsoluteLevesIndexesFromRootHash(r.r.Hash[:])
			if err != nil {
				panic("cannot get absolute leaves indexes")
			}

			s.bigRequestQueue <- request{
				t: time.Now(),
				f: func() {
					setDedupSendingLeaves()
					defer setDedupStopSendingLeaves()

					for _, i := range leavesIndexes {
						if i >= uint32(len(s.s.files)) {
							break
						}
						// TODO: Based on the RFC, we should always put the manifest hash
						// inside the metadata packet, even if the queried hash was a sub-tree.
						// Probably we could have thought this better...
						s.s.sendDataCh <- s.s.files[i].MetadataPacket([32]byte(mt.Root()))
					}
					s.nBigRequests.Add(-1)
				},
			}
		// In this case the default is for files. If streams were implemented then
		// we would have another case
		default:
			s.l.Debug("Received request packet for file hash", "hash", htos(r.r.Hash))

			nleaves, err := mt.NumberOfLeavesFromRootHash(r.r.Hash[:])
			if err != nil {
				panic("cannot calculate number of leaves from root hash")
			}

			s.l.Debug("number of leaves", "leaves", nleaves)

			if nleaves > maxLeavesForFiles {
				// number of leaves too large, we send a merkle tree
				s.smallRequestQueue <- request{
					t: time.Now(),
					f: func() {
						setDedupSendingLeaves()
						defer setDedupStopSendingLeaves()

						pkt, err := MerkleTreePacket(mt, r.r.Hash)
						if err != nil {
							s.l.Error("error creating merkle tree packet", "error", err)
							return
						}
						s.s.sendDataCh <- pkt
					},
				}

				continue
			}

			s.l.Debug("sending all the chunks for hash", "hash", htos(r.r.Hash))
			leavesIndexes, err := mt.GetAbsoluteLevesIndexesFromRootHash(r.r.Hash[:])
			if err != nil {
				panic("cannot get absolute leaves indexes")
			}

			s.l.Debug("d1", "leavesIndexes", leavesIndexes)
			file := s.s.filesHashes[[32]byte(mt.Root())]
			s.l.Debug("d2", "file", htos([32]byte(file.mt.Root())))

			r := request{
				t: time.Now(),
				f: func() {
					setDedupSendingLeaves()
					defer setDedupStopSendingLeaves()

					for j, i := range leavesIndexes {
						if i >= uint32(len(file.chunks)) {
							s.l.Debug("d3", "leavesIndexes", leavesIndexes, "len", len(file.chunks))
							break
						}
						pkts, err := file.ChunkPacketWithSubtree(uint64(i), r.r.Hash, uint32(j))
						if err != nil {
							s.l.Error("error creating chunk packet", "file", file.name, "blob_index", i, "error", err)
							return
						}

						s.l.Debug("Sending chunk packets for file", "file", file.name, "blob_index", i, "num_packets", len(pkts))
						for _, pkt := range pkts {
							s.s.sendDataCh <- &pkt
						}
					}
				},
			}
			// If leavesIndexes are not many (1) we classify this as a small request
			if len(leavesIndexes) > 1 {
				s.bigRequestQueue <- r
			} else {
				s.smallRequestQueue <- r
			}
		}
	}
}

func (s *scheduler) handleLeafRequests() {
	for {
		r := <-s.leafQueue
		s.l.Debug("Received leaf request packet", "hash", htos(r.r.RootHash), "index", r.r.Index)

		mt, ok := s.s.hashLookup[r.r.RootHash]
		if !ok {
			s.l.Debug("Received request packet for unknown hash", "hash", htos(r.r.RootHash))

			continue
		}

		setDedupSendingLeaf := func() {
			s.dedupLeafRWMutex.RLock()
			dedup, ok := s.dedupLeaf[*r.r]
			s.dedupLeafRWMutex.RUnlock()
			if !ok {
				return
			}

			dedup.sending = true
		}

		setDedupStopSendingLeaf := func() {
			s.dedupLeafRWMutex.RLock()
			dedup, ok := s.dedupLeaf[*r.r]
			s.dedupLeafRWMutex.RUnlock()
			if !ok {
				return
			}

			dedup.sending = false
			dedup.finishedSending = time.Now()
		}

		switch {
		// TODO: Maybe just check the pointers? In theory we only have 1 object in
		// memeory for the merkle manifest tree
		case bytes.Equal(mt.Root(), s.s.currentManifest.Root()):
			if uint32(len(s.s.files)) < r.r.Index {
				panic(fmt.Sprintf("requested index %d is out of bounds for manifest with %d files", r.r.Index, len(s.s.files)))
			}
			s.smallRequestQueue <- request{
				t: time.Now(),
				f: func() {
					setDedupSendingLeaf()
					defer setDedupStopSendingLeaf()

					s.s.sendDataCh <- s.s.files[r.r.Index].MetadataPacket([32]byte(mt.Root()))
				},
			}
		// In this case the default is for files. If streams were implemented then
		// we would have another case
		default:
			file := s.s.filesHashes[[32]byte(mt.Root())]
			if r.r.Index >= uint32(len(file.chunks)) {
				// Because we don't advertise the number of chunks in the file, but only
				// the depth of the Merkle tree, we can receive requests for chunks that
				// don't exist. In this case we just ignore the request.
				continue
			}
			s.smallRequestQueue <- request{
				t: time.Now(),
				f: func() {
					setDedupSendingLeaf()
					defer setDedupStopSendingLeaf()

					pkts, err := file.ChunkPacket(r.r.Index)
					if err != nil {
						s.l.Error("error creating chunk packet", "file", file.name, "chunk_index", r.r.Index, "error", err)
						return
					}
					for _, pkt := range pkts {
						s.s.sendDataCh <- &pkt
					}
				},
			}
		}
	}
}

func (s *scheduler) handleBigRequests() {
	for req := range s.bigRequestQueue {
		s.l.Debug("Trying to schedule big request")
		s.bigRequestsSemaphore <- struct{}{}

		if time.Since(req.t) > oldRequestTimeout {
			<-s.bigRequestsSemaphore
			s.l.Debug("Dropping old big request", "age", time.Since(req.t))
			continue
		}

		s.nBigRequests.Add(1)

		s.l.Debug("BIG REQUEST SCHEDULED")
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
		s.smallRequestsSemaphore <- struct{}{}

		if time.Since(req.t) > oldRequestTimeout {
			<-s.smallRequestsSemaphore
			s.l.Debug("Dropping old small request", "age", time.Since(req.t))
			continue
		}

		s.nSmallRequests.Add(1)

		s.l.Debug("SMALL REQUEST SCHEDULED")
		go func() {
			defer func() {
				<-s.smallRequestsSemaphore
				s.nSmallRequests.Add(-1)
			}()
			req.f()
		}()
	}
}
