package client

import (
	"fmt"
	"slices"
	"time"

	merkletree "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/merkle_tree"
)

const (
	schedulerMessageTypeManifest = iota
	schedulerMessageTypeMetadata
	schedulerMessageTypeData
	schedulerMessageTypeMerkleTree
	schedulerMessageTypeRequestFiles
)

type schedulerManifestMessage struct {
	h [32]byte // manifest hash
}

type schedulerMetadataMessage struct {
	h [32]byte // manifest hash that is inside the metadata packet
}

type schedulerRequestFilesMessage struct {
	h [32]byte // manifest hash that we have to use to request files
}

type schedulerDataMessage struct {
	rootHash [32]byte
	leafHash [32]byte
}

type schedulerMerkleTreeMessage struct {
	rootHash [32]byte
	depth    uint8
	leaves   []byte
}

type schedulerMessage struct {
	t uint8 // type of message
	m any
}

// scheduler is responsible for sending requests and re-tries in order to
// receive everything from the server(s).
type scheduler struct {
	// Channel to receive messages from the client
	rec chan schedulerMessage

	// channel to signal a file is complete
	fileCompleteChan chan [32]byte

	// channel to signal the completion of a (porition) of the a manifest
	manifestCompleteChan chan [32]byte

	client *Client
}

// TODO: This is simplified. Probably we don't need the whole client but
// just some subfields
func newScheduler(client *Client) *scheduler {
	return &scheduler{
		rec:                  make(chan schedulerMessage, 1024*1024),
		fileCompleteChan:     make(chan [32]byte),
		manifestCompleteChan: make(chan [32]byte),
		client:               client,
	}
}

func (s *scheduler) run(finish chan struct{}) {
	// TODO: provably the requestor for manifests and clients could be merged :)
	manifestsToRequest := make(chan *merkletree.MerkleTree)
	manifestRequestorChan := make(chan [32]byte)
	manifestMerkleTreeChan := make(chan schedulerMerkleTreeMessage)
	manifestCheckFinish := make(chan struct{})
	manifestFinish := make(chan bool)
	go s.mtRequestor(manifestsToRequest, manifestRequestorChan, s.manifestCompleteChan, manifestMerkleTreeChan, manifestCheckFinish, manifestFinish)

	filesToRequest := make(chan *merkletree.MerkleTree)
	filesRequestorChan := make(chan [32]byte)
	filesMerkleTreeChan := make(chan schedulerMerkleTreeMessage)
	filesCheckFinish := make(chan struct{})
	filesFinish := make(chan bool)
	go s.mtRequestor(filesToRequest, filesRequestorChan, s.fileCompleteChan, filesMerkleTreeChan, filesCheckFinish, filesFinish)

	for {
		select {
		case msg := <-s.rec:
			switch msg.t {
			case schedulerMessageTypeManifest:
				manifestMsg := msg.m.(schedulerManifestMessage)
				go func() {
					s.client.treesMapMutex.Lock()
					mt := s.client.trees[manifestMsg.h]
					if mt.t != merkleTreeMetadata {
						panic("mt is not of right type")
					}
					s.client.treesMapMutex.Unlock()
					manifestsToRequest <- mt.mt
				}()
			case schedulerMessageTypeMetadata:
				manifestRequestorChan <- msg.m.(schedulerMetadataMessage).h

			case schedulerMessageTypeRequestFiles:
				h := msg.m.(schedulerRequestFilesMessage).h
				s.client.treesMapMutex.Lock()
				mt := s.client.trees[h].mt
				s.client.treesMapMutex.Unlock()
				filesToRequest <- mt
			case schedulerMessageTypeData:
				// we only need to send the rootHash becuase the file requestor does not
				// know about leaves hashes of files. Only the chunks re-requestor will know
				filesRequestorChan <- msg.m.(schedulerDataMessage).rootHash
			case schedulerMessageTypeMerkleTree:
				mmt := msg.m.(schedulerMerkleTreeMessage)
				s.client.l["scheduler"].Debug("Received merkle tree", "root_hash", htos(mmt.rootHash), "depth", mmt.depth, "leaves_len", len(mmt.leaves))

				s.client.treesMapMutex.Lock()
				t, ok := s.client.trees[mmt.rootHash]
				if !ok {
					s.client.l["scheduler"].Error("Received merkle tree for unknown tree", "root_hash", htos(mmt.rootHash))
					s.client.treesMapMutex.Unlock()
					continue
				}
				s.client.treesMapMutex.Unlock()

				switch t.t {
				case merkleTreeFile:
					filesMerkleTreeChan <- mmt
				case merkleTreeMetadata:
					manifestMerkleTreeChan <- mmt
				default:
					panic("unknown merkle tree type")
				}

			default:
				panic("unknown message type")
			}
		case <-time.After(2 * time.Second):
			s.client.l["scheduler"].Debug("Checking if manifest and files requestors are finished")
			filesCheckFinish <- struct{}{}
			if <-filesFinish {
				manifestCheckFinish <- struct{}{}
				if <-manifestFinish {
					s.client.l["scheduler"].Info("Scheduler finished")
					finish <- struct{}{}
					return
				}
			}
		}
	}
}

func (s *scheduler) requestManifest(h [32]byte) {
	s.rec <- schedulerMessage{
		t: schedulerMessageTypeManifest,
		m: schedulerManifestMessage{
			h: h,
		},
	}
}

func (s *scheduler) signalManifestComplete(h [32]byte) {
	s.client.l["scheduler"].Info("Signaling manifest complete", "hash", htos(h))
	s.manifestCompleteChan <- h
}

func (s *scheduler) signalFileComplete(h [32]byte) {
	s.client.l["scheduler"].Info("Signaling file complete", "hash", htos(h))
	s.fileCompleteChan <- h
}

func (s *scheduler) requestFile(h [32]byte) {
	s.rec <- schedulerMessage{
		t: schedulerMessageTypeRequestFiles,
		m: schedulerRequestFilesMessage{
			h: h,
		},
	}
}

// This is very similar to a requestor, but is based on a merkle tree.
func (s *scheduler) mtRequestor(toRequest chan *merkletree.MerkleTree, requestorChan chan [32]byte, completeChan chan [32]byte, merkleTreeChan chan schedulerMerkleTreeMessage, checkFinish chan struct{}, finish chan bool) {
	reRequestTime := 30 * time.Second
	timer := time.NewTimer(0)
	timer.Stop() // Stop the timer initially

	guessedHeights := make(map[[32]byte]bool)

	var currentMT *merkletree.MerkleTree
	mtsToRequest := make([]*merkletree.MerkleTree, 0)

	var currentHash *[32]byte

	atLeastOneRequest := false

	queryNextHashOrAdvance := func() {
		if currentMT == nil {
			if len(mtsToRequest) == 0 {
				return
			}
			currentMT = mtsToRequest[0]
			mtsToRequest = mtsToRequest[1:]
		}

		for {
			h, ok := currentMT.NextHashToQuery()
			if !ok {
				if len(mtsToRequest) > 0 {
					currentMT = mtsToRequest[0]
					mtsToRequest = mtsToRequest[1:]
					continue
				} else {
					timer.Stop()
					currentHash = nil
					currentMT = nil

					return
				}
			}
			currentHash = (*[32]byte)(h)
			s.client.l["scheduler"].Info("Requesting next hash", "hash", htos(*currentHash))
			s.client.sendCh <- s.client.RequestLeavesPacket(*currentHash)
			timer.Reset(reRequestTime)
			return
		}
	}

	for {
		select {
		case mt := <-toRequest:
			atLeastOneRequest = true
			if currentMT != nil {
				s.client.l["scheduler"].Info("Adding mt to queue", "hash", htos([32]byte(mt.Root())))
				mtsToRequest = append(mtsToRequest, mt)
				continue
			}

			if mt == nil {
				panic("received nil merkle tree")
			}
			// Need to request hash
			currentMT = mt
			s.client.l["scheduler"].Info("Requesting hash", "hash", htos([32]byte(mt.Root())))

			queryNextHashOrAdvance()

		case h := <-requestorChan:
			if currentMT != nil && *currentHash == h {
				// Received a packet for the current hash, reset the timer
				timer.Reset(reRequestTime)
			}
			// If the hash is not the current one, we can ignore it since we are
			// not requesting it

		case h := <-completeChan:
			s.client.l["scheduler"].Info("Received complete signal for hash", "hash", htos(h))
			if currentMT != nil && *currentHash == h {
				s.client.l["scheduler"].Info("Finished receiving hash", "hash", htos(h))
				// Finished receiving the current hash, move to the next one
				queryNextHashOrAdvance()

			} else if indx := slices.IndexFunc(mtsToRequest, func(mt *merkletree.MerkleTree) bool { return [32]byte(mt.Root()) == h }); indx != -1 { // TODO: Does this make sens to have
				s.client.l["scheduler"].Info("Removing hash from queue", "hash", htos(h))
				// Remove the hash from the queue
				mtsToRequest = slices.Delete(mtsToRequest, indx, indx+1)
			} else {
				s.client.l["scheduler"].Info("Received complete signal for unknown hash", "hash", htos(h))
			}
		case msg := <-merkleTreeChan:
			if msg.rootHash != *currentHash {
				// We ignore the merkle tree if it's not the current one that we are requesting
				continue
			}
			fmt.Println("received valid MT message")

			// For merkle trees of files we can just put the nodes because we know
			// the total height. For merkle trees of metadata we cannot do this and
			// we have to guess the height

			s.client.treesMapMutex.Lock()
			currentMTtype := s.client.trees[[32]byte(currentMT.Root())].t
			s.client.treesMapMutex.Unlock()

			if currentMTtype == merkleTreeMetadata && !guessedHeights[[32]byte(currentMT.Root())] {
				h, err := merkletree.FindMTHeight(currentMT.Root(), 1<<msg.depth, msg.leaves, hashFunc())
				if err != nil {
					panic(fmt.Sprintf("error finding height for merkle tree: %s", err.Error()))
				}

				currentMT.Grow(1 << h)
				currentMT.DisableGrow()
				guessedHeights[[32]byte(currentMT.Root())] = true
			}

			err := currentMT.PutNodes(msg.rootHash[:], msg.depth, msg.leaves)
			if err != nil {
				s.client.l["scheduler"].Error("cannot put nodes into merkle tree", "error", err)

				continue
			}

			// empty nodes should be marked as valid with valid childs
			h := hashFunc()
			h.Reset()
			h.Write([]byte{0})
			emptyHash := h.Sum(nil) // empty node hash
			indexes, ok := currentMT.FindNodeIndexesFromHash(emptyHash)
			if ok {
				for _, i := range indexes {
					currentMT.MarkNodeAsValidWithValidChilds(i)
				}
			}

			nodes, err := currentMT.GetNodesBasedOnRootHash(msg.rootHash[:], msg.depth)
			if err != nil {
				panic("cannot get nodes just inserted")
			}

			s.client.treesMapMutex.Lock()
			s.client.filesMapMutex.Lock()
			for _, n := range nodes {
				if slices.Equal(n.Hash(), emptyHash) {
					// skip empty nodes
					continue
				}
				s.client.trees[[32]byte(n.Hash())] = mtType{
					mt: currentMT,
					t:  s.client.trees[[32]byte(currentMT.Root())].t,
				}
				s.client.filesMap[[32]byte(n.Hash())] = s.client.filesMap[[32]byte(currentMT.Root())]
			}
			s.client.filesMapMutex.Unlock()
			s.client.treesMapMutex.Unlock()

			queryNextHashOrAdvance()

		case <-timer.C:
			if currentMT == nil {
				continue
			}

			h, ok := currentMT.NextHashToQuery()
			if !ok {
				if !s.client.filesMap[[32]byte(currentMT.Root())].isFinished {
					panic("no hash to query and file is not complete")
				}
				// file is complete

				if len(mtsToRequest) > 0 {
					currentMT = mtsToRequest[0]
					mtsToRequest = mtsToRequest[1:]
					h, ok = currentMT.NextHashToQuery()
					if !ok {
						panic("no hash to query") // Is this possible?
					}
				} else {
					timer.Stop()
					currentHash = nil
					currentMT = nil

					continue
				}
			}

			if currentHash != (*[32]byte)(h) {
				currentHash = (*[32]byte)(h) // TODO: maybe we can just assign?
			}

			// This branch is needed when we already requested a file, but the server
			// decided to server another request. The file that we requests so didn't
			// get send. If the server sends our file, this branch is no more needed
			// as the chunkHandler already takes care of re-requesting missing chunks
			// in the file. So it does not make sens to re-request the entire file just
			// becuase we are missing some chunks.
			s.client.filesMapMutex.Lock()
			file, ok := s.client.filesMap[[32]byte(currentMT.Root())]
			s.client.filesMapMutex.Unlock()
			if ok && file.receivedAtLeastOne {
				continue
			}

			// Re-request the current hash
			s.client.l["scheduler"].Info("re-requesting next hash from TIMER branch", "hash", htos(*currentHash))
			s.client.sendCh <- s.client.RequestLeavesPacket(*currentHash)
			timer.Reset(reRequestTime)

		case <-checkFinish:
			if currentMT != nil || len(mtsToRequest) > 0 || !atLeastOneRequest {
				finish <- false
				continue
			}
			s.client.l["scheduler"].Info("No more hashes to request, finishing")
			finish <- true
			return
		}
	}
}

func (s *scheduler) reRequest(h [32]byte) {
	// TODO: Now a simple send. Could be improved in the future
	s.client.sendCh <- s.client.RequestLeavesPacket(h)
}

func (s *scheduler) requestLeafByIndex(rootHash [32]byte, leafIndex uint32) {
	// TODO: Now a simple send. Could be improved in the future
	// Expecially to avoid sending to many requests
	s.client.sendCh <- s.client.RequestLeafPacket(leafIndex, rootHash)
}
