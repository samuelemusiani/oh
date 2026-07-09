package client

import (
	"slices"
	"time"
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

	client *Client
}

// TODO: This is simplified. Probably we don't need the whole client but
// just some subfields
func newScheduler(client *Client) *scheduler {
	return &scheduler{
		rec:              make(chan schedulerMessage),
		fileCompleteChan: make(chan [32]byte),
		client:           client,
	}
}

func (s *scheduler) run(finish chan struct{}) {
	manifestsToRequest := make(chan [32]byte)
	manifestRequestorChan := make(chan [32]byte)
	manifestCompleteChan := make(chan [32]byte)
	go s.requestor(manifestsToRequest, manifestRequestorChan, manifestCompleteChan)

	filesToRequest := make(chan [32]byte)
	filesRequestorChan := make(chan [32]byte)
	go s.requestor(filesToRequest, filesRequestorChan, s.fileCompleteChan)

	for msg := range s.rec {
		switch msg.t {
		case schedulerMessageTypeManifest:
			manifestMsg := msg.m.(schedulerManifestMessage)
			go func() {
				manifestsToRequest <- manifestMsg.h
			}()
		case schedulerMessageTypeMetadata:
			manifestRequestorChan <- msg.m.(schedulerMetadataMessage).h

		case schedulerMessageTypeRequestFiles:
			h := msg.m.(schedulerRequestFilesMessage).h
			manifestCompleteChan <- h
			s.client.metadataBuffMutex.Lock()
			metas := s.client.metadataBuff[h]
			for _, m := range metas {
				// the file requestor will consume all of the data, this is not
				// really blocking
				filesToRequest <- m.DataHash
			}
			s.client.metadataBuffMutex.Unlock()

		case schedulerMessageTypeData:
			// we only need to send the rootHash becuase the file requestor does not
			// know about leaves hashes of files. Only the chunks re-requestor will know
			filesRequestorChan <- msg.m.(schedulerDataMessage).rootHash
		case schedulerMessageTypeMerkleTree:
			panic("merkle tree message type not implemented")
		default:
			panic("unknown message type")
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
	s.rec <- schedulerMessage{
		t: schedulerMessageTypeRequestFiles,
		m: schedulerRequestFilesMessage{
			h: h,
		},
	}
}

func (s *scheduler) signalFileComplete(h [32]byte) {
	s.client.l["scheduler"].Info("Signaling file complete", "hash", htos(h))
	s.fileCompleteChan <- h
}

func (s *scheduler) requestor(toRequest chan [32]byte, requestorChan chan [32]byte, completeChan chan [32]byte) {
	reRequestTime := 5 * time.Second
	timer := time.NewTimer(1 * time.Second) // Set to max duration
	timer.Stop()                            // Stop the timer initially

	var currentHash *[32]byte
	hashesToRequest := make([][32]byte, 0)

	for {
		select {
		case h := <-toRequest:
			if currentHash != nil {
				s.client.l["scheduler"].Info("Adding hash to queue", "hash", htos(h))
				hashesToRequest = append(hashesToRequest, h)
				continue
			}
			// Need to request hash
			currentHash = &h
			s.client.l["scheduler"].Info("Requesting hash", "hash", htos(h))
			s.client.sendCh <- s.client.RequestLeavesPacket(h)
			timer.Reset(reRequestTime)

		case h := <-requestorChan:
			if currentHash != nil && *currentHash == h {
				// Received a packet for the current hash, reset the timer
				timer.Reset(reRequestTime)
			}
			// If the hash is not the current one, we can ignore it since we are
			// not requesting it

		case h := <-completeChan:
			s.client.l["scheduler"].Info("Received complete signal for hash", "hash", htos(h))
			if currentHash != nil && *currentHash == h {
				s.client.l["scheduler"].Info("Finished receiving hash", "hash", htos(h))
				// Finished receiving the current hash, move to the next one
				currentHash = nil
				if len(hashesToRequest) > 0 {
					currentHash = &hashesToRequest[0]
					hashesToRequest = hashesToRequest[1:]
					s.client.l["scheduler"].Info("Requesting next hash", "hash", htos(*currentHash))
					s.client.sendCh <- s.client.RequestLeavesPacket(*currentHash)
					timer.Reset(reRequestTime)
				} else {
					timer.Stop()
				}
			} else if indx := slices.Index(hashesToRequest, h); indx != -1 {
				s.client.l["scheduler"].Info("Removing hash from queue", "hash", htos(h))
				// Remove the hash from the queue
				hashesToRequest = slices.Delete(hashesToRequest, indx, indx+1)
			} else {
				s.client.l["scheduler"].Info("Received complete signal for unknown hash", "hash", htos(h))
			}
		case <-timer.C:
			if currentHash != nil {
				// Re-request the current hash
				s.client.sendCh <- s.client.RequestLeavesPacket(*currentHash)
				timer.Reset(reRequestTime)
			}
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
