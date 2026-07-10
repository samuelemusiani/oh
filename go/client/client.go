package client

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"

	// Profiling
	"net/http"
	_ "net/http/pprof"
)

const (
	fecType = internal.FECTypeNull
)

var (
	hashFunc = sha256.New
)

type Client struct {
	config        oh.Config
	multicastConn *internal.Multicast
	fecDecoder    *internal.FECDecoder
	chunker       *chunker.Chunker

	// Directory to save files to
	dir string

	metadataBuff      map[[32]byte][]*packet.Metadata
	metadataBuffMutex sync.Mutex
	filesMap          map[[32]byte]*file
	filesMapMutex     sync.Mutex

	sendCh    chan packet.Message
	receiveCh chan packet.MessageWithSrc

	scheduler *scheduler

	l map[string]*slog.Logger
}

func New(c oh.Config) (*Client, error) {
	go func() {
		panic(http.ListenAndServe("localhost:6060", nil))
	}()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	m, err := internal.OpenMulticastConnection(c.Address(), c.Port(), c.Interface())
	if err != nil {
		return nil, fmt.Errorf("error opening multicast connection: %w", err)
	}

	l := makeLoggers()

	client := &Client{
		config:        c,
		multicastConn: m,
		fecDecoder:    internal.NewFECDecoder(internal.NewFEC(fecType), l["fec"]),
		chunker:       chunker.NewChunker(hashFunc),

		metadataBuff: make(map[[32]byte][]*packet.Metadata),
		filesMap:     make(map[[32]byte]*file),

		sendCh:    make(chan packet.Message, 1024),
		receiveCh: make(chan packet.MessageWithSrc, 1024),

		l: l,
	}

	client.scheduler = newScheduler(client)

	return client, nil
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

func (c *Client) sendLoop() {
	// Clients are not expected to send any FEC blocks, we can ignore the construction

	batchSize := 1024
	batch := make([][]byte, 0, batchSize)

	sendBatch := func() {
		if len(batch) == 0 {
			return
		}
		err := c.multicastConn.SendBatch(batch)
		if err != nil {
			c.l["send"].Error("error sending batch", "error", err)
		}
		batch = batch[:0]
	}

	var packetsSent atomic.Int64

	var leavesRequested atomic.Int64
	var leafRequested atomic.Int64

	go func() {
		for {
			fmt.Println("Packets sent:", packetsSent.Load(), "leaves", leavesRequested.Load(), "leaf", leafRequested.Load())
			packetsSent.Store(0)
			leavesRequested.Store(0)
			leafRequested.Store(0)
			time.Sleep(1 * time.Second)
		}
	}()

	for {
		// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
		select {
		case msg := <-c.sendCh:
			switch msg.Type() {
			case packet.MessageTypeRequestLeaves:
				leavesRequested.Add(1)
			case packet.MessageTypeRequestLeaf:
				leafRequested.Add(1)
			}
			data, err := packet.WritePacket(msg)
			if err != nil {
				c.l["send"].Error("error encoding message", "error", err)
				continue
			}
			packetsSent.Add(1)

			batch = append(batch, data)
			if len(batch) >= batchSize {
				sendBatch()
			}
		case <-time.After(20 * time.Millisecond):
			sendBatch()
		}
	}
}

func (c *Client) receiveLoop() {
	tmpPacketBuff := make(chan internal.PacketWithSrc)
	go func() {
		panic(c.fecDecoder.Run(tmpPacketBuff, c.receiveCh))
	}()

	batchSize := 32
	buff := make([][]byte, batchSize)
	for i := range buff {
		buff[i] = make([]byte, 2048) // Just to make sure :)
	}
	for {
		n, srcs, err := c.multicastConn.ReceiveBatch(buff)
		if err != nil {
			c.l["receive"].Error("error receiving message", "error", err)
			continue
		}
		for i := range n {
			copyBuff := make([]byte, len(buff[i]))
			copy(copyBuff, buff[i])
			tmpPacketBuff <- internal.PacketWithSrc{
				Packet: copyBuff,
				Src:    srcs[i],
			}
			buff[i] = buff[i][:cap(buff[i])] // Reset the buffer slice to the original size
		}
		buff = buff[:batchSize] // Reset the buffer slice to the original size
	}
}

func (c *Client) run() error {
	finish := make(chan struct{})
	go c.sendLoop()
	go c.receiveLoop()
	go c.scheduler.run(finish)

	for {
		select {
		case msg := <-c.receiveCh:
			err := c.handleMessage(msg)
			if err != nil {
				c.l["global"].Error("error handling message", "error", err)
			}

		case <-finish:
			// Check that files are received correctly
			wg := sync.WaitGroup{}
			for h := range c.filesMap {
				wg.Go(func() {
					mt, err := c.merkleTreeForFileFromFile(h)
					if err != nil {
						c.l["global"].Error("error getting merkle tree for file", "hash", htos(h), "error", err)
						return
					}
					if !bytes.Equal(mt.Root(), h[:]) {
						c.l["global"].Error("file hash does not match merkle tree root", "hash", htos(h), "merkle_root", htos([32]byte(mt.Root())))
					}
				})
			}
			wg.Wait()

			c.l["global"].Info("All files received, exiting")
			return nil
		}
	}
}

func (c *Client) handleMessage(msg packet.MessageWithSrc) error {
	switch msg.MSG.Type() {
	case packet.MessageTypeAdvertisement:
		adv := msg.MSG.(*packet.Advertisement)
		h := htos(adv.Hash)
		c.l["mhandler"].Info("Received advertisement packet", "hash", h)

		err := c.handleAdvertisement(adv)
		if err != nil {
			return fmt.Errorf("error handling advertisement: %w", err)
		}

	case packet.MessageTypeMetadata:
		mtd := msg.MSG.(*packet.Metadata)
		h := htos(mtd.DataHash)
		c.l["mhandler"].Info("Received metadata packet", "hash", h)

		// Do not block
		go func() {
			c.scheduler.rec <- schedulerMessage{
				t: schedulerMessageTypeMetadata,
				m: schedulerMetadataMessage{
					h: mtd.DataHash,
				},
			}
		}()

		err := c.handleMetadata(mtd)
		if err != nil {
			return fmt.Errorf("error handling metadata: %w", err)
		}

	case packet.MessageTypeData:
		data := msg.MSG.(*packet.Data)
		fh := htos(data.RootHash)
		bh := htos(data.LeafHash)
		c.l["mhandler"].Info("Received data packet", "file_hash", fh, "blob_hash", bh, "blob_id", data.LeafIndex, "packet_id", data.PacketIndex)

		go func() {
			c.scheduler.rec <- schedulerMessage{
				t: schedulerMessageTypeData,
				m: schedulerDataMessage{
					rootHash: data.RootHash,
					leafHash: data.LeafHash,
				},
			}
		}()

		c.filesMapMutex.Lock()
		f, ok := c.filesMap[data.RootHash]
		if !ok {
			// TODO: Remove
			panic(fmt.Sprintf("received data packet for unknown file: %s", fh))
		}
		c.filesMapMutex.Unlock()

		if f.isFinished {
			return nil
		}

		f.dataChan <- &dataInChunk{
			chunkHash:   data.LeafHash,
			chunkID:     data.LeafIndex,
			packetIndex: data.PacketIndex,
			data:        data.Data,
		}

	default:
		return fmt.Errorf("unknown message type: %d", msg.MSG.Type())
	}

	return nil
}

func makeLoggers() map[string]*slog.Logger {
	makeLogger := func(component string, level *slog.LevelVar) *slog.Logger {
		h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
			Level: level,
		})
		return slog.New(h).With("component", component)
	}

	levels := map[string]*slog.LevelVar{
		"global":    {},
		"fec":       {},
		"send":      {},
		"receive":   {},
		"mhandler":  {},
		"metadata":  {},
		"file":      {},
		"request":   {},
		"scheduler": {},
	}

	loggers := map[string]*slog.Logger{
		"global":    makeLogger("global", levels["global"]),
		"fec":       makeLogger("fec", levels["fec"]),
		"send":      makeLogger("send", levels["send"]),
		"receive":   makeLogger("receive", levels["receive"]),
		"mhandler":  makeLogger("mhandler", levels["mhandler"]),
		"metadata":  makeLogger("metadata", levels["metadata"]),
		"file":      makeLogger("file", levels["file"]),
		"request":   makeLogger("request", levels["request"]),
		"scheduler": makeLogger("scheduler", levels["scheduler"]),
	}

	// Change level at any time, takes effect immediately:
	levels["global"].Set(slog.LevelDebug)
	levels["fec"].Set(slog.LevelInfo)
	levels["send"].Set(slog.LevelDebug)
	levels["receive"].Set(slog.LevelInfo)
	levels["mhandler"].Set(slog.LevelWarn)
	levels["metadata"].Set(slog.LevelInfo)
	levels["file"].Set(slog.LevelInfo)
	levels["request"].Set(slog.LevelInfo)
	levels["scheduler"].Set(slog.LevelWarn)

	return loggers
}
