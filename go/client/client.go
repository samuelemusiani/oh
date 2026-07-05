package client

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/chunker"
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
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

	filesToRequest chan [32]byte

	sendCh    chan packet.Message
	receiveCh chan packet.MessageWithSrc

	l map[string]*slog.Logger
}

func New(c oh.Config) (*Client, error) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	m, err := internal.OpenMulticastConnection(c.Address(), c.Port(), c.Interface())
	if err != nil {
		return nil, fmt.Errorf("error opening multicast connection: %w", err)
	}

	l := makeLoggers()

	return &Client{
		config:        c,
		multicastConn: m,
		fecDecoder:    internal.NewFECDecoder(internal.NewFEC(fecType), l["fec"]),
		chunker:       chunker.NewChunker(hashFunc()),

		metadataBuff:   make(map[[32]byte][]*packet.Metadata),
		filesMap:       make(map[[32]byte]*file),
		filesToRequest: make(chan [32]byte),

		sendCh:    make(chan packet.Message, 1024),
		receiveCh: make(chan packet.MessageWithSrc, 1024),

		l: l,
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

func (c *Client) sendLoop() {
	// Clients are not expected to send any FEC blocks, we can ignore the construction
	for {
		// TODO: add some sort of windowing mechanism to avoid sending too many messages at once
		msg := <-c.sendCh
		data, err := packet.WritePacket(msg)
		if err != nil {
			c.l["send"].Error("error encoding message", "error", err)
			continue
		}

		if err := c.multicastConn.Send(data); err != nil {
			c.l["send"].Error("error sending message", "error", err)
			continue
		}
	}
}

func (c *Client) receiveLoop() {
	tmpPacketBuff := make(chan internal.PacketWithSrc)
	go func() {
		panic(c.fecDecoder.Run(tmpPacketBuff, c.receiveCh))
	}()

	buff := make([]byte, 65536)
	for {
		n, src, err := c.multicastConn.Receive(buff)
		if err != nil {
			c.l["receive"].Error("error receiving message", "error", err)
			continue
		}
		copyBuff := make([]byte, n)
		copy(copyBuff, buff[:n])
		tmpPacketBuff <- internal.PacketWithSrc{
			Packet: copyBuff,
			Src:    src,
		}
	}
}

func (c *Client) run() error {
	finish := make(chan struct{})
	go c.sendLoop()
	go c.receiveLoop()
	go c.requestFiles(finish)

	for {
		select {
		case msg := <-c.receiveCh:
			err := c.handleMessage(msg)
			if err != nil {
				c.l["global"].Error("error handling message", "error", err)
			}

		case <-finish:
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

		err := c.handleMetadata(mtd)
		if err != nil {
			return fmt.Errorf("error handling metadata: %w", err)
		}

	case packet.MessageTypeData:
		data := msg.MSG.(*packet.Data)
		fh := htos(data.RootHash)
		bh := htos(data.LeafHash)
		c.l["mhandler"].Info("Received data packet", "file_hash", fh, "blob_hash", bh, "blob_id", data.LeafIndex, "packet_id", data.PacketIndex)

		c.metadataBuffMutex.Lock()
		c.filesMapMutex.Lock()
		f, ok := c.filesMap[data.RootHash]
		if !ok {
			err := c.prepareFile(data.RootHash)
			if err != nil {
				c.metadataBuffMutex.Unlock()
				c.filesMapMutex.Unlock()
				return fmt.Errorf("error preparing file: %w", err)
			}
			f = c.filesMap[data.RootHash]
		}
		c.metadataBuffMutex.Unlock()
		c.filesMapMutex.Unlock()

		f.chunkChan <- chunk{
			chunkHash:   data.LeafHash,
			chunkID:     data.LeafIndex,
			chunkOffset: data.BlobOffset,
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
		"global":   {},
		"fec":      {},
		"send":     {},
		"receive":  {},
		"mhandler": {},
		"metadata": {},
		"file":     {},
	}

	loggers := map[string]*slog.Logger{
		"global":   makeLogger("global", levels["global"]),
		"fec":      makeLogger("fec", levels["fec"]),
		"send":     makeLogger("send", levels["send"]),
		"receive":  makeLogger("receive", levels["receive"]),
		"mhandler": makeLogger("mhandler", levels["mhandler"]),
		"metadata": makeLogger("metadata", levels["metadata"]),
		"file":     makeLogger("file", levels["file"]),
	}

	// Change level at any time, takes effect immediately:
	levels["global"].Set(slog.LevelDebug)
	levels["fec"].Set(slog.LevelInfo)
	levels["send"].Set(slog.LevelDebug)
	levels["receive"].Set(slog.LevelWarn)
	levels["mhandler"].Set(slog.LevelWarn)
	levels["metadata"].Set(slog.LevelWarn)
	levels["file"].Set(slog.LevelInfo)

	return loggers
}
