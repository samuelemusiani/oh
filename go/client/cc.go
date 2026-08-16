package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

type ccRequest struct {
	id              uint16
	packetsReceived uint64
	cancel          func()
	done            chan struct{}
}

type cc struct {
	m        sync.Mutex
	requests map[[16]byte]*ccRequest
}

func (c *Client) initCC() {
	c.cc = cc{
		requests: make(map[[16]byte]*ccRequest),
	}
}

func (c *Client) handleFeedbackRequest(fr *packet.FeedbackRequest, src net.IP) {
	// c.l["cc"].Debug("handling feedback", "src", src.String(), "fr", fr.RequestID)
	c.cc.m.Lock()

	src = src.To16()
	ccr, ok := c.cc.requests[[16]byte(src)]
	if !ok {
		// c.l["cc"].Debug("not found in hashmap")
		ccr = &ccRequest{
			id:              fr.RequestID,
			packetsReceived: 0,
			cancel:          nil,
			done:            make(chan struct{}, 1),
		}
		c.cc.requests[[16]byte(src)] = ccr
	} else {
		// c.l["cc"].Debug("found in hashmap")
		if ccr.id >= fr.RequestID {
			return
		}
		ccr.cancel()
		<-ccr.done
		ccr.id = fr.RequestID
		ccr.packetsReceived = 0
	}
	c.cc.m.Unlock()

	// c.l["cc"].Debug("starting feedbacks")
	ctx, cancel := context.WithCancel(context.Background())
	go c.sendFeedbacks(ctx, fr.RequestID, fr.Timespan, fr.NumberOfFeedbacks, src, ccr.done)
	ccr.cancel = cancel
}

func (c *Client) sendFeedbacks(ctx context.Context, id, timespan, numberOfFeedbacks uint16, serverIP net.IP, done chan<- struct{}) {
	serverIP = serverIP.To16()
	serverIPbytes := [16]byte(serverIP)

	defer func() {
		done <- struct{}{}
	}()
	absStart := time.Now()

	baseSleepDuration := time.Duration(timespan/numberOfFeedbacks) * time.Millisecond
	time.Sleep(baseSleepDuration)

	for range numberOfFeedbacks {
		start := time.Now()
		if time.Since(absStart) >= time.Duration(uint64(float32(timespan)*1.5))*time.Millisecond {
			return
		}

		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}

		pktsReceived := c.getPacketsReceivedByServerIP(serverIPbytes)
		if pktsReceived > uint64((1<<16)-1) {
			panic("received more packets then capacity of uint16")
		}

		index := uint16(((1 << 16) - 1) * min(float64((time.Since(absStart)).Milliseconds())/float64(timespan), 1))
		c.sendCh <- c.FeedbackPacket(id, index, uint16(pktsReceived), serverIPbytes)

		time.Sleep(baseSleepDuration - time.Since(start))
	}
}

func (c *Client) getPacketsReceivedByServerIP(serverIP [16]byte) uint64 {
	c.cc.m.Lock()
	defer c.cc.m.Unlock()
	return c.cc.requests[serverIP].packetsReceived
}

func (c *Client) addPacketReceivedByServerIP(serverIP [16]byte) {
	c.cc.m.Lock()
	defer c.cc.m.Unlock()
	ccr, ok := c.cc.requests[serverIP]
	if !ok {
		return
	}
	ccr.packetsReceived++
}
