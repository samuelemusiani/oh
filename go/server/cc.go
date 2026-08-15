package server

import (
	"fmt"
	"math"
	"net"
	"slices"
	"time"

	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

const (
	timeSpan   = 500 //ms
	nFeedbacks = 10
)

func (s *Server) congestionControl() {
	s.ccMutex.Lock()
	s.sentPacketsNumber = make([]uint64, nFeedbacks)
	s.ccMutex.Unlock()

	for {
		s.adjustRate(s.latestID)
		// TODO: probably not necessary
		s.ccMutex.Lock()
		clear(s.sentPacketsNumber)
		clear(s.clientFeedbacks)
		s.ccMutex.Unlock()

		s.latestID++
		s.l.Warn("sending feedback request packet", "id", s.latestID, "timeSpan", timeSpan, "nFeedbacks", nFeedbacks)
		p := FeedbackRequestPacket(s.latestID, timeSpan, nFeedbacks)
		s.sendNowCh <- p

		s.sentPackets.Store(0)

		for i := range nFeedbacks {
			time.Sleep(time.Duration(timeSpan/nFeedbacks) * time.Millisecond)
			s.sentPacketsNumber[i] = s.sentPackets.Load()
		}

		// fmt.Println("packets: ", s.sentPacketsNumber)

		time.Sleep(timeSpan * 3 / 2 * time.Millisecond)
	}
}

func (s *Server) adjustRate(id uint16) {
	s.l.Warn("adjusting rate")
	s.ccMutex.Lock()
	defer s.ccMutex.Unlock()

	allLosses := make([]float64, 0, len(s.clientFeedbacks))

	for client, feedback := range s.clientFeedbacks {
		// avgLoss := feedback.lossSum / float64(feedback.samples)
		avgLoss := avgLostWithMovingVector(feedback.vector, s.sentPacketsNumber)
		s.l.Warn("feedback", "id", id, "client", net.IP(client[:]).String(), "avg_loss", avgLoss*100)
		fmt.Println("feedback vector: ", feedback.vector)
		fmt.Println("server vector:\t ", s.sentPacketsNumber)
		allLosses = append(allLosses, avgLoss)
	}

	if len(allLosses) == 0 {
		if s.latestID != 0 {
			ll := s.sendingLimit.Swap(s.sendingLimit.Load() / 2)
			s.lastLimitAtLoss.Store(ll)
			s.lastLoss = time.Now()
			fmt.Println("DECREASING RATE", s.sendingLimit.Load(), s.lastLimitAtLoss.Load())
		}
		return
	}

	slices.Sort(allLosses)
	p := 90
	index := min(int(math.Floor(float64(len(allLosses))*float64(p)/100)), len(allLosses))

	fmt.Println("index", index, "allLosses[index]", allLosses[index])
	if allLosses[index] < 0.01 && s.limitedByRate.Load() {
		// factor := 1 + 0.01*math.Pow(float64(time.Since(s.lastLoss))/1e9, 1.5)
		// fmt.Println("factor", factor)
		// s.sendingLimit.Store(int64(float64(s.sendingLimit.Load()) * factor))
		if s.lastLimitAtLoss.Load() == 0 { // No loss yet
			s.sendingLimit.Store(s.sendingLimit.Load() * 2)
		} else { // Cubic
			s.sendingLimit.Store(int64(math.Floor(cubic(0.7, s.lastLimitAtLoss.Load(), s.lastLoss, 0.4))))
		}
		fmt.Println("INCREASING RATE", s.sendingLimit.Load())
	} else if allLosses[index] > 0.05 {
		ll := s.sendingLimit.Swap(s.sendingLimit.Load() / 2) // TODO: probably we have to match beta
		s.lastLimitAtLoss.Store(ll)
		s.lastLoss = time.Now()
		fmt.Println("DECREASING RATE", s.sendingLimit.Load(), s.lastLimitAtLoss.Load())
	}
}

type clientFeedback struct {
	// lossSum float64
	// samples uint16
	vector []uint16
}

func (s *Server) handleFeedback(f *packet.Feedback, src net.IP) {
	// TODO: Check that server IP is current IP :)
	// TODO: Check if request id make sense (is up to date with what we sent)
	if f.RequestID != s.latestID {
		s.l.Warn("ignoring feedback. IDs don't match", "request_id", f.RequestID, "server_id", s.latestID)
		return
	}

	index := int(float64(len(s.sentPacketsNumber)-1) * float64(f.PacketIndex) / float64(1<<16-1))
	index = min(index, len(s.sentPacketsNumber))
	// loss := 1.0 - float64(f.NumberPackets)/float64(s.sentPacketsNumber[index])
	// s.l.Warn("received feedback", "id", f.RequestID, "index", f.PacketIndex, "packets", f.NumberPackets, "sindex", index, "spackets", s.sentPacketsNumber[index], "serverIP", net.IP(f.ServerIP[:]).String(), "loss", loss)

	s.ccMutex.Lock()
	defer s.ccMutex.Unlock()

	srcBytes := [16]byte(src.To16())
	fstats, ok := s.clientFeedbacks[srcBytes]
	if !ok {
		fstats = &clientFeedback{}
		s.clientFeedbacks[srcBytes] = fstats
	}
	fstats.vector = append(fstats.vector, f.NumberPackets)
}

func avgLostWithMovingVector(a []uint16, b []uint64) float64 {
	loss := 1.0 // Max possible loss

	for offset := range 2 {
		sum := 0.0
		tot := 0
		i := offset
		j := 0
		for i < len(a) && j < len(b) {
			if b[j] == 0 {
				i++
				j++
				continue
			}
			sum += 1 - float64(a[i])/float64(b[j])
			tot++
			i++
			j++
		}
		loss = max(0, min(loss, sum/float64(tot)))
	}

	return loss
}

func cubic(beta float64, wMax int64, lastLoss time.Time, C float64) float64 {
	fmt.Println("beta", beta, "wMax", wMax, "lastLoss", float64(time.Since(lastLoss).Milliseconds())/1e3, "C", C)
	K := math.Pow((float64(wMax)*(1-beta))/C, 1.0/3.0)
	tmp := C*math.Pow((float64(time.Since(lastLoss).Milliseconds())/1e3-K), 3.0) + float64(wMax)
	fmt.Println("K", K, "tmp", tmp)
	return tmp
}
