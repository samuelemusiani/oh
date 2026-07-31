package server

import (
	"time"
)

const (
	timeSpan   = 1000 //ms
	nFeedbacks = 2
)

func (s *Server) congestionControl() {
	id := uint16(0)

	for {
		s.l.Warn("sending feedback request packet", "id", id, "timeSpan", timeSpan, "nFeedbacks", nFeedbacks)
		p := FeedbackRequestPacket(id, timeSpan, nFeedbacks)
		s.sendNowCh <- p
		time.Sleep(2 * timeSpan * time.Millisecond)
		id++
	}
}
