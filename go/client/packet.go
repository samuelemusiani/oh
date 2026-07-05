package client

import "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"

func (s *Client) RequestPacket(hash [32]byte) *packet.RequestLeaves {
	return &packet.RequestLeaves{
		Hash: hash,
	}
}
