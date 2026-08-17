package client

import (
	"gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go/internal/packet"
)

func (*Client) RequestLeavesPacket(hash [32]byte) *packet.RequestLeaves {
	return &packet.RequestLeaves{
		Hash: hash,
	}
}

func (*Client) RequestLeafPacket(index uint32, rootHash [32]byte) *packet.RequestLeaf {
	return &packet.RequestLeaf{
		Index:    index,
		RootHash: rootHash,
	}
}

func (*Client) FeedbackPacket(id, index, packetsReceived uint16, serverIP [16]byte) *packet.Feedback {
	return &packet.Feedback{
		RequestID:     id,
		PacketIndex:   index,
		NumberPackets: packetsReceived,
		ServerIP:      serverIP,
	}
}
