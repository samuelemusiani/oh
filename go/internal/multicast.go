package internal

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"syscall"

	oh "gitlab.lrz.de/protocol-design-ss26-meta-group-1-5/ass2_impl/go"

	"golang.org/x/net/ipv4"
)

const BUFFER_SIZE = 8 * 1024 * 1024

type Multicast struct {
	conn *ipv4.PacketConn
	addr net.UDPAddr

	myAddrs map[string]struct{}
}

func OpenMulticastConnection(addr net.IP, port uint16, iface string) (*Multicast, error) {
	if addr == nil {
		return nil, oh.ErrNilAddress
	}

	if !addr.IsMulticast() {
		return nil, oh.ErrInvalidAddress
	}

	netIface, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, fmt.Errorf("error finding network interface %s: %w", iface, err)
	}

	addrs, err := netIface.Addrs()
	if err != nil {
		return nil, fmt.Errorf("error getting addresses for interface %s: %w", iface, err)
	}

	ipSet := make(map[string]struct{})
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			if ip := ipnet.IP.To4(); ip != nil {
				ipSet[ip.String()] = struct{}{}
			}
		}
	}

	// create UDP socket with SO_REUSEADDR (so multiple listeners can bind)
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var ctrlErr error
			err := c.Control(func(fd uintptr) {
				ctrlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				if ctrlErr != nil {
					ctrlErr = fmt.Errorf("error setting SO_REUSEADDR: %w", ctrlErr)
					return
				}

				ctrlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, BUFFER_SIZE)
				if ctrlErr != nil {
					ctrlErr = fmt.Errorf("error setting SO_RCVBUF: %w", ctrlErr)
					return
				}
			})

			if err != nil {
				return fmt.Errorf("error controlling socket: %w", err)
			}

			return ctrlErr
		},
	}

	c, err := lc.ListenPacket(context.Background(), "udp4", net.JoinHostPort("0.0.0.0", strconv.FormatUint(uint64(port), 10)))
	if err != nil {
		return nil, fmt.Errorf("error creating UDP socket: %w", err)
	}

	p := ipv4.NewPacketConn(c)
	if err := p.JoinGroup(netIface, &net.UDPAddr{IP: addr}); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("error joining multicast group: %w", err)
	}

	if err := p.SetControlMessage(ipv4.FlagDst, true); err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("error setting control message: %w", err)
	}

	// TODO: Set TTL to a reasonable value.
	// 2 should be reasonable as we only have 1 router in the network
	err = p.SetMulticastTTL(2)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("error setting TTL: %w", err)
	}

	err = p.SetMulticastInterface(netIface)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("error setting multicast interface: %w", err)
	}

	return &Multicast{
		conn:    p,
		addr:    net.UDPAddr{IP: addr, Port: int(port)},
		myAddrs: ipSet,
	}, nil
}

func (m *Multicast) Send(data []byte) error {
	// fmt.Printf("Sending %d bytes\n", len(data))
	_, err := m.conn.WriteTo(data, nil, &m.addr)
	if err != nil {
		return fmt.Errorf("error sending data: %w", err)
	}

	return nil
}

func (m *Multicast) Receive(buf []byte) (n int, src net.IP, err error) {
loop:
	for {
		n, cm, _, err := m.conn.ReadFrom(buf)
		if err != nil {
			return 0, nil, fmt.Errorf("error receiving data: %w", err)
		}

		if cm == nil || cm.Dst == nil {
			return 0, nil, fmt.Errorf("error receiving data: control message is nil or missing destination address")
		}

		if _, ok := m.myAddrs[cm.Src.String()]; ok {
			continue loop
		}

		return n, cm.Src, nil
	}
}
