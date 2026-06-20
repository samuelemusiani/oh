package oh

import (
	"errors"
	"net"
)

var (
	ErrNilAddress     = errors.New("address cannot be nil")
	ErrInvalidAddress = errors.New("address must be a valid multicast address")
)

func NewConfig(address net.IP, port uint16, iface string) (Config, error) {
	c := Config{
		address: address,
		port:    port,
		iface:   iface,
	}

	if err := checkConfig(c); err != nil {
		return Config{}, err
	}

	return c, nil
}

type Config struct {
	// Multicast group address to send/receive files
	address net.IP
	// Port number to send/receive files
	port uint16
	// Network interface to use for multicast communication (e.g., "eth0")
	iface string
}

func (c Config) Address() net.IP {
	return c.address
}

func (c Config) Port() uint16 {
	return c.port
}

func (c Config) Interface() string {
	return c.iface
}

func checkConfig(c Config) error {
	if c.address == nil {
		return ErrNilAddress
	}

	if !c.address.IsMulticast() {
		return ErrInvalidAddress
	}

	if c.port == 0 {
		return errors.New("port number must be between 1 and 65535")
	}

	if c.iface == "" {
		return errors.New("network interface cannot be empty")
	}

	return nil
}
