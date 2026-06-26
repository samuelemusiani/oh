package client

import "encoding/hex"

func htos(h [32]byte) string {
	return hex.EncodeToString(h[:])
}
