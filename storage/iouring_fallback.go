//go:build !linux

package storage

import (
	"context"
	"net"
)

var enableLinuxIOUring bool

// customDialer is a standard fallback dialer for non-Linux platforms
func customDialer(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
