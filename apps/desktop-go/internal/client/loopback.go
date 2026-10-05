package client

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/roshankarande/waku/apps/desktop-go/internal/protocol"
)

// LoopbackE2EOptions describes a real protocol-v7 connection to a daemon
// bound on the local host. It is intended for daemon interoperability tests;
// unlike a fake transport it exercises HTTP upgrade, Hello auth and codecs.
type LoopbackE2EOptions struct {
	ConnectOptions
	SessionID protocol.UUID
	RuntimeID protocol.UUID
}

// ConnectLoopbackE2E connects only to an IP-literal loopback endpoint. The
// returned cleanup is idempotent and should be deferred by the test.
func ConnectLoopbackE2E(ctx context.Context, opts ConnectOptions) (*Client, func(), error) {
	u, err := daemonURL(opts.Address)
	if err != nil {
		return nil, nil, err
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || !ip.IsLoopback() {
		return nil, nil, fmt.Errorf("E2E daemon address %q is not an IP-literal loopback endpoint", opts.Address)
	}
	c, err := Connect(ctx, opts)
	if err != nil {
		return nil, nil, err
	}
	return c, func() { _ = c.CloseTransport() }, nil
}

// RunLoopbackRequest performs one fully correlated request over a real
// loopback daemon connection and always closes the transport afterward.
func RunLoopbackRequest(ctx context.Context, opts LoopbackE2EOptions, command any) ([]byte, error) {
	c, cleanup, err := ConnectLoopbackE2E(ctx, opts.ConnectOptions)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	payload, err := c.Request(ctx, opts.SessionID, opts.RuntimeID, command)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}
