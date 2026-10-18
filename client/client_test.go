package client

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// silentServer accepts connections and reads requests without ever answering them
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

func TestCancelInterruptsAnUnansweredRequest(t *testing.T) {
	for name, tc := range map[string]struct {
		call        func(context.Context, *Client) error
		wantUnknown bool
	}{
		"write": {func(ctx context.Context, c *Client) error { return c.Set(ctx, "k", []byte("v")) }, true},
		"read": {func(ctx context.Context, c *Client) error {
			_, _, err := c.Get(ctx, "k")
			return err
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := Dial(silentServer(t))
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.Close()
			ctx, cancel := context.WithCancel(t.Context())
			time.AfterFunc(100*time.Millisecond, cancel)

			start := time.Now()
			err = tc.call(ctx, c)
			if d := time.Since(start); d > time.Second {
				t.Fatalf("returned %v after cancel, want promptly", d)
			}
			if !errors.Is(err, context.Canceled) || errors.Is(err, ErrOutcomeUnknown) != tc.wantUnknown {
				t.Fatalf("got %v, want context.Canceled, outcome unknown %v", err, tc.wantUnknown)
			}
		})
	}
}
