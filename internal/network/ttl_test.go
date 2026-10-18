package network

import (
	"testing"
	"time"

	"github.com/divizn/godis/internal/protocol"
)

func TestTTLCalls(t *testing.T) {
	addr, f := serveFlaky(t, newMemStore(), func(int) (bool, bool) { return false, false })
	c := dial(t, addr)
	ctx := t.Context()

	for _, ttl := range []time.Duration{0, -time.Second, time.Duration(protocol.MaxTTL+1) * time.Millisecond} {
		start := time.Now()
		if err := c.SetTTL(within(t, 2*time.Second), "k", []byte("v"), ttl); err == nil || time.Since(start) > time.Second {
			t.Fatalf("set with ttl %v: got %v after %v, want an immediate client-side error", ttl, err, time.Since(start))
		}
	}
	if n := len(f.requests()); n != 0 {
		t.Fatalf("rejected ttls still sent %d requests", n)
	}

	lastTTL := func() int64 {
		reqs := f.requests()
		return reqs[len(reqs)-1].TTL
	}
	for _, tc := range []struct {
		ttl  time.Duration
		want int64
	}{{time.Second, 1000}, {1500 * time.Microsecond, 2}, {time.Nanosecond, 1}} {
		if err := c.SetTTL(ctx, "r", []byte("v"), tc.ttl); err != nil {
			t.Fatalf("set with ttl %v: %v", tc.ttl, err)
		}
		if got := lastTTL(); got != tc.want {
			t.Fatalf("ttl %v sent as %d ms, want %d", tc.ttl, got, tc.want)
		}
	}
	for _, tc := range []struct {
		ttl  time.Duration
		want int64
	}{{-5 * time.Second, 0}, {2500 * time.Microsecond, 3}} {
		if _, err := c.Expire(ctx, "r", tc.ttl); err != nil {
			t.Fatalf("expire with ttl %v: %v", tc.ttl, err)
		}
		if got := lastTTL(); got != tc.want {
			t.Fatalf("expire ttl %v sent as %d ms, want %d", tc.ttl, got, tc.want)
		}
	}

	if _, found, err := c.TTL(ctx, "missing"); err != nil || found {
		t.Fatalf("ttl of a missing key: %v, %v", found, err)
	}
	if err := c.SetTTL(ctx, "a", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if d, found, err := c.TTL(ctx, "a"); err != nil || !found || d <= 0 || d > time.Minute {
		t.Fatalf("ttl after set: %v, %v, %v", d, found, err)
	}
	if ok, err := c.Expire(ctx, "a", time.Hour); err != nil || !ok {
		t.Fatalf("expire of a live key: %v, %v", ok, err)
	}
	if d, _, _ := c.TTL(ctx, "a"); d <= time.Minute || d > time.Hour {
		t.Fatalf("ttl after expire: %v", d)
	}
	if ok, err := c.Expire(ctx, "missing", time.Hour); err != nil || ok {
		t.Fatalf("expire of a missing key: %v, %v", ok, err)
	}
	if err := c.Set(ctx, "a", []byte("w")); err != nil {
		t.Fatal(err)
	}
	if d, found, err := c.TTL(ctx, "a"); err != nil || !found || d != 0 {
		t.Fatalf("ttl after a plain set: %v, %v, %v", d, found, err)
	}
	if ok, err := c.Expire(ctx, "a", 0); err != nil || !ok {
		t.Fatalf("expire with ttl 0: %v, %v", ok, err)
	}
	if _, ok := mustGet(t, c, "a"); ok {
		t.Fatal("key still there after expire with ttl 0")
	}
}
