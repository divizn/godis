package node

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/divizn/godis/client"
)

var benchValue = bytes.Repeat([]byte("v"), 128)

// leaderAddr starts a 3-node cluster with the production election timeout and returns its leader's client address
func leaderAddr(b *testing.B) string {
	b.Helper()
	// keeps leader-change log lines out of the benchmark output
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prev) })
	c := newTestClusterWithElection(b, 3, 0)
	return c.peer(c.leader()).ClientAddr
}

func dial(b *testing.B, addr string) *client.Client {
	b.Helper()
	cl, err := client.Dial(addr)
	if err != nil {
		b.Fatalf("dial %s: %v", addr, err)
	}
	b.Cleanup(func() { cl.Close() })
	return cl
}

func reportOpsPerSec(b *testing.B) {
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}

func BenchmarkSet(b *testing.B) {
	cl := dial(b, leaderAddr(b))
	for b.Loop() {
		if err := cl.Set(b.Context(), "k", benchValue); err != nil {
			b.Fatal(err)
		}
	}
	reportOpsPerSec(b)
}

func BenchmarkSetParallel(b *testing.B) {
	addr := leaderAddr(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cl, err := client.Dial(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer cl.Close()
		for pb.Next() {
			if err := cl.Set(b.Context(), "k", benchValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportOpsPerSec(b)
}

func BenchmarkGet(b *testing.B) {
	cl := dial(b, leaderAddr(b))
	if err := cl.Set(b.Context(), "k", benchValue); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, _, err := cl.Get(b.Context(), "k"); err != nil {
			b.Fatal(err)
		}
	}
	reportOpsPerSec(b)
}
