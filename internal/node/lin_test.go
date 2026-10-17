package node

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/internal/lincheck"
	"github.com/divizn/godis/internal/network"
)

// TestLinearizableAcrossRestarts runs clients through the real client and TCP stack while nodes,
// the leader included, are stopped and restarted, then checks the history for linearizability
func TestLinearizableAcrossRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("15s of real time")
	}
	c := newTestCluster(t, 3)
	c.leader()

	end := time.Now().Add(15 * time.Second)
	var clock atomic.Int64
	var mu sync.Mutex
	var history []lincheck.Op
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() {
			ops := linClient(c, &clock, i, end)
			mu.Lock()
			history = append(history, ops...)
			mu.Unlock()
		})
	}

	var down string
	for time.Now().Before(end) {
		time.Sleep(500*time.Millisecond + rand.N(1500*time.Millisecond))
		switch r := rand.IntN(3); {
		case down != "":
			c.start(down)
			down = ""
		case r == 0:
			c.stopAll()
			for _, p := range c.peers {
				c.start(p.ID)
			}
		case r == 1:
			down = c.leader()
		default:
			down = c.peers[rand.IntN(len(c.peers))].ID
		}
		if down != "" {
			c.stop(down)
		}
	}
	wg.Wait()
	if down != "" {
		c.start(down)
	}

	pending := 0
	for _, op := range history {
		if op.Return == lincheck.Pending {
			pending++
		}
	}
	t.Logf("%d ops (%d pending)", len(history), pending)
	res := lincheck.Check(history, 1<<22)
	if !res.Linearizable {
		slices.SortFunc(res.Ops, func(a, b lincheck.Op) int { return cmp.Compare(a.Call, b.Call) })
		var b strings.Builder
		for _, op := range res.Ops {
			fmt.Fprintln(&b, op)
		}
		t.Fatalf("key %q not linearizable (budget ran out: %v):\n%s", res.Key, res.Unknown, b.String())
	}
}

func linClient(c *testCluster, clock *atomic.Int64, client int, end time.Time) []lincheck.Op {
	var ops []lincheck.Op
	var cl *network.Client
	defer func() {
		if cl != nil {
			cl.Close()
		}
	}()
	for n := 0; time.Now().Before(end); n++ {
		time.Sleep(rand.N(50 * time.Millisecond))
		if cl == nil {
			var err error
			if cl, err = network.Dial(c.peers[rand.IntN(len(c.peers))].ClientAddr); err != nil {
				continue
			}
		}
		op := lincheck.Op{Client: client, Key: []string{"x", "y"}[rand.IntN(2)]}
		op.Call = clock.Add(1)
		var err error
		switch r := rand.IntN(10); {
		case r < 4:
			op.Kind, op.Value = lincheck.Set, fmt.Sprintf("c%d-%d", client, n)
			err = cl.Set(op.Key, []byte(op.Value))
		case r < 5:
			op.Kind = lincheck.Del
			err = cl.Del(op.Key)
		default:
			op.Kind = lincheck.Get
			var v []byte
			v, op.Found, err = cl.Get(op.Key)
			op.Value = string(v)
		}
		switch {
		case err == nil:
			op.Return = clock.Add(1)
		case op.Kind == lincheck.Get:
		default:
			op.Return = lincheck.Pending
		}
		if err != nil {
			cl.Close()
			cl = nil
			if op.Kind == lincheck.Get {
				continue
			}
		}
		ops = append(ops, op)
	}
	return ops
}
