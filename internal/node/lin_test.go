package node

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/client"
	"github.com/divizn/godis/internal/consensus"
	"github.com/divizn/godis/internal/lincheck"
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
			ops := linClient(t, c, &clock, i, end)
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
			for id, n := range c.nodes {
				if n.Status().Role == consensus.Leader {
					down = id
				}
			}
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

// linClient runs operations through one client session, which retries unknown outcomes itself; an op is
// pending only if the client gave up on it
func linClient(t *testing.T, c *testCluster, clock *atomic.Int64, id int, end time.Time) []lincheck.Op {
	var addrs []string
	for _, p := range c.peers {
		addrs = append(addrs, p.ClientAddr)
	}
	rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
	cl, err := client.Dial(addrs...)
	if err != nil {
		t.Errorf("dial: %v", err)
		return nil
	}
	defer cl.Close()

	var ops []lincheck.Op
	for n := 0; time.Now().Before(end); n++ {
		time.Sleep(rand.N(50 * time.Millisecond))
		op := lincheck.Op{Client: id, Key: []string{"x", "y"}[rand.IntN(2)]}
		value := fmt.Sprintf("c%d-%d", id, n)
		op.Call = clock.Add(1)
		var err error
		switch r := rand.IntN(20); {
		case r < 5:
			op.Kind, op.Value = lincheck.Set, value
			err = cl.Set(t.Context(), op.Key, []byte(value))
		case r < 7:
			op.Kind = lincheck.Del
			err = cl.Del(t.Context(), op.Key)
		case r < 10:
			op.Kind, op.Value = lincheck.Cas, value
			var v []byte
			if v, op.ExpectFound, err = cl.Get(t.Context(), op.Key); err != nil {
				continue
			}
			op.Call, op.Expected = clock.Add(1), string(v)
			op.Found, err = cl.CAS(t.Context(), op.Key, v, op.ExpectFound, []byte(value))
		case r < 13:
			op.Kind, op.Key, op.Delta = lincheck.Incr, "n", 1+rand.Int64N(3)
			var v int64
			v, err = cl.Incr(t.Context(), op.Key, op.Delta)
			op.Value = fmt.Sprint(v)
		default:
			op.Kind = lincheck.Get
			if rand.IntN(3) == 0 {
				op.Key = "n"
			}
			var v []byte
			v, op.Found, err = cl.Get(t.Context(), op.Key)
			op.Value = string(v)
		}
		switch {
		case err == nil:
			op.Return = clock.Add(1)
		case op.Kind == lincheck.Get:
			continue
		case errors.Is(err, client.ErrOutcomeUnknown):
			op.Return = lincheck.Pending
		default:
			t.Errorf("%s %s: definite error %v", op.Kind, op.Key, err)
			continue
		}
		ops = append(ops, op)
	}
	return ops
}
