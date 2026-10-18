package consensus

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/divizn/godis/internal/lincheck"
	"github.com/divizn/godis/internal/storage"
)

var (
	linRuns = flag.Int("lin.runs", 3, "randomized runs of TestLinearizable")
	linSeed = flag.Uint64("lin.seed", 0, "replay the fault schedule of one TestLinearizable run")
)

const (
	linClients  = 4
	linOps      = 200
	linOpWindow = 2 * time.Second
	linBudget   = 1 << 20
)

var linKeys = []string{"x", "y", "z"}

// TestLinearizable runs clients against a Raft cluster with a storage.Store state machine under a seeded
// fault schedule, then checks the recorded history for linearizability
func TestLinearizable(t *testing.T) {
	seeds := []uint64{*linSeed}
	if *linSeed == 0 {
		seeds = seeds[:0]
		for range *linRuns {
			seeds = append(seeds, rand.Uint64())
		}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { linRun(t, seed) })
		})
	}
}

type linRecorder struct {
	clock   atomic.Int64
	mu      sync.Mutex
	history []lincheck.Op
}

func (r *linRecorder) record(op lincheck.Op) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.history = append(r.history, op)
}

func linRun(t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, seed))
	c := newStoppedCluster(t, []int{3, 5}[rng.IntN(2)], rng.Uint64())
	c.kv = true
	c.snapEntries = 1 + seed%20
	for _, id := range c.ids {
		c.start(id)
	}
	defer c.stopAll()

	duration := 30*time.Second + time.Duration(rng.Int64N(int64(30*time.Second)))
	end := time.Now().Add(duration)
	think := duration * linClients / linOps
	var rec linRecorder
	var wg sync.WaitGroup
	for i := range linClients {
		crng := rand.New(rand.NewPCG(rng.Uint64(), uint64(i)))
		wg.Go(func() { linClient(c, &rec, crng, i, end, think) })
	}
	nrng := rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64()))
	wg.Go(func() { nemesis(c, nrng, end) })
	wg.Wait()

	c.net.heal()
	c.net.setFaults(0, 0)
	c.net.setSlowAppend(0)
	for _, id := range c.ids {
		if c.node(id) == nil {
			c.start(id)
		}
	}
	marker := storage.EncodeSet("end", nil)
	waitFor(t, 30*time.Second, "the end marker to commit", func() bool {
		leader, _ := c.waitLeader()
		return c.propose(leader, string(marker), time.Second) == nil
	})
	c.waitApplied(string(marker))
	// a client retries only a write that never ran, and values are unique, so each set applies at most once
	sets := map[string]bool{}
	for _, op := range rec.history {
		if op.Kind == lincheck.Set {
			sets[string(storage.EncodeSet(op.Key, []byte(op.Value)))] = true
		}
	}
	applied := map[string]bool{}
	for _, e := range c.checkSame() {
		if sets[e.cmd] && applied[e.cmd] {
			t.Errorf("seed %d: write %q applied twice; replay with -lin.seed=%d", seed, e.cmd, seed)
		}
		applied[e.cmd] = true
	}

	pending := 0
	for _, op := range rec.history {
		if op.Return == lincheck.Pending {
			pending++
		}
	}
	t.Logf("%d nodes, %v, %d ops (%d pending), %d leader terms, snapshots every %d entries: %d taken, %d installed",
		len(c.ids), duration.Round(time.Second), len(rec.history), pending, c.net.leaderTerms(), c.snapEntries, c.snapshots(), c.net.installCount())
	res := lincheck.Check(rec.history, linBudget)
	if !res.Linearizable {
		slices.SortFunc(res.Ops, func(a, b lincheck.Op) int { return cmp.Compare(a.Call, b.Call) })
		var b strings.Builder
		for _, op := range res.Ops {
			fmt.Fprintln(&b, op)
		}
		verdict := "not linearizable"
		if res.Unknown {
			verdict = "search budget ran out"
		}
		t.Fatalf("seed %d, %d nodes, %d ops: key %q %s:\n%sreplay with -lin.seed=%d", seed, len(c.ids), len(rec.history), res.Key, verdict, b.String(), seed)
	}
}

func (c *cluster) nodeStore(id string) (*Node, *storage.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.nodes[id]; n != nil {
		return n, c.logs[id].store
	}
	return nil, nil
}

// linClient runs operations until end, retrying an operation only on NotLeaderError, which means it never ran
func linClient(c *cluster, rec *linRecorder, rng *rand.Rand, client int, end time.Time, think time.Duration) {
	target := c.ids[rng.IntN(len(c.ids))]
	for n := 0; ; n++ {
		time.Sleep(time.Duration(rng.Int64N(int64(2 * think))))
		if time.Now().After(end) {
			return
		}
		op := lincheck.Op{Client: client, Key: linKeys[rng.IntN(len(linKeys))]}
		var cmd []byte
		switch r := rng.IntN(10); {
		case r < 4:
			op.Kind, op.Value = lincheck.Set, fmt.Sprintf("c%d-%d", client, n)
			cmd = storage.EncodeSet(op.Key, []byte(op.Value))
		case r < 5:
			op.Kind = lincheck.Del
			cmd = storage.EncodeDelete(op.Key)
		default:
			op.Kind = lincheck.Get
		}

		op.Call = rec.clock.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), linOpWindow)
		var err error
		for {
			n, store := c.nodeStore(target)
			if n == nil {
				err = ErrStopped
			} else if op.Kind == lincheck.Get {
				if err = n.ReadIndex(ctx); err == nil {
					v, ok := store.Get(op.Key)
					op.Value, op.Found = string(v), ok
				}
			} else {
				err = n.Propose(ctx, cmd)
			}
			var nle *NotLeaderError
			if err == nil || ctx.Err() != nil || n != nil && !errors.As(err, &nle) {
				break
			}
			if n != nil && nle.Leader != "" {
				target = nle.Leader
			} else {
				target = c.ids[rng.IntN(len(c.ids))]
				time.Sleep(10 * time.Millisecond)
			}
		}
		cancel()
		switch {
		case err == nil:
			op.Return = rec.clock.Add(1)
		case op.Kind == lincheck.Get:
			continue
		default:
			op.Return = lincheck.Pending
		}
		rec.record(op)
	}
}

// nemesis injects faults from rng until end: partitions, node stops and restarts, message loss and delay,
// and appends with entries held back while empty ones pass
func nemesis(c *cluster, rng *rand.Rand, end time.Time) {
	shuffled := func() []string {
		ids := slices.Clone(c.ids)
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		return ids
	}
	without := func(id string) []string {
		return slices.DeleteFunc(slices.Clone(c.ids), func(x string) bool { return x == id })
	}
	for time.Now().Before(end) {
		time.Sleep(200*time.Millisecond + time.Duration(rng.Int64N(int64(time.Second))))
		leader, _ := c.leaderAmong(c.live()...)
		switch r := rng.IntN(14); {
		case r < 2:
			ids := shuffled()
			cut := 1 + rng.IntN(len(ids)-1)
			c.net.partition(ids[:cut], ids[cut:])
		case r < 3 && leader != "":
			c.net.partition([]string{leader}, without(leader))
		case r < 4 && leader != "":
			rest := without(leader)
			rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
			k := (len(c.ids) - 1) / 2
			c.net.partition(append(rest[:k:k], leader), rest[k:])
		case r < 5 && leader != "":
			c.net.deafen(leader)
		case r < 7:
			c.net.heal()
		case r < 8:
			c.stop(c.ids[rng.IntN(len(c.ids))])
		case r < 9 && leader != "":
			c.stop(leader)
		case r < 10:
			for _, id := range c.ids {
				if c.node(id) == nil {
					c.start(id)
				}
			}
		case r < 11:
			c.stopAll()
			for _, id := range c.ids {
				c.start(id)
			}
		case r < 12:
			if rng.IntN(2) == 0 {
				c.net.setFaults(0, 0)
			} else {
				c.net.setFaults(rng.Float64()/5, time.Duration(rng.Int64N(int64(10*time.Millisecond))))
			}
		default:
			if rng.IntN(3) == 0 {
				c.net.setSlowAppend(0)
			} else {
				c.net.setSlowAppend(testElection + time.Duration(rng.Int64N(int64(3*testElection))))
			}
		}
	}
}
