package consensus

import (
	"bytes"
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
	linRetryFor = 10 * time.Second
)

var (
	linKeys     = []string{"x", "y", "z"}
	linCounters = []string{"n1", "n2"}
)

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
	retries atomic.Int64
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
	c.fsSeed = rng.Uint64() | 1
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
		lc := &linClient{t: t, c: c, rec: &rec, rng: rand.New(rand.NewPCG(rng.Uint64(), uint64(i))), client: i, seen: map[string]*string{}}
		wg.Go(func() { lc.run(end, think) })
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
	marker := storage.EncodeSet(0, 1, "end", nil)
	waitFor(t, 30*time.Second, "the end marker to commit", func() bool {
		leader, _ := c.waitLeader()
		return c.propose(leader, string(marker), time.Second) == nil
	})
	c.waitApplied(string(marker))

	// the log holds a retried write once per attempt that reached it; replaying it must execute each (client, seq) once
	// and rebuild exactly the stores the nodes have
	replay := storage.New()
	executed := map[string]bool{}
	writes := 0
	for _, e := range c.checkSame() {
		dups := replay.Duplicates()
		if _, err := replay.Apply(e.index, []byte(e.cmd)); err != nil {
			t.Fatalf("seed %d: replay entry %d: %v", seed, e.index, err)
		}
		if replay.Duplicates() != dups {
			continue
		}
		if executed[e.cmd] {
			t.Errorf("seed %d: write %q took effect twice; replay with -lin.seed=%d", seed, e.cmd, seed)
		}
		executed[e.cmd] = true
		if e.cmd != string(marker) {
			writes++
		}
	}
	for _, id := range c.live() {
		if _, store := c.nodeStore(id); !bytes.Equal(store.Snapshot(), replay.Snapshot()) {
			t.Errorf("seed %d: %s store differs from a replay of the log that applies each write once; replay with -lin.seed=%d", seed, id, seed)
		}
	}

	pending, ops := 0, 0
	for _, op := range rec.history {
		if op.Kind != lincheck.Get {
			ops++
		}
		if op.Return == lincheck.Pending {
			pending++
		}
	}
	t.Logf("%d nodes, %v, %d ops, %d writes (%d pending), %d applied, %d retries, %d duplicates dropped, %d leader terms, %d crashes, snapshots every %d entries: %d taken, %d installed",
		len(c.ids), duration.Round(time.Second), len(rec.history), ops, pending, writes, rec.retries.Load(), replay.Duplicates(),
		c.net.leaderTerms(), c.crashes, c.snapEntries, c.snapshots(), c.net.installCount())
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

// linClient is one client session: it retries a write whose outcome is unknown with the same (client, seq),
// on whichever node it tries next, until linRetryFor passes, as the network client does
type linClient struct {
	t      *testing.T
	c      *cluster
	rec    *linRecorder
	rng    *rand.Rand
	client int
	// id is the session's client id, the client in its low byte and a new session number above it each time
	// a write fails before any under id applied, as the network client does
	id, seq   uint64
	sessions  uint64
	confirmed bool
	target    string
	// seen is the last value this client saw at each key, nil for absent, so a cas often expects the current value
	seen map[string]*string
}

func (lc *linClient) run(end time.Time, think time.Duration) {
	lc.target = lc.c.ids[lc.rng.IntN(len(lc.c.ids))]
	lc.id = uint64(lc.client + 1)
	for n := 0; ; n++ {
		time.Sleep(time.Duration(lc.rng.Int64N(int64(2 * think))))
		if time.Now().After(end) {
			return
		}
		op := lincheck.Op{Client: lc.client, Key: linKeys[lc.rng.IntN(len(linKeys))]}
		value := fmt.Sprintf("c%d-%d", lc.client, n)
		var cmd []byte
		switch r := lc.rng.IntN(20); {
		case r < 5:
			lc.seq++
			op.Kind, op.Value = lincheck.Set, value
			cmd = storage.EncodeSet(lc.id, lc.seq, op.Key, []byte(value))
		case r < 7:
			lc.seq++
			op.Kind = lincheck.Del
			cmd = storage.EncodeDelete(lc.id, lc.seq, op.Key)
		case r < 10:
			lc.seq++
			op.Kind, op.Value = lincheck.Cas, value
			if v := lc.seen[op.Key]; v != nil {
				op.Expected, op.ExpectFound = *v, true
			}
			cmd = storage.EncodeCas(lc.id, lc.seq, op.Key, []byte(op.Expected), op.ExpectFound, []byte(value))
		case r < 13:
			lc.seq++
			op.Kind, op.Key, op.Delta = lincheck.Incr, linCounters[lc.rng.IntN(len(linCounters))], 1+lc.rng.Int64N(3)
			cmd = storage.EncodeIncr(lc.id, lc.seq, op.Key, op.Delta)
		default:
			op.Kind = lincheck.Get
			if lc.rng.IntN(3) == 0 {
				op.Key = linCounters[lc.rng.IntN(len(linCounters))]
			}
		}

		op.Call = lc.rec.clock.Add(1)
		body, found, ok := lc.send(op.Key, cmd)
		if cmd != nil && !ok && !lc.confirmed {
			lc.sessions++
			lc.id, lc.seq = uint64(lc.client+1)|lc.sessions<<8, 0
		}
		if cmd != nil && ok {
			lc.confirmed = true
		}
		if !ok {
			if op.Kind != lincheck.Get {
				op.Return = lincheck.Pending
				lc.rec.record(op)
			}
			continue
		}
		op.Return = lc.rec.clock.Add(1)
		switch op.Kind {
		case lincheck.Get:
			op.Value, op.Found = string(body), found
		case lincheck.Cas:
			op.Found = len(body) == 1 && body[0] == 1
		case lincheck.Incr:
			op.Value = string(body)
		}
		lc.see(op)
		lc.rec.record(op)
	}
}

// see records the value op leaves at its key, as far as this client can tell
func (lc *linClient) see(op lincheck.Op) {
	v := op.Value
	switch {
	case op.Kind == lincheck.Del, op.Kind == lincheck.Get && !op.Found:
		lc.seen[op.Key] = nil
	case op.Kind == lincheck.Cas && !op.Found:
		delete(lc.seen, op.Key)
	default:
		lc.seen[op.Key] = &v
	}
}

// send runs a write of cmd, or with no cmd a read of key, until it has a definite answer, reporting false if it
// gave up; a NotLeaderError means it never ran, any other error that it may have, so it is resent unchanged
func (lc *linClient) send(key string, cmd []byte) (body []byte, found, ok bool) {
	deadline := time.Now().Add(linRetryFor)
	for {
		n, store := lc.c.nodeStore(lc.target)
		err := ErrStopped
		if n != nil {
			ctx, cancel := context.WithTimeout(context.Background(), min(linOpWindow, time.Until(deadline)))
			if cmd == nil {
				if err = n.ReadIndex(ctx); err == nil {
					body, found = store.Get(key)
				}
			} else {
				var r []byte
				if r, err = n.Propose(ctx, cmd); err == nil {
					if body, err = storage.ParseResult(r); err != nil {
						lc.t.Errorf("write %q failed: %v", cmd, err)
						cancel()
						return nil, false, false
					}
				}
			}
			cancel()
			if err == nil {
				return body, found, true
			}
		}
		if time.Now().After(deadline) {
			return nil, false, false
		}
		var nle *NotLeaderError
		if n != nil && errors.As(err, &nle) && nle.Leader != "" {
			lc.target = nle.Leader
			continue
		}
		if n != nil && cmd != nil && nle == nil {
			lc.rec.retries.Add(1)
		}
		lc.target = lc.c.ids[lc.rng.IntN(len(lc.c.ids))]
		time.Sleep(10 * time.Millisecond)
	}
}

// nemesis injects faults from rng until end: partitions, node stops and restarts, node crashes that lose
// whatever was not fsynced, message loss and delay, and appends with entries held back while empty ones pass
func nemesis(c *cluster, rng *rand.Rand, end time.Time) {
	shuffled := func() []string {
		ids := slices.Clone(c.ids)
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		return ids
	}
	without := func(id string) []string {
		return slices.DeleteFunc(slices.Clone(c.ids), func(x string) bool { return x == id })
	}
	// crashNode half the time lets the node run until its filesystem freezes itself a few writes from now
	crashNode := func(id string) {
		if rng.IntN(2) == 0 {
			c.fsys[id].FreezeAfter(1 + rng.IntN(12))
			time.Sleep(testElection + time.Duration(rng.Int64N(int64(4*testElection))))
		}
		c.crash(id)
	}
	for time.Now().Before(end) {
		time.Sleep(200*time.Millisecond + time.Duration(rng.Int64N(int64(time.Second))))
		leader, _ := c.leaderAmong(c.live()...)
		switch r := rng.IntN(17); {
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
		case r < 14:
			if rng.IntN(3) == 0 {
				c.net.setSlowAppend(0)
			} else {
				c.net.setSlowAppend(testElection + time.Duration(rng.Int64N(int64(3*testElection))))
			}
		case r < 15:
			if live := c.live(); len(live) > 0 {
				crashNode(live[rng.IntN(len(live))])
			}
		case r < 16:
			if leader != "" {
				crashNode(leader)
			}
		default:
			c.crash(c.live()...)
			for _, id := range c.ids {
				c.start(id)
			}
		}
	}
}
