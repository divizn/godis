package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/client"
	"github.com/divizn/godis/internal/consensus"
	"github.com/prometheus/client_golang/prometheus"
)

type testCluster struct {
	t        testing.TB
	election time.Duration
	root     string
	peers    []Peer
	nodes    map[string]*Node
	regs     map[string]*prometheus.Registry
	// snapEntries is passed to every node started from now on, zero for the default
	snapEntries uint64
}

func listen(t testing.TB, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	return ln
}

func newTestCluster(t testing.TB, size int) *testCluster {
	t.Helper()
	return newTestClusterWithElection(t, size, 150*time.Millisecond)
}

// newTestClusterWithElection takes an election timeout, zero for the production default
func newTestClusterWithElection(t testing.TB, size int, election time.Duration) *testCluster {
	t.Helper()
	c := &testCluster{t: t, election: election, root: t.TempDir(), nodes: map[string]*Node{}, regs: map[string]*prometheus.Registry{}}
	var lns [][2]net.Listener
	for i := range size {
		cl, rl := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
		lns = append(lns, [2]net.Listener{cl, rl})
		c.peers = append(c.peers, Peer{ID: fmt.Sprintf("node-%d", i+1), RaftAddr: rl.Addr().String(), ClientAddr: cl.Addr().String()})
	}
	for i, p := range c.peers {
		c.startOn(p.ID, lns[i][0], lns[i][1])
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) peer(id string) Peer {
	for _, p := range c.peers {
		if p.ID == id {
			return p
		}
	}
	c.t.Fatalf("no peer %s", id)
	return Peer{}
}

func (c *testCluster) startOn(id string, cl, rl net.Listener) {
	c.t.Helper()
	reg := prometheus.NewRegistry()
	n, err := Start(Config{ID: id, Peers: c.peers, Dir: filepath.Join(c.root, id), ClientListener: cl, RaftListener: rl, ElectionTimeout: c.election, Metrics: reg, SnapshotEntries: c.snapEntries})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.nodes[id], c.regs[id] = n, reg
}

func (c *testCluster) start(id string) {
	c.t.Helper()
	p := c.peer(id)
	c.startOn(id, listen(c.t, p.ClientAddr), listen(c.t, p.RaftAddr))
}

func (c *testCluster) stop(id string) {
	c.t.Helper()
	n := c.nodes[id]
	delete(c.nodes, id)
	if n == nil {
		return
	}
	if err := n.Stop(); err != nil {
		c.t.Errorf("stop %s: %v", id, err)
	}
}

func (c *testCluster) stopAll() {
	for _, p := range c.peers {
		c.stop(p.ID)
	}
}

func (c *testCluster) leader() string {
	c.t.Helper()
	var leader string
	waitFor(c.t, "a leader", func() error {
		for id, n := range c.nodes {
			if n.Status().Role == consensus.Leader {
				leader = id
				return nil
			}
		}
		return fmt.Errorf("no leader among %d nodes", len(c.nodes))
	})
	return leader
}

func (c *testCluster) other(than string) string {
	for _, p := range c.peers {
		if p.ID != than {
			return p.ID
		}
	}
	return ""
}

func waitFor(t testing.TB, what string, f func() error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %v", what, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// via runs f against a fresh client on node id's client address
func (c *testCluster) via(id string, f func(*client.Client) error) error {
	cl, err := client.Dial(c.peer(id).ClientAddr)
	if err != nil {
		return err
	}
	defer cl.Close()
	return f(cl)
}

func set(key, value string) func(*client.Client) error {
	return func(cl *client.Client) error { return cl.Set(context.Background(), key, []byte(value)) }
}

func del(key string) func(*client.Client) error {
	return func(cl *client.Client) error { return cl.Del(context.Background(), key) }
}

// get fails the test on a wrong value; an error is returned for the caller to retry
func get(t *testing.T, key string, want string, wantOK bool) func(*client.Client) error {
	return func(cl *client.Client) error {
		v, ok, err := cl.Get(context.Background(), key)
		if err != nil {
			return err
		}
		if ok != wantOK || string(v) != want {
			t.Fatalf("get %q: got %q, %v; want %q, %v", key, v, ok, want, wantOK)
		}
		return nil
	}
}

func TestWriteThroughFollowerReadAnywhere(t *testing.T) {
	c := newTestCluster(t, 3)
	follower := c.other(c.leader())

	if err := c.via(follower, set("k", "v")); err != nil {
		t.Fatalf("set through follower %s: %v", follower, err)
	}
	for _, p := range c.peers {
		if err := c.via(p.ID, get(t, "k", "v", true)); err != nil {
			t.Fatalf("get through %s: %v", p.ID, err)
		}
	}
}

func TestFailoverAndCatchUp(t *testing.T) {
	c := newTestCluster(t, 3)
	old := c.leader()
	if err := c.via(old, set("a", "1")); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := c.via(old, set("gone", "x")); err != nil {
		t.Fatalf("set gone: %v", err)
	}

	c.stop(old)
	survivor := c.other(old)
	if err := c.via(survivor, set("b", "2")); err != nil {
		t.Fatalf("set right after the leader stopped: %v", err)
	}
	if err := c.via(survivor, del("gone")); err != nil {
		t.Fatalf("del after failover: %v", err)
	}
	if err := c.via(survivor, get(t, "a", "1", true)); err != nil {
		t.Fatalf("get a after failover: %v", err)
	}

	c.start(old)
	store := c.nodes[old].store
	waitFor(t, old+" to catch up", func() error {
		a, _ := store.Get("a")
		b, _ := store.Get("b")
		_, gone := store.Get("gone")
		if string(a) != "1" || string(b) != "2" || gone {
			return fmt.Errorf("restarted node has a=%q b=%q gone present=%v", a, b, gone)
		}
		return nil
	})
}

func TestFullRestartKeepsData(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	for i := range 10 {
		if err := c.via(c.peers[i%3].ID, set(fmt.Sprint("k", i), fmt.Sprint("v", i))); err != nil {
			t.Fatalf("set k%d: %v", i, err)
		}
	}
	if err := c.via(c.peers[0].ID, del("k3")); err != nil {
		t.Fatalf("del k3: %v", err)
	}

	c.stopAll()
	for _, p := range c.peers {
		c.start(p.ID)
	}
	for i := range 10 {
		want, wantOK := fmt.Sprint("v", i), i != 3
		if !wantOK {
			want = ""
		}
		waitFor(t, fmt.Sprint("get k", i), func() error {
			return c.via(c.peers[i%3].ID, get(t, fmt.Sprint("k", i), want, wantOK))
		})
	}
}

func TestFollowerCatchesUpBySnapshotOverTheWire(t *testing.T) {
	c := newTestCluster(t, 3)
	c.stopAll()
	c.snapEntries = 5
	for _, p := range c.peers {
		c.start(p.ID)
	}
	leader := c.leader()
	follower := c.other(leader)
	c.stop(follower)
	behind := c.nodes[leader].Status().CommitIndex
	for i := range 30 {
		if err := c.via(leader, set(fmt.Sprint("k", i%7), fmt.Sprint("v", i))); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if s := c.nodes[leader].Status(); s.SnapshotIndex <= behind {
		t.Fatalf("leader snapshot index %d, want past the stopped follower's %d", s.SnapshotIndex, behind)
	}

	c.start(follower)
	store := c.nodes[follower].store
	waitFor(t, follower+" to catch up", func() error {
		for i := 23; i < 30; i++ {
			if v, _ := store.Get(fmt.Sprint("k", i%7)); string(v) != fmt.Sprint("v", i) {
				return fmt.Errorf("k%d = %q", i%7, v)
			}
		}
		return nil
	})
	c.stop(leader)
	for i := 23; i < 30; i++ {
		if err := c.via(follower, get(t, fmt.Sprint("k", i%7), fmt.Sprint("v", i), true)); err != nil {
			t.Fatalf("get k%d after the leader stopped: %v", i%7, err)
		}
	}
}

func TestStopAnswersInFlightWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()

	var wg sync.WaitGroup
	var ok atomic.Int64
	stopped := make(chan string, 8)
	for i := range 8 {
		wg.Go(func() {
			cl, err := client.Dial(c.peer(leader).ClientAddr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer cl.Close()
			for j := 0; ; j++ {
				err := cl.Set(t.Context(), fmt.Sprint("k", i, "-", j), []byte("v"))
				if err != nil {
					if strings.Contains(err.Error(), consensus.ErrStopped.Error()) {
						stopped <- err.Error()
					}
					return
				}
				ok.Add(1)
			}
		})
	}
	waitFor(t, "50 writes", func() error {
		if n := ok.Load(); n < 50 {
			return fmt.Errorf("%d writes so far", n)
		}
		return nil
	})
	c.stop(leader)
	wg.Wait()
	close(stopped)
	for err := range stopped {
		t.Errorf("write in flight during stop answered %q", err)
	}
}

func TestMaxSizeValuesCommitWithoutElection(t *testing.T) {
	c := newTestClusterWithElection(t, 3, 0)
	leader := c.leader()
	term := c.nodes[leader].Status().Term
	value := strings.Repeat("v", 8<<20)
	for i := range 3 {
		if err := c.via(leader, set(fmt.Sprint("big", i), value)); err != nil {
			t.Fatalf("set value %d of %d bytes: %v", i, len(value), err)
		}
	}
	if s := c.nodes[leader].Status(); s.Role != consensus.Leader || s.Term != term {
		t.Fatalf("leader %s moved from term %d to %v in term %d", leader, term, s.Role, s.Term)
	}
}

func TestRaftErrorsTellTheClientWhatToRetry(t *testing.T) {
	s := kvStore{clientAddrs: map[string]string{"node-2": "addr-2"}}
	var re *client.RedirectError
	if err := s.classify(fmt.Errorf("propose: %w", &consensus.NotLeaderError{Leader: "node-2"})); !errors.As(err, &re) || re.Addr != "addr-2" {
		t.Fatalf("not the leader: got %v, want a redirect to addr-2", err)
	}
	for _, cause := range []error{consensus.ErrLeadershipLost, context.DeadlineExceeded} {
		if err := s.classify(fmt.Errorf("propose: %w", cause)); errors.As(err, &re) || !errors.Is(err, client.ErrOutcomeUnknown) || !errors.Is(err, cause) {
			t.Fatalf("%v with the write logged: got %v, want an unknown outcome the client retries", cause, err)
		}
	}
	if err := s.classify(fmt.Errorf("propose: %w", consensus.ErrCommandTooLarge)); errors.Is(err, client.ErrOutcomeUnknown) || errors.As(err, &re) {
		t.Fatalf("command too large: got %v, want a definite error", err)
	}
}

func TestWriteRetriedAcrossALeaderKillAppliesOnce(t *testing.T) {
	// the production timeout keeps the write logged on the leader for a whole second before it steps down
	c := newTestClusterWithElection(t, 3, 0)
	leader := c.leader()
	var followers []string
	addrs := []string{c.peer(leader).ClientAddr}
	for _, p := range c.peers {
		if p.ID != leader {
			followers = append(followers, p.ID)
			addrs = append(addrs, p.ClientAddr)
		}
	}
	cl, err := client.Dial(addrs...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.Incr(t.Context(), "other", 1); err != nil {
		t.Fatalf("warm up incr: %v", err)
	}

	for _, f := range followers {
		c.stop(f)
	}
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := cl.Incr(t.Context(), "n", 1)
		done <- result{n, err}
	}()
	waitFor(t, leader+" to step down without a quorum", func() error {
		if c.nodes[leader].Status().Role == consensus.Leader {
			return errors.New("still leading")
		}
		return nil
	})
	c.stop(leader)
	c.start(leader)
	c.start(followers[0])

	r := <-done
	if r.err != nil || r.n != 1 {
		t.Fatalf("incr retried across a leader kill: got %d, %v; want 1", r.n, r.err)
	}
	if v, ok, err := cl.Get(t.Context(), "n"); err != nil || !ok || string(v) != "1" {
		t.Fatalf("get n: got %q, %v, %v; want 1", v, ok, err)
	}
	if d := c.nodes[c.leader()].store.Duplicates(); d == 0 {
		t.Fatal("the leader dropped no duplicate: the first attempt never committed, so the retry was not deduplicated")
	}
	if _, err := cl.Incr(t.Context(), "other", 1); err != nil {
		t.Fatalf("incr after the retry: %v", err)
	}
	if err := cl.Set(t.Context(), "s", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Incr(t.Context(), "s", 1); err == nil || errors.Is(err, client.ErrOutcomeUnknown) {
		t.Fatalf("incr of a non-integer: got %v, want a definite error", err)
	}
}
