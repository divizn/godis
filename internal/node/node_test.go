package node

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/divizn/godis/internal/consensus"
	"github.com/divizn/godis/internal/network"
	"github.com/prometheus/client_golang/prometheus"
)

type testCluster struct {
	t        testing.TB
	election time.Duration
	root     string
	peers    []Peer
	nodes    map[string]*Node
	regs     map[string]*prometheus.Registry
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
	n, err := Start(Config{ID: id, Peers: c.peers, Dir: filepath.Join(c.root, id), ClientListener: cl, RaftListener: rl, ElectionTimeout: c.election, Metrics: reg})
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
func (c *testCluster) via(id string, f func(*network.Client) error) error {
	cl, err := network.Dial(c.peer(id).ClientAddr)
	if err != nil {
		return err
	}
	defer cl.Close()
	return f(cl)
}

func set(key, value string) func(*network.Client) error {
	return func(cl *network.Client) error { return cl.Set(key, []byte(value)) }
}

func del(key string) func(*network.Client) error {
	return func(cl *network.Client) error { return cl.Del(key) }
}

// get fails the test on a wrong value; an error is returned for the caller to retry
func get(t *testing.T, key string, want string, wantOK bool) func(*network.Client) error {
	return func(cl *network.Client) error {
		v, ok, err := cl.Get(key)
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
