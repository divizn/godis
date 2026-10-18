package consensus

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/divizn/godis/internal/storage"
)

const (
	testElection  = 100 * time.Millisecond
	testHeartbeat = 20 * time.Millisecond
)

var errUnreachable = errors.New("unreachable")

type memNet struct {
	mu    sync.Mutex
	rng   *rand.Rand
	nodes map[string]*Node
	group map[string]int
	// deaf receives nothing, though what it sends arrives
	deaf  string
	drop  float64
	delay time.Duration
	// slowAppend delays every AppendEntries that carries entries, and every InstallSnapshot
	slowAppend time.Duration
	installs   int

	leaders   map[uint64]string
	violation string
	oversized string
}

func newMemNet(seed uint64) *memNet {
	return &memNet{rng: rand.New(rand.NewPCG(seed, seed)), nodes: map[string]*Node{}, group: map[string]int{}, leaders: map[uint64]string{}}
}

func (m *memNet) partition(groups ...[]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.group)
	m.deaf = ""
	for g, ids := range groups {
		for _, id := range ids {
			m.group[id] = g + 1
		}
	}
}

func (m *memNet) heal() { m.partition() }

func (m *memNet) deafen(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deaf = id
}

func (m *memNet) setFaults(drop float64, delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drop, m.delay = drop, delay
}

func (m *memNet) setSlowAppend(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.slowAppend = d
}

func (m *memNet) register(id string, n *Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n == nil {
		delete(m.nodes, id)
	} else {
		m.nodes[id] = n
	}
}

func (m *memNet) hop(ctx context.Context, from, to string) (*Node, error) {
	m.mu.Lock()
	n := m.nodes[to]
	ok := n != nil && m.group[from] == m.group[to] && to != m.deaf && (m.drop == 0 || m.rng.Float64() >= m.drop)
	var d time.Duration
	if m.delay > 0 {
		d = time.Duration(m.rng.Int64N(int64(m.delay)))
	}
	m.mu.Unlock()
	if !ok {
		return nil, errUnreachable
	}
	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return n, nil
}

func (m *memNet) sawLeader(term uint64, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if prev, ok := m.leaders[term]; ok && prev != id && m.violation == "" {
		m.violation = fmt.Sprintf("term %d has leaders %s and %s", term, prev, id)
	}
	m.leaders[term] = id
}

func (m *memNet) sawBatch(entries []Entry) {
	size := 0
	for _, e := range entries {
		size += len(e.Cmd)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(entries) > 1 && size > maxBatchBytes && m.oversized == "" {
		m.oversized = fmt.Sprintf("batch of %d entries carries %d bytes, limit %d", len(entries), size, maxBatchBytes)
	}
}

func (m *memNet) leaderTerms() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.leaders)
}

type memTransport struct {
	net  *memNet
	from string
}

func (t memTransport) RequestVote(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error) {
	n, err := t.net.hop(ctx, t.from, to)
	if err != nil {
		return RequestVoteReply{}, err
	}
	r, err := n.HandleRequestVote(args)
	if err != nil {
		return RequestVoteReply{}, err
	}
	if _, err := t.net.hop(ctx, to, t.from); err != nil {
		return RequestVoteReply{}, err
	}
	return r, nil
}

func (m *memNet) holdBack(ctx context.Context) error {
	m.mu.Lock()
	slow := m.slowAppend
	m.mu.Unlock()
	if slow == 0 {
		return nil
	}
	select {
	case <-time.After(slow):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *memNet) installCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.installs
}

func (t memTransport) AppendEntries(ctx context.Context, to string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	t.net.sawLeader(args.Term, args.Leader)
	t.net.sawBatch(args.Entries)
	if len(args.Entries) > 0 {
		if err := t.net.holdBack(ctx); err != nil {
			return AppendEntriesReply{}, err
		}
	}
	n, err := t.net.hop(ctx, t.from, to)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	r, err := n.HandleAppendEntries(args)
	if err != nil {
		return AppendEntriesReply{}, err
	}
	if _, err := t.net.hop(ctx, to, t.from); err != nil {
		return AppendEntriesReply{}, err
	}
	return r, nil
}

func (t memTransport) InstallSnapshot(ctx context.Context, to string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	t.net.sawLeader(args.Term, args.Leader)
	if err := t.net.holdBack(ctx); err != nil {
		return InstallSnapshotReply{}, err
	}
	n, err := t.net.hop(ctx, t.from, to)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	r, err := n.HandleInstallSnapshot(args)
	if err != nil {
		return InstallSnapshotReply{}, err
	}
	t.net.mu.Lock()
	t.net.installs++
	t.net.mu.Unlock()
	if _, err := t.net.hop(ctx, to, t.from); err != nil {
		return InstallSnapshotReply{}, err
	}
	return r, nil
}

type applied struct {
	index uint64
	cmd   string
}

type applyLog struct {
	// gate holds back applying the command "slow" until closed
	gate chan struct{}
	// store, if set, also applies every command as a storage command
	store   *storage.Store
	mu      sync.Mutex
	entries []applied
	// calls lists the indexes Apply got in this incarnation, restored entries excluded
	calls    []uint64
	saves    int
	restores int
}

// appliedState is an applyLog's snapshot: every entry it has applied, so histories stay comparable across installs
type appliedState struct {
	Index []uint64
	Cmd   [][]byte
	KV    []byte
}

func (a *applyLog) save() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	var s appliedState
	for _, e := range a.entries {
		s.Index = append(s.Index, e.index)
		s.Cmd = append(s.Cmd, []byte(e.cmd))
	}
	if a.store != nil {
		s.KV = a.store.Snapshot()
	}
	a.saves++
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(s); err != nil {
		panic(fmt.Sprintf("encode snapshot: %v", err))
	}
	return b.Bytes()
}

func (a *applyLog) restore(b []byte) error {
	var s appliedState
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&s); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	if a.store != nil {
		if err := a.store.Restore(s.KV); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = a.entries[:0]
	for i, idx := range s.Index {
		a.entries = append(a.entries, applied{idx, string(s.Cmd[i])})
	}
	a.restores++
	return nil
}

func (a *applyLog) applyCalls() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.calls)
}

func (a *applyLog) apply(index uint64, cmd []byte) []byte {
	if string(cmd) == "slow" {
		<-a.gate
	}
	result := append([]byte("applied "), cmd...)
	if a.store != nil {
		if err := a.store.Apply(cmd); err != nil {
			panic(fmt.Sprintf("apply %d: %v", index, err))
		}
		result = nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, applied{index, string(cmd)})
	a.calls = append(a.calls, index)
	return result
}

func (a *applyLog) snapshot() []applied {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.entries)
}

func (a *applyLog) has(cmd string) bool {
	return slices.ContainsFunc(a.snapshot(), func(e applied) bool { return e.cmd == cmd })
}

type cluster struct {
	t    *testing.T
	net  *memNet
	ids  []string
	root string

	mu    sync.Mutex
	nodes map[string]*Node
	logs  map[string]*applyLog
	past  map[string][]*applyLog
	gate  chan struct{}
	// changes records each node's OnLeaderChange calls across restarts
	changes map[string][]leaderChange

	attempt int
	// kv gives each node incarnation a storage.Store fed by its applied commands
	kv bool
	// snapEntries, if set, turns on snapshots every snapEntries applied entries
	snapEntries uint64
}

type leaderChange struct {
	term   uint64
	leader string
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := newStoppedCluster(t, size, rand.Uint64())
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

// newStoppedCluster creates a cluster with its network seeded but no node started
func newStoppedCluster(t *testing.T, size int, seed uint64) *cluster {
	c := &cluster{
		t:     t,
		net:   newMemNet(seed),
		root:  t.TempDir(),
		nodes: map[string]*Node{},
		logs:  map[string]*applyLog{},
		past:  map[string][]*applyLog{},
		gate:  make(chan struct{}),

		changes: map[string][]leaderChange{},
	}
	for i := range size {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
	}
	return c
}

func (c *cluster) start(id string) {
	c.t.Helper()
	al := &applyLog{gate: c.gate}
	if c.kv {
		al.store = storage.New()
	}
	cfg := Config{
		ID:                id,
		Peers:             c.ids,
		Dir:               filepath.Join(c.root, id),
		Transport:         memTransport{net: c.net, from: id},
		Apply:             al.apply,
		ElectionTimeout:   testElection,
		HeartbeatInterval: testHeartbeat,
		OnLeaderChange: func(term uint64, leader string) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.changes[id] = append(c.changes[id], leaderChange{term, leader})
		},
	}
	if c.snapEntries > 0 {
		cfg.Snapshot, cfg.Restore, cfg.SnapshotEntries = al.save, al.restore, c.snapEntries
	}
	n, err := New(cfg)
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.mu.Lock()
	c.nodes[id], c.logs[id] = n, al
	c.mu.Unlock()
	c.net.register(id, n)
}

func (c *cluster) stop(id string) {
	c.t.Helper()
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	if al := c.logs[id]; al != nil {
		c.past[id] = append(c.past[id], al)
		delete(c.logs, id)
	}
	c.mu.Unlock()
	if n == nil {
		return
	}
	c.net.register(id, nil)
	if err := n.Stop(); err != nil {
		c.t.Errorf("stop %s: %v", id, err)
	}
}

func (c *cluster) stopAll() {
	for _, id := range c.ids {
		c.stop(id)
	}
}

func (c *cluster) node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *cluster) log(id string) *applyLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logs[id]
}

func (c *cluster) live() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []string
	for _, id := range c.ids {
		if c.nodes[id] != nil {
			ids = append(ids, id)
		}
	}
	return ids
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *cluster) leaderAmong(ids ...string) (string, uint64) {
	var best string
	var term uint64
	for _, id := range ids {
		n := c.node(id)
		if n == nil {
			continue
		}
		if s := n.Status(); s.Role == Leader && s.Term >= term {
			best, term = id, s.Term
		}
	}
	return best, term
}

func (c *cluster) waitLeader(ids ...string) (string, uint64) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.live()
	}
	var id string
	var term uint64
	waitFor(c.t, 10*time.Second, fmt.Sprintf("a leader among %v", ids), func() bool {
		id, term = c.leaderAmong(ids...)
		return id != ""
	})
	return id, term
}

func (c *cluster) propose(id, cmd string, timeout time.Duration) error {
	n := c.node(id)
	if n == nil {
		return ErrStopped
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := n.Propose(ctx, []byte(cmd))
	return err
}

func (c *cluster) mustPropose(id, cmd string) {
	c.t.Helper()
	if err := c.propose(id, cmd, 5*time.Second); err != nil {
		c.t.Fatalf("propose %q on %s: %v", cmd, id, err)
	}
}

// commitAny retries prefix-suffixed commands on whoever leads until one commits
func (c *cluster) commitAny(prefix string) string {
	c.t.Helper()
	var cmd string
	waitFor(c.t, 30*time.Second, "a proposal to commit", func() bool {
		leader, _ := c.waitLeader()
		c.attempt++
		cmd = fmt.Sprint(prefix, c.attempt)
		return c.propose(leader, cmd, time.Second) == nil
	})
	return cmd
}

func (c *cluster) waitApplied(cmd string, ids ...string) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = c.live()
	}
	for _, id := range ids {
		waitFor(c.t, 10*time.Second, fmt.Sprintf("%s to apply %q", id, cmd), func() bool {
			return c.log(id).has(cmd)
		})
	}
}

func (c *cluster) checkOneLeaderPerTerm() {
	c.t.Helper()
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	if c.net.violation != "" {
		c.t.Fatal(c.net.violation)
	}
}

// live nodes applied the same sequence, earlier incarnations a prefix of it
func (c *cluster) checkSame() []applied {
	c.t.Helper()
	c.checkOneLeaderPerTerm()
	ids := c.live()
	want := c.log(ids[0]).snapshot()
	for _, id := range ids[1:] {
		if got := c.log(id).snapshot(); !slices.Equal(got, want) {
			c.t.Fatalf("%s applied %v, %s applied %v", id, got, ids[0], want)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, logs := range c.past {
		for _, al := range logs {
			got := al.snapshot()
			if len(got) > len(want) || !slices.Equal(got, want[:len(got)]) {
				c.t.Fatalf("earlier incarnation of %s applied %v, not a prefix of %v", id, got, want)
			}
		}
	}
	return want
}

func cmds(entries []applied) []string {
	var out []string
	for _, e := range entries {
		out = append(out, e.cmd)
	}
	return out
}

// snapshots counts the snapshots every incarnation of every node took
func (c *cluster) snapshots() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	count := func(al *applyLog) {
		al.mu.Lock()
		defer al.mu.Unlock()
		total += al.saves
	}
	for _, al := range c.logs {
		count(al)
	}
	for _, logs := range c.past {
		for _, al := range logs {
			count(al)
		}
	}
	return total
}

func (c *cluster) leaderChanges(id string) []leaderChange {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.changes[id])
}
