// Package node assembles a godis server: a Raft node, the key-value state machine it feeds, and its peer and client servers.
package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/divizn/godis/internal/consensus"
	"github.com/divizn/godis/internal/network"
	"github.com/divizn/godis/internal/protocol"
	"github.com/divizn/godis/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	requestTimeout         = 5 * time.Second
	changeTimeout          = 30 * time.Second
	defaultElectionTimeout = time.Second
	defaultSweepInterval   = time.Second
)

type Peer struct {
	ID         string
	RaftAddr   string
	ClientAddr string
}

type Config struct {
	ID string
	// Peers lists every member, including ID, when the cluster starts. Empty means a node that joins a running cluster:
	// it has no configuration until the cluster adds it. Once the log holds a configuration, that one is used instead.
	Peers []Peer
	// Dir holds the Raft state and log.
	Dir            string
	ClientListener net.Listener
	RaftListener   net.Listener
	// ElectionTimeout defaults to 1s, long enough to replicate a maximum size value; heartbeats are a tenth of it.
	ElectionTimeout time.Duration
	// Metrics registers the node's metrics; nil means none.
	Metrics prometheus.Registerer
	// SnapshotEntries is how many applied entries trigger a snapshot, zero for the consensus default.
	SnapshotEntries uint64
	// SweepInterval is how often the leader checks for expired keys, defaulting to 1s.
	SweepInterval time.Duration
	// MembershipOps allows clients to add, remove and list nodes. These requests are not authenticated, so anyone who
	// reaches the client port could remove every node.
	MembershipOps bool
}

type Node struct {
	raft  *consensus.Node
	store *storage.Store
	tr    *transport

	cancel     context.CancelFunc
	clientDone chan error
	sweepDone  chan struct{}

	raftLn   net.Listener
	raftDone chan error
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	connWG   sync.WaitGroup
}

// Start runs the node until Stop. It takes ownership of both listeners, closing them if it fails.
func Start(cfg Config) (*Node, error) {
	n, err := start(cfg)
	if err != nil {
		cfg.ClientListener.Close()
		cfg.RaftListener.Close()
		return nil, err
	}
	return n, nil
}

func start(cfg Config) (*Node, error) {
	var members []consensus.Member
	tr := &transport{addrs: map[string]string{}, clients: map[conn]*rpc.Client{}}
	book := &addrBook{client: map[string]string{}}
	for _, p := range cfg.Peers {
		members = append(members, consensus.Member{ID: p.ID, Addr: p.RaftAddr + "/" + p.ClientAddr})
		tr.addrs[p.ID] = p.RaftAddr
		book.client[p.ID] = p.ClientAddr
	}

	election := cfg.ElectionTimeout
	if election == 0 {
		election = defaultElectionTimeout
	}
	sweepEvery := cmp.Or(cfg.SweepInterval, defaultSweepInterval)
	store := storage.New()
	m := newMetrics(cfg.Metrics)
	raft, err := consensus.New(consensus.Config{
		ID:        cfg.ID,
		Members:   members,
		Dir:       cfg.Dir,
		Transport: tr,

		ElectionTimeout:   election,
		HeartbeatInterval: election / 10,
		Apply: func(index uint64, cmd []byte) []byte {
			result, err := store.Apply(index, cmd)
			if err != nil {
				panic(fmt.Sprintf("apply committed command %d: %v", index, err))
			}
			return result
		},
		Snapshot:        store.Snapshot,
		Restore:         store.Restore,
		SnapshotEntries: cfg.SnapshotEntries,
		OnLeaderChange:  func(uint64, string) { m.leaderChanges.Inc() },
		OnMembership: func(members []consensus.Member) {
			for _, mem := range members {
				raftAddr, clientAddr, _ := strings.Cut(mem.Addr, "/")
				tr.setAddr(mem.ID, raftAddr)
				book.set(mem.ID, clientAddr)
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	registerRaftGauges(cfg.Metrics, raft)
	registerStoreCounters(cfg.Metrics, store)
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", raftService{raft}); err != nil {
		return nil, errors.Join(fmt.Errorf("register raft rpc: %w", err), raft.Stop())
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		raft:       raft,
		store:      store,
		tr:         tr,
		cancel:     cancel,
		clientDone: make(chan error, 1),
		sweepDone:  make(chan struct{}),
		raftLn:     cfg.RaftListener,
		raftDone:   make(chan error, 1),
		conns:      map[net.Conn]struct{}{},
	}
	go func() { n.raftDone <- n.serveRaft(srv) }()
	kv := kvStore{raft: raft, store: store, tr: tr, book: book, membershipOps: cfg.MembershipOps, metrics: m}
	go func() { n.clientDone <- network.NewServer(kv).Serve(ctx, cfg.ClientListener) }()
	go func() {
		defer close(n.sweepDone)
		kv.sweepEvery(ctx, sweepEvery)
	}()
	return n, nil
}

func (n *Node) Status() consensus.Status {
	return n.raft.Status()
}

// Done is closed once Raft stops, by Stop or because it halted on a persistence failure; Err then says why.
func (n *Node) Done() <-chan struct{} {
	return n.raft.Done()
}

func (n *Node) Err() error {
	return n.raft.Err()
}

// Stop lets in-flight client requests finish, then stops Raft and closes every connection.
func (n *Node) Stop() error {
	n.cancel()
	<-n.sweepDone
	errs := []error{<-n.clientDone, n.raft.Stop()}

	n.raftLn.Close()
	if err := <-n.raftDone; !errors.Is(err, net.ErrClosed) {
		errs = append(errs, fmt.Errorf("raft accept: %w", err))
	}
	n.mu.Lock()
	for c := range n.conns {
		c.Close()
	}
	n.mu.Unlock()
	n.connWG.Wait()
	n.tr.close()
	return errors.Join(errs...)
}

func (n *Node) serveRaft(srv *rpc.Server) error {
	for {
		conn, err := n.raftLn.Accept()
		if err != nil {
			return err
		}
		n.mu.Lock()
		n.conns[conn] = struct{}{}
		n.mu.Unlock()
		n.connWG.Go(func() {
			srv.ServeConn(conn)
			n.mu.Lock()
			delete(n.conns, conn)
			n.mu.Unlock()
		})
	}
}

// kvStore serves the client protocol: writes through the Raft log, reads after a ReadIndex, both on the leader only
type kvStore struct {
	raft          *consensus.Node
	store         *storage.Store
	tr            *transport
	book          *addrBook
	membershipOps bool
	metrics       *metrics
}

// addrBook maps node ids to client addresses, for redirects; it only grows, since a redirect to a removed node is harmless
type addrBook struct {
	mu     sync.Mutex
	client map[string]string
}

func (b *addrBook) set(id, addr string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.client[id] = addr
}

func (b *addrBook) get(id string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.client[id]
}

func (s kvStore) Get(key string) ([]byte, bool, error) {
	start := time.Now()
	v, ok, err := s.get(key)
	s.metrics.observe("get", start, ok, err)
	return v, ok, err
}

func (s kvStore) Set(client, seq uint64, key string, value []byte, ttl time.Duration) error {
	start := time.Now()
	now := s.now()
	_, err := s.write(storage.EncodeSet(client, seq, now, deadline(now, ttl), key, value))
	s.metrics.observe("set", start, true, err)
	return err
}

func (s kvStore) Delete(client, seq uint64, key string) error {
	start := time.Now()
	_, err := s.write(storage.EncodeDelete(client, seq, s.now(), key))
	s.metrics.observe("del", start, true, err)
	return err
}

func (s kvStore) Cas(client, seq uint64, key string, expected []byte, expectFound bool, value []byte) (bool, error) {
	start := time.Now()
	body, err := s.write(storage.EncodeCas(client, seq, s.now(), 0, key, expected, expectFound, value))
	s.metrics.observe("cas", start, true, err)
	if err != nil {
		return false, err
	}
	return len(body) == 1 && body[0] == 1, nil
}

func (s kvStore) Incr(client, seq uint64, key string, delta int64) (int64, error) {
	start := time.Now()
	body, err := s.write(storage.EncodeIncr(client, seq, s.now(), key, delta))
	s.metrics.observe("incr", start, true, err)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("incr result %q: %w", body, err)
	}
	return n, nil
}

func (s kvStore) Expire(client, seq uint64, key string, ttl time.Duration) (bool, error) {
	start := time.Now()
	now := s.now()
	body, err := s.write(storage.EncodeExpire(client, seq, now, key, deadline(now, ttl)))
	s.metrics.observe("expire", start, true, err)
	if err != nil {
		return false, err
	}
	return len(body) == 1 && body[0] == 1, nil
}

func (s kvStore) TTL(key string) (time.Duration, bool, error) {
	start := time.Now()
	remaining, ok, err := s.ttl(key)
	s.metrics.observe("ttl", start, ok, err)
	return remaining, ok, err
}

// now is the time a proposal is stamped with, in Unix ms: the wall clock, but never behind a command this node applied
func (s kvStore) now() uint64 {
	return max(uint64(time.Now().UnixMilli()), s.store.Clock())
}

// deadline is the absolute time ttl from now, 0 for a ttl of 0 or less
func deadline(now uint64, ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	return now + uint64(ttl.Milliseconds())
}

// sweepEvery proposes a sweep whenever a key has expired, until ctx is done; on a follower the proposal is refused
func (s kvStore) sweepEvery(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweep(ctx)
		}
	}
}

func (s kvStore) sweep(ctx context.Context) {
	now := s.now()
	if !s.store.AnyDue(now) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	_, err := s.raft.Propose(ctx, storage.EncodeSweep(now))
	var nle *consensus.NotLeaderError
	if err != nil && ctx.Err() == nil && !errors.As(err, &nle) && !errors.Is(err, consensus.ErrStopped) {
		slog.Warn("propose sweep", "err", err)
	}
}

func (s kvStore) get(key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := s.raft.ReadIndex(ctx); err != nil {
		return nil, false, s.classify(fmt.Errorf("read index: %w", err))
	}
	v, ok := s.store.Get(key, uint64(time.Now().UnixMilli()))
	return v, ok, nil
}

func (s kvStore) ttl(key string) (time.Duration, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := s.raft.ReadIndex(ctx); err != nil {
		return 0, false, s.classify(fmt.Errorf("read index: %w", err))
	}
	ms, ok := s.store.TTL(key, uint64(time.Now().UnixMilli()))
	return time.Duration(ms) * time.Millisecond, ok, nil
}

// write proposes cmd and returns the body of its result, or the error of a command that failed
func (s kvStore) write(cmd []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	r, err := s.raft.Propose(ctx, cmd)
	if err != nil {
		return nil, s.classify(fmt.Errorf("propose: %w", err))
	}
	return storage.ParseResult(r)
}

// classify tells the client what a Raft error means: a redirect never ran, a command Raft refused to log is
// definite, and anything else, a lost leadership or a timeout above all, may still apply and is retried with the same seq
func (s kvStore) classify(err error) error {
	var nle *consensus.NotLeaderError
	switch {
	case errors.As(err, &nle):
		return &protocol.RedirectError{Addr: s.book.get(nle.Leader)}
	case errors.Is(err, consensus.ErrCommandTooLarge), errors.Is(err, consensus.ErrEmptyCommand),
		errors.Is(err, consensus.ErrChangeInProgress), errors.Is(err, consensus.ErrMemberExists), errors.Is(err, consensus.ErrNotMember),
		errors.Is(err, consensus.ErrLastMember), errors.Is(err, consensus.ErrLeaderNotReady), errors.Is(err, consensus.ErrCatchUpTimeout):
		return err
	}
	return fmt.Errorf("%w: %w", protocol.ErrOutcomeUnknown, err)
}

var errMembershipOpsOff = errors.New("membership operations are disabled; start the server with --membership-ops")

func (s kvStore) AddNode(id, raftAddr, clientAddr string) error {
	start := time.Now()
	err := s.addNode(id, raftAddr, clientAddr)
	s.metrics.observe("addnode", start, true, err)
	return err
}

func (s kvStore) addNode(id, raftAddr, clientAddr string) error {
	if !s.membershipOps {
		return errMembershipOpsOff
	}
	if id == "" || raftAddr == "" || clientAddr == "" || strings.Contains(raftAddr+clientAddr, "/") {
		return errors.New("a node needs an id and a raft and a client address without slashes")
	}
	m := consensus.Member{ID: id, Addr: raftAddr + "/" + clientAddr}
	members := s.raft.Members()
	if i := slices.IndexFunc(members, func(x consensus.Member) bool { return x.ID == id }); i >= 0 {
		if members[i] == m {
			return nil
		}
		return fmt.Errorf("node %s is already a member at a different address", id)
	}
	s.tr.setAddr(id, raftAddr)
	ctx, cancel := context.WithTimeout(context.Background(), changeTimeout)
	defer cancel()
	if err := s.raft.AddMember(ctx, m); err != nil {
		return s.classify(fmt.Errorf("add node %s: %w", id, err))
	}
	return nil
}

func (s kvStore) RemoveNode(id string) error {
	start := time.Now()
	err := s.removeNode(id)
	s.metrics.observe("removenode", start, true, err)
	return err
}

func (s kvStore) removeNode(id string) error {
	if !s.membershipOps {
		return errMembershipOpsOff
	}
	ctx, cancel := context.WithTimeout(context.Background(), changeTimeout)
	defer cancel()
	if err := s.raft.RemoveMember(ctx, id); err != nil {
		return s.classify(fmt.Errorf("remove node %s: %w", id, err))
	}
	return nil
}

func (s kvStore) Nodes() ([]protocol.Node, error) {
	if !s.membershipOps {
		return nil, errMembershipOpsOff
	}
	var nodes []protocol.Node
	for _, m := range s.raft.Members() {
		raftAddr, clientAddr, _ := strings.Cut(m.Addr, "/")
		nodes = append(nodes, protocol.Node{ID: m.ID, RaftAddr: raftAddr, ClientAddr: clientAddr})
	}
	return nodes, nil
}
