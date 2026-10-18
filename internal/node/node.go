// Package node assembles a godis server: a Raft node, the key-value state machine it feeds, and its peer and client servers.
package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/divizn/godis/internal/consensus"
	"github.com/divizn/godis/internal/network"
	"github.com/divizn/godis/internal/storage"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	requestTimeout         = 5 * time.Second
	defaultElectionTimeout = time.Second
)

type Peer struct {
	ID         string
	RaftAddr   string
	ClientAddr string
}

type Config struct {
	ID string
	// Peers lists every member, including ID.
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
}

type Node struct {
	raft  *consensus.Node
	store *storage.Store
	tr    *transport

	cancel     context.CancelFunc
	clientDone chan error

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
	ids := make([]string, 0, len(cfg.Peers))
	raftAddrs := map[string]string{}
	clientAddrs := map[string]string{}
	for _, p := range cfg.Peers {
		ids = append(ids, p.ID)
		raftAddrs[p.ID] = p.RaftAddr
		clientAddrs[p.ID] = p.ClientAddr
	}

	election := cfg.ElectionTimeout
	if election == 0 {
		election = defaultElectionTimeout
	}
	store := storage.New()
	m := newMetrics(cfg.Metrics)
	tr := &transport{addrs: raftAddrs, clients: map[conn]*rpc.Client{}}
	raft, err := consensus.New(consensus.Config{
		ID:        cfg.ID,
		Peers:     ids,
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
	})
	if err != nil {
		return nil, fmt.Errorf("start raft: %w", err)
	}
	registerRaftGauges(cfg.Metrics, raft)
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
		raftLn:     cfg.RaftListener,
		raftDone:   make(chan error, 1),
		conns:      map[net.Conn]struct{}{},
	}
	go func() { n.raftDone <- n.serveRaft(srv) }()
	kv := kvStore{raft: raft, store: store, clientAddrs: clientAddrs, metrics: m}
	go func() { n.clientDone <- network.NewServer(kv).Serve(ctx, cfg.ClientListener) }()
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
	raft        *consensus.Node
	store       *storage.Store
	clientAddrs map[string]string
	metrics     *metrics
}

func (s kvStore) Get(key string) ([]byte, bool, error) {
	start := time.Now()
	v, ok, err := s.get(key)
	s.metrics.observe("get", start, ok, err)
	return v, ok, err
}

func (s kvStore) Set(key string, value []byte) error {
	start := time.Now()
	err := s.propose(storage.EncodeSet(rand.Uint64(), 1, key, value))
	s.metrics.observe("set", start, true, err)
	return err
}

func (s kvStore) Delete(key string) error {
	start := time.Now()
	err := s.propose(storage.EncodeDelete(rand.Uint64(), 1, key))
	s.metrics.observe("del", start, true, err)
	return err
}

func (s kvStore) get(key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := s.raft.ReadIndex(ctx); err != nil {
		return nil, false, s.redirect(fmt.Errorf("read index: %w", err))
	}
	v, ok := s.store.Get(key)
	return v, ok, nil
}

func (s kvStore) propose(cmd []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if _, err := s.raft.Propose(ctx, cmd); err != nil {
		return s.redirect(fmt.Errorf("propose: %w", err))
	}
	return nil
}

func (s kvStore) redirect(err error) error {
	var nle *consensus.NotLeaderError
	if errors.As(err, &nle) {
		return &network.RedirectError{Addr: s.clientAddrs[nle.Leader]}
	}
	return err
}
