// Package consensus replicates a command log across a static cluster with Raft (extended paper, figure 2).
package consensus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/divizn/godis/internal/wal"
)

const (
	maxBatch      = 64
	maxBatchBytes = 1 << 20
	// room in a wal record for the batch's varint header
	maxCommandSize = wal.MaxRecordSize - 3*binary.MaxVarintLen64
)

var (
	ErrStopped         = errors.New("consensus: node stopped")
	ErrProposalDropped = errors.New("consensus: proposal overwritten by a newer leader")
	ErrEmptyCommand    = errors.New("consensus: empty command")
	ErrNoQuorum        = errors.New("consensus: leadership not confirmed by a majority")
	ErrCommandTooLarge = errors.New("consensus: command too large for one log record")
	// ErrLeadershipLost fails a proposal already in the log when its leader steps down: it may still commit.
	ErrLeadershipLost = errors.New("consensus: leadership lost, outcome unknown")
)

// NotLeaderError carries the current leader's id, empty if unknown. Propose returns it only for a command it never logged.
type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "consensus: not the leader, leader unknown"
	}
	return "consensus: not the leader, leader is " + e.Leader
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

// Entry is one log entry. An empty Cmd is a leader's no-op and is never applied.
type Entry struct {
	Term uint64
	Cmd  []byte
}

type RequestVoteArgs struct {
	Term         uint64
	Candidate    string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term    uint64
	Granted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	Leader       string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesReply carries conflict hints on a failed consistency check so the leader can skip a whole term.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

// InstallSnapshotArgs carries a leader's whole snapshot, covering the log up to LastIndex.
// ponytail: one RPC, no chunking, so a snapshot must fit in memory and one gob message; chunk it if the store outgrows that
type InstallSnapshotArgs struct {
	Term      uint64
	Leader    string
	LastIndex uint64
	LastTerm  uint64
	Data      []byte
}

type InstallSnapshotReply struct {
	Term uint64
}

// Transport sends Raft RPCs to a peer, served there by HandleRequestVote, HandleAppendEntries and HandleInstallSnapshot.
type Transport interface {
	RequestVote(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, to string, args AppendEntriesArgs) (AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, to string, args InstallSnapshotArgs) (InstallSnapshotReply, error)
}

type Config struct {
	ID string
	// Peers lists every member, including ID.
	Peers     []string
	Dir       string
	Transport Transport
	// Apply gets committed commands in order, once per process lifetime: after a restart, every command after the snapshot again.
	// Its result is opaque to Raft: Propose returns it on the leader, followers drop it.
	Apply func(index uint64, cmd []byte) []byte
	// Snapshot encodes the state machine as of the last command applied; it is never called during Apply.
	// Snapshot and Restore are set together; with neither, the log is never compacted.
	Snapshot func() []byte
	// Restore replaces the state machine with one a Snapshot encoded, here or on the leader.
	Restore func([]byte) error
	// SnapshotEntries is how many entries applied since the last snapshot trigger the next, default 10000.
	SnapshotEntries uint64
	// ElectionTimeout is randomized up to twice its value, default 150ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval defaults to 50ms and must be below ElectionTimeout.
	HeartbeatInterval time.Duration
	// OnLeaderChange is called with the node's lock held each time the leader it knows changes to a known one,
	// so at most once per term; it must not block or call into the Node.
	OnLeaderChange func(term uint64, leader string)
}

type Status struct {
	ID            string
	Role          Role
	Term          uint64
	Leader        string
	CommitIndex   uint64
	SnapshotIndex uint64
}

type Node struct {
	id             string
	peers          []string
	dir            string
	tr             Transport
	apply          func(uint64, []byte) []byte
	snapshot       func() []byte
	restore        func([]byte) error
	snapEntries    uint64
	onLeaderChange func(uint64, string)
	election       time.Duration
	heartbeat      time.Duration
	wake           map[string]chan struct{}

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	// ponytail: one lock over all state, held across fsyncs; split it and add group commit if a benchmark asks
	mu        sync.Mutex
	applyCond *sync.Cond
	wal       *wal.Log
	role      Role
	term      uint64
	votedFor  string
	leader    string
	// log[0] stands for the snapshot, holding its last term; entry i is log[i-snapIndex]
	log         []Entry
	snapIndex   uint64
	commitIndex uint64
	lastApplied uint64
	deadline    time.Time
	votes       int
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	// contact is when the leader last had a reply in its term from each peer
	contact map[string]time.Time
	waiters map[uint64]chan outcome
	// applied is closed and replaced each time lastApplied advances or the node stops leading
	applied chan struct{}
	// pending is an installed snapshot the applier has yet to restore
	pending *InstallSnapshotArgs
}

// New restores the node's term, vote and log from cfg.Dir and starts it as a follower.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.Dir == "" || cfg.Transport == nil || cfg.Apply == nil {
		return nil, errors.New("consensus: ID, Dir, Transport and Apply are required")
	}
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, fmt.Errorf("consensus: peers %v do not include %q", cfg.Peers, cfg.ID)
	}
	if len(slices.Compact(slices.Sorted(slices.Values(cfg.Peers)))) != len(cfg.Peers) {
		return nil, fmt.Errorf("consensus: duplicate peers in %v", cfg.Peers)
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 150 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.HeartbeatInterval >= cfg.ElectionTimeout {
		return nil, fmt.Errorf("consensus: heartbeat %v must be below election timeout %v", cfg.HeartbeatInterval, cfg.ElectionTimeout)
	}
	if (cfg.Snapshot == nil) != (cfg.Restore == nil) {
		return nil, errors.New("consensus: Snapshot and Restore must be set together")
	}
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = 10000
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir: %w", err)
	}
	term, vote, err := readState(cfg.Dir)
	if err != nil {
		return nil, err
	}
	snapIndex, snapTerm, data, err := readSnapshot(cfg.Dir)
	if err != nil {
		return nil, err
	}
	if snapIndex > 0 {
		if cfg.Restore == nil {
			return nil, errors.New("consensus: raft dir holds a snapshot but Restore is not set")
		}
		if err := cfg.Restore(data); err != nil {
			return nil, fmt.Errorf("restore raft snapshot at %d: %w", snapIndex, err)
		}
	}

	n := &Node{
		id:             cfg.ID,
		peers:          slices.DeleteFunc(slices.Clone(cfg.Peers), func(p string) bool { return p == cfg.ID }),
		dir:            cfg.Dir,
		tr:             cfg.Transport,
		apply:          cfg.Apply,
		snapshot:       cfg.Snapshot,
		restore:        cfg.Restore,
		snapEntries:    cfg.SnapshotEntries,
		onLeaderChange: cfg.OnLeaderChange,
		election:       cfg.ElectionTimeout,
		heartbeat:      cfg.HeartbeatInterval,
		wake:           map[string]chan struct{}{},
		term:           term,
		votedFor:       vote,
		log:            []Entry{{Term: snapTerm}},
		snapIndex:      snapIndex,
		commitIndex:    snapIndex,
		lastApplied:    snapIndex,
		waiters:        map[uint64]chan outcome{},
		applied:        make(chan struct{}),
	}
	n.wal, err = wal.Open(filepath.Join(cfg.Dir, "log"), n.replay)
	if err != nil {
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.ctx, n.cancel = context.WithCancelCause(context.Background())
	n.resetDeadline()

	n.wg.Go(n.runTimer)
	n.wg.Go(n.runApplier)
	for _, p := range n.peers {
		ch := make(chan struct{}, 1)
		n.wake[p] = ch
		n.wg.Go(func() { n.replicate(p, ch) })
		n.wg.Go(func() { n.heartbeats(p) })
	}
	return n, nil
}

func (n *Node) replay(rec []byte) error {
	start, entries, err := decodeBatch(rec)
	if err != nil {
		return fmt.Errorf("decode raft log batch: %w", err)
	}
	if start == 0 || start > n.lastIndex()+1 {
		return fmt.Errorf("raft log batch starts at %d but log ends at %d", start, n.lastIndex())
	}
	// entries the snapshot covers are committed, left over from a crash before the log was rewritten
	if start <= n.snapIndex {
		skip := n.snapIndex + 1 - start
		if skip >= uint64(len(entries)) {
			return nil
		}
		start, entries = n.snapIndex+1, entries[skip:]
	}
	n.log = append(n.log[:start-n.snapIndex], entries...)
	return nil
}

// outcome answers a waiting proposal with the result of applying its entry, or why it has none
type outcome struct {
	result []byte
	err    error
}

// Propose returns the result of applying cmd once it is committed and applied here; if ctx ends first it may still commit later.
func (n *Node) Propose(ctx context.Context, cmd []byte) ([]byte, error) {
	if len(cmd) == 0 {
		return nil, ErrEmptyCommand
	}
	if len(cmd) > maxCommandSize {
		return nil, fmt.Errorf("%w: %d bytes, limit %d", ErrCommandTooLarge, len(cmd), maxCommandSize)
	}
	n.mu.Lock()
	if n.ctx.Err() != nil {
		n.mu.Unlock()
		return nil, context.Cause(n.ctx)
	}
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		return nil, &NotLeaderError{Leader: leader}
	}
	idx := n.lastIndex() + 1
	if err := n.appendLog(idx, []Entry{{Term: n.term, Cmd: slices.Clone(cmd)}}); err != nil {
		n.mu.Unlock()
		return nil, err
	}
	done := make(chan outcome, 1)
	n.waiters[idx] = done
	n.advanceCommit()
	n.wakeAll()
	n.mu.Unlock()

	select {
	case o := <-done:
		return o.result, o.err
	case <-ctx.Done():
		n.mu.Lock()
		if n.waiters[idx] == done {
			delete(n.waiters, idx)
		}
		n.mu.Unlock()
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, context.Cause(n.ctx)
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Role: n.role, Term: n.term, Leader: n.leader, CommitIndex: n.commitIndex, SnapshotIndex: n.snapIndex}
}

// Stop halts the node and closes its log, returning the persistence error that halted it, if any.
// Done is closed once the node stops, by Stop or because it could not persist its state; Err then says why.
func (n *Node) Done() <-chan struct{} {
	return n.ctx.Done()
}

// Err is nil while the node runs, ErrStopped after Stop, or the persistence failure that halted it.
func (n *Node) Err() error {
	return context.Cause(n.ctx)
}

func (n *Node) Stop() error {
	n.mu.Lock()
	n.halt(ErrStopped)
	n.mu.Unlock()
	n.wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	var closeErr error
	if n.wal != nil {
		if err := n.wal.Close(); err != nil {
			closeErr = fmt.Errorf("close raft log: %w", err)
		}
		n.wal = nil
	}
	if cause := context.Cause(n.ctx); !errors.Is(cause, ErrStopped) {
		return cause
	}
	return closeErr
}

// HandleRequestVote serves a RequestVote RPC. An error means no reply must be sent.
func (n *Node) HandleRequestVote(args RequestVoteArgs) (RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() != nil {
		return RequestVoteReply{}, context.Cause(n.ctx)
	}
	if args.Term < n.term {
		return RequestVoteReply{Term: n.term}, nil
	}
	if args.Term > n.term {
		if err := n.becomeFollower(args.Term); err != nil {
			return RequestVoteReply{}, err
		}
	}
	last := n.lastIndex()
	upToDate := args.LastLogTerm > n.termAt(last) ||
		args.LastLogTerm == n.termAt(last) && args.LastLogIndex >= last
	if (n.votedFor != "" && n.votedFor != args.Candidate) || !upToDate {
		return RequestVoteReply{Term: n.term}, nil
	}
	if err := n.setTermVote(n.term, args.Candidate); err != nil {
		return RequestVoteReply{}, err
	}
	n.resetDeadline()
	return RequestVoteReply{Term: n.term, Granted: true}, nil
}

// HandleAppendEntries serves an AppendEntries RPC. An error means no reply must be sent.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) (AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() != nil {
		return AppendEntriesReply{}, context.Cause(n.ctx)
	}
	if args.Term < n.term {
		return AppendEntriesReply{Term: n.term}, nil
	}
	if err := n.becomeFollower(args.Term); err != nil {
		return AppendEntriesReply{}, err
	}
	n.setLeader(args.Leader)
	n.resetDeadline()

	reply := AppendEntriesReply{Term: n.term}
	if last := n.lastIndex(); args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1
		return reply, nil
	}
	// entries the snapshot covers are committed, so they match whatever the leader sends for them
	if args.PrevLogIndex >= n.snapIndex {
		if t := n.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
			i := args.PrevLogIndex
			for i > n.snapIndex+1 && n.termAt(i-1) == t {
				i--
			}
			reply.ConflictTerm, reply.ConflictIndex = t, i
			return reply, nil
		}
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.snapIndex || idx <= n.lastIndex() && n.termAt(idx) == e.Term {
			continue
		}
		if err := n.appendLog(idx, args.Entries[i:]); err != nil {
			return AppendEntriesReply{}, err
		}
		break
	}
	if c := min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries))); c > n.commitIndex {
		n.commitIndex = c
		n.applyCond.Broadcast()
	}
	reply.Success = true
	return reply, nil
}

// HandleInstallSnapshot serves an InstallSnapshot RPC, replying once the snapshot is durable. An error means no reply must be sent.
func (n *Node) HandleInstallSnapshot(args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() != nil {
		return InstallSnapshotReply{}, context.Cause(n.ctx)
	}
	if args.Term < n.term {
		return InstallSnapshotReply{Term: n.term}, nil
	}
	if n.restore == nil {
		return InstallSnapshotReply{}, errors.New("consensus: snapshot sent to a node without Restore")
	}
	if err := n.becomeFollower(args.Term); err != nil {
		return InstallSnapshotReply{}, err
	}
	n.setLeader(args.Leader)
	n.resetDeadline()

	reply := InstallSnapshotReply{Term: n.term}
	if args.LastIndex <= n.commitIndex {
		return reply, nil
	}
	tmp := filepath.Join(n.dir, "snapshot.recv.tmp")
	if err := writeSnapshot(tmp, args.LastIndex, args.LastTerm, args.Data); err != nil {
		err = fmt.Errorf("write received raft snapshot: %w", err)
		n.halt(err)
		return InstallSnapshotReply{}, err
	}
	if args.LastIndex > n.lastIndex() || n.termAt(args.LastIndex) != args.LastTerm {
		for i, done := range n.waiters {
			if i > args.LastIndex {
				done <- outcome{err: ErrProposalDropped}
				delete(n.waiters, i)
			}
		}
	}
	if err := n.compact(tmp, args.LastIndex, args.LastTerm); err != nil {
		return InstallSnapshotReply{}, err
	}
	n.commitIndex = args.LastIndex
	n.pending = &args
	n.applyCond.Broadcast()
	return reply, nil
}

// compact durably makes the snapshot written to tmp this node's, keeping the log after it only if the log holds its last entry; mu must be held
func (n *Node) compact(tmp string, index, term uint64) error {
	if err := renameSnapshot(n.dir, tmp); err != nil {
		n.halt(err)
		return err
	}
	var tail []Entry
	if index <= n.lastIndex() && n.termAt(index) == term {
		tail = n.log[index-n.snapIndex+1:]
	}
	n.log = append([]Entry{{Term: term}}, tail...)
	n.snapIndex = index

	err := n.wal.Close()
	n.wal = nil
	if err == nil {
		n.wal, err = wal.Rewrite(filepath.Join(n.dir, "log"), logRecords(n.snapIndex+1, n.log[1:]))
	}
	if err != nil {
		err = fmt.Errorf("rewrite raft log: %w", err)
		n.halt(err)
		return err
	}
	return nil
}

func (n *Node) lastIndex() uint64 { return n.snapIndex + uint64(len(n.log)-1) }

// termAt is the term of entry i, from snapIndex to lastIndex; mu must be held
func (n *Node) termAt(i uint64) uint64 { return n.log[i-n.snapIndex].Term }

func (n *Node) majority() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) resetDeadline() {
	n.deadline = time.Now().Add(n.election + rand.N(n.election))
}

// halt stops the node with cause; mu must be held
func (n *Node) halt(cause error) {
	if n.ctx.Err() == nil && !errors.Is(cause, ErrStopped) {
		slog.Error("raft node halted", "node", n.id, "err", cause)
	}
	n.cancel(cause)
	n.applyCond.Broadcast()
}

func (n *Node) setTermVote(term uint64, vote string) error {
	if term == n.term && vote == n.votedFor {
		return nil
	}
	if err := writeState(n.dir, term, vote); err != nil {
		n.halt(err)
		return err
	}
	n.term, n.votedFor = term, vote
	return nil
}

// appendLog durably replaces the log from index start onwards with entries
func (n *Node) appendLog(start uint64, entries []Entry) error {
	if err := n.wal.Append(encodeBatch(start, entries)); err != nil {
		err = fmt.Errorf("append raft log: %w", err)
		n.halt(err)
		return err
	}
	for i, done := range n.waiters {
		if i >= start {
			done <- outcome{err: ErrProposalDropped}
			delete(n.waiters, i)
		}
	}
	n.log = append(n.log[:start-n.snapIndex], entries...)
	return nil
}

func (n *Node) becomeFollower(term uint64) error {
	if n.role == Leader {
		slog.Info("stepped down", "node", n.id, "term", n.term)
		n.resetDeadline()
	}
	n.role = Follower
	if term > n.term {
		n.leader = ""
		return n.setTermVote(term, "")
	}
	return nil
}

// setLeader records the leader of the current term, logging and reporting a change; mu must be held
func (n *Node) setLeader(leader string) {
	if leader == n.leader {
		return
	}
	n.leader = leader
	if leader == n.id {
		slog.Info("became leader", "node", n.id, "term", n.term)
	} else {
		slog.Info("following leader", "node", n.id, "term", n.term, "leader", leader)
	}
	if n.onLeaderChange != nil {
		n.onLeaderChange(n.term, leader)
	}
}

func (n *Node) runTimer() {
	t := time.NewTicker(n.election / 10)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		n.mu.Lock()
		switch {
		case n.ctx.Err() != nil:
		case n.role == Leader:
			n.checkQuorum()
		case time.Now().After(n.deadline):
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// checkQuorum steps down a leader that has not heard from a majority for an election timeout; mu must be held
func (n *Node) checkQuorum() {
	heard := 1
	for _, p := range n.peers {
		if time.Since(n.contact[p]) < n.election {
			heard++
		}
	}
	if heard >= n.majority() {
		return
	}
	if err := n.becomeFollower(n.term); err != nil {
		return
	}
	n.leader = ""
	for i, done := range n.waiters {
		done <- outcome{err: ErrLeadershipLost}
		delete(n.waiters, i)
	}
	close(n.applied)
	n.applied = make(chan struct{})
}

func (n *Node) startElection() {
	if err := n.setTermVote(n.term+1, n.id); err != nil {
		return
	}
	n.role = Candidate
	n.leader = ""
	n.votes = 1
	n.resetDeadline()
	if n.votes >= n.majority() {
		n.becomeLeader()
		return
	}
	last := n.lastIndex()
	args := RequestVoteArgs{Term: n.term, Candidate: n.id, LastLogIndex: last, LastLogTerm: n.termAt(last)}
	for _, p := range n.peers {
		n.wg.Go(func() { n.requestVote(p, args) })
	}
}

func (n *Node) requestVote(peer string, args RequestVoteArgs) {
	ctx, cancel := context.WithTimeout(n.ctx, n.election)
	defer cancel()
	reply, err := n.tr.RequestVote(ctx, peer, args)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() != nil {
		return
	}
	if reply.Term > n.term {
		if err := n.becomeFollower(reply.Term); err != nil {
			return
		}
	}
	if n.role != Candidate || n.term != args.Term || !reply.Granted {
		return
	}
	n.votes++
	if n.votes >= n.majority() {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.setLeader(n.id)
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.contact = map[string]time.Time{}
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.contact[p] = time.Now()
	}
	if err := n.appendLog(n.lastIndex()+1, []Entry{{Term: n.term}}); err != nil {
		return
	}
	n.advanceCommit()
	n.wakeAll()
}

func (n *Node) wakeAll() {
	for _, p := range n.peers {
		n.wakeup(p)
	}
}

func (n *Node) wakeup(peer string) {
	select {
	case n.wake[peer] <- struct{}{}:
	default:
	}
}

// advanceCommit commits by counting replicas only for entries of the current term (figure 8)
func (n *Node) advanceCommit() {
	for i := n.lastIndex(); i > n.commitIndex && n.termAt(i) == n.term; i-- {
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= i {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = i
			n.applyCond.Broadcast()
			return
		}
	}
}

// replicate sends entries to peer when woken, by a proposal, a new leadership, its own follow-up or a heartbeat reply from a lagging or wiped peer
func (n *Node) replicate(peer string, wake chan struct{}) {
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-wake:
		}

		n.mu.Lock()
		if n.ctx.Err() != nil || n.role != Leader {
			n.mu.Unlock()
			continue
		}
		next := n.nextIndex[peer]
		if next <= n.snapIndex {
			term, dir := n.term, n.dir
			n.mu.Unlock()
			n.sendSnapshot(peer, term, dir)
			continue
		}
		args := AppendEntriesArgs{
			Term:         n.term,
			Leader:       n.id,
			PrevLogIndex: next - 1,
			PrevLogTerm:  n.termAt(next - 1),
			Entries:      slices.Clone(n.batch(next)),
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		// ponytail: fixed timeout so a big batch can finish transferring and syncing; scale it with batch bytes if peers get slower
		ctx, cancel := context.WithTimeout(n.ctx, 10*n.election)
		reply, err := n.tr.AppendEntries(ctx, peer, args)
		cancel()
		if err != nil {
			continue
		}

		n.mu.Lock()
		n.handleAppendReply(peer, args, reply)
		n.mu.Unlock()
	}
}

// sendSnapshot sends the snapshot on disk to a peer whose next entry it covers, as leader of term
func (n *Node) sendSnapshot(peer string, term uint64, dir string) {
	index, lastTerm, data, err := readSnapshot(dir)
	if err != nil {
		n.mu.Lock()
		n.halt(fmt.Errorf("read raft snapshot to send: %w", err))
		n.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{Term: term, Leader: n.id, LastIndex: index, LastTerm: lastTerm, Data: data}
	ctx, cancel := context.WithTimeout(n.ctx, 10*n.election)
	reply, err := n.tr.InstallSnapshot(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.recordReply(peer, term, reply.Term) {
		return
	}
	if index > n.matchIndex[peer] {
		n.matchIndex[peer] = index
		n.advanceCommit()
	}
	n.nextIndex[peer] = index + 1
	if n.nextIndex[peer] <= n.lastIndex() {
		n.wakeup(peer)
	}
}

// heartbeats sends an empty AppendEntries to peer every heartbeat interval, whatever replicate is doing
func (n *Node) heartbeats(peer string) {
	t := time.NewTicker(n.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}

		n.mu.Lock()
		if n.ctx.Err() != nil || n.role != Leader {
			n.mu.Unlock()
			continue
		}
		match := n.matchIndex[peer]
		// a peer behind the snapshot fails the check but still resets its election timer
		prev := max(match, n.snapIndex)
		args := AppendEntriesArgs{
			Term:         n.term,
			Leader:       n.id,
			PrevLogIndex: prev,
			PrevLogTerm:  n.termAt(prev),
			LeaderCommit: min(match, n.commitIndex),
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(n.ctx, n.election)
		reply, err := n.tr.AppendEntries(ctx, peer, args)
		cancel()
		if err != nil {
			continue
		}

		n.mu.Lock()
		if n.recordReply(peer, args.Term, reply.Term) && (!reply.Success || n.matchIndex[peer] < n.lastIndex()) {
			n.wakeup(peer)
		}
		n.mu.Unlock()
	}
}

// batch returns the entries from next, at most maxBatch of them and maxBatchBytes in total, but at least one if any remain; mu must be held
func (n *Node) batch(next uint64) []Entry {
	end, size := next, 0
	for end <= n.lastIndex() && end-next < maxBatch {
		size += len(n.log[end-n.snapIndex].Cmd)
		if end > next && size > maxBatchBytes {
			break
		}
		end++
	}
	return n.log[next-n.snapIndex : end-n.snapIndex]
}

// recordReply steps down on a higher term and records contact, reporting whether a reply to a request sent in term answers this leadership; mu must be held
func (n *Node) recordReply(peer string, term, replyTerm uint64) bool {
	if n.ctx.Err() != nil {
		return false
	}
	if replyTerm > n.term {
		if err := n.becomeFollower(replyTerm); err != nil {
			return false
		}
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.contact[peer] = time.Now()
	return true
}

func (n *Node) handleAppendReply(peer string, args AppendEntriesArgs, reply AppendEntriesReply) {
	if !n.recordReply(peer, args.Term, reply.Term) {
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.advanceCommit()
		}
		n.nextIndex[peer] = match + 1
	} else {
		n.nextIndex[peer] = n.nextAfterConflict(reply)
	}
	if n.nextIndex[peer] <= n.lastIndex() && (reply.Success || n.nextIndex[peer] <= args.PrevLogIndex) {
		n.wakeup(peer)
	}
}

func (n *Node) nextAfterConflict(r AppendEntriesReply) uint64 {
	if r.ConflictTerm != 0 {
		for i := n.lastIndex(); i > n.snapIndex && n.termAt(i) >= r.ConflictTerm; i-- {
			if n.termAt(i) == r.ConflictTerm {
				return i + 1
			}
		}
	}
	return max(1, min(r.ConflictIndex, n.lastIndex()+1))
}

func (n *Node) runApplier() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for n.ctx.Err() == nil && n.pending == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.ctx.Err() != nil {
			return
		}
		if p := n.pending; p != nil {
			n.pending = nil
			n.mu.Unlock()
			err := n.restore(p.Data)
			n.mu.Lock()
			if err != nil {
				n.halt(fmt.Errorf("restore raft snapshot at %d: %w", p.LastIndex, err))
				return
			}
			n.lastApplied = p.LastIndex
			n.notifyApplied(0, nil, ErrLeadershipLost)
			continue
		}
		start := n.lastApplied + 1
		entries := slices.Clone(n.log[start-n.snapIndex : n.commitIndex-n.snapIndex+1])
		results := make([][]byte, len(entries))
		n.mu.Unlock()
		for i, e := range entries {
			if len(e.Cmd) > 0 {
				results[i] = n.apply(start+uint64(i), e.Cmd)
			}
		}
		n.mu.Lock()
		n.lastApplied = start + uint64(len(entries)) - 1
		n.notifyApplied(start, results, nil)
		if n.snapshot != nil && n.pending == nil && n.lastApplied >= n.snapIndex+n.snapEntries {
			n.takeSnapshot()
		}
	}
}

// notifyApplied wakes readers and answers the waiters up to lastApplied with err, or with results of the entries from start; mu must be held
func (n *Node) notifyApplied(start uint64, results [][]byte, err error) {
	close(n.applied)
	n.applied = make(chan struct{})
	for i, done := range n.waiters {
		if i <= n.lastApplied {
			o := outcome{err: err}
			if err == nil {
				o.result = results[i-start]
			}
			done <- o
			delete(n.waiters, i)
		}
	}
}

// takeSnapshot snapshots the state machine at lastApplied and compacts the log, encoding and writing it without mu; mu must be held
func (n *Node) takeSnapshot() {
	index, term := n.lastApplied, n.termAt(n.lastApplied)
	tmp := filepath.Join(n.dir, "snapshot.tmp")
	n.mu.Unlock()
	err := writeSnapshot(tmp, index, term, n.snapshot())
	n.mu.Lock()
	if err != nil {
		n.halt(fmt.Errorf("write raft snapshot: %w", err))
		return
	}
	if n.ctx.Err() != nil || n.snapIndex >= index {
		return
	}
	_ = n.compact(tmp, index, term)
}

// ReadIndex returns once a majority has confirmed this node still leads and everything committed
// before the call is applied here, so a read of the state machine afterwards is linearizable.
// ponytail: one heartbeat round per read; batch concurrent reads onto one round if read throughput matters
func (n *Node) ReadIndex(ctx context.Context) error {
	n.mu.Lock()
	if n.ctx.Err() != nil {
		n.mu.Unlock()
		return context.Cause(n.ctx)
	}
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		return &NotLeaderError{Leader: leader}
	}
	term, idx := n.term, n.readIndex()
	acks := make(chan bool, len(n.peers))
	args := AppendEntriesArgs{Term: term, Leader: n.id}
	for _, p := range n.peers {
		n.wg.Go(func() { acks <- n.confirm(p, args) })
	}
	n.mu.Unlock()

	confirmed := 1
	for range n.peers {
		if confirmed >= n.majority() {
			break
		}
		select {
		case ok := <-acks:
			if ok {
				confirmed++
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return context.Cause(n.ctx)
		}
	}
	if confirmed < n.majority() {
		return ErrNoQuorum
	}

	for {
		n.mu.Lock()
		if n.role != Leader || n.term != term {
			leader := n.leader
			n.mu.Unlock()
			return &NotLeaderError{Leader: leader}
		}
		if n.lastApplied >= idx {
			n.mu.Unlock()
			return nil
		}
		applied := n.applied
		n.mu.Unlock()
		select {
		case <-applied:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return context.Cause(n.ctx)
		}
	}
}

// readIndex is the commit index, or the last index until the leader has committed in its own term; mu must be held
func (n *Node) readIndex() uint64 {
	if n.termAt(n.commitIndex) == n.term {
		return n.commitIndex
	}
	return n.lastIndex()
}

// confirm sends an empty AppendEntries that any follower of this term accepts without change, reporting whether it acked the term
func (n *Node) confirm(peer string, args AppendEntriesArgs) bool {
	ctx, cancel := context.WithTimeout(n.ctx, n.election)
	defer cancel()
	reply, err := n.tr.AppendEntries(ctx, peer, args)
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() == nil && reply.Term > n.term {
		if err := n.becomeFollower(reply.Term); err != nil {
			return false
		}
	}
	if n.role == Leader && n.term == args.Term && reply.Term == args.Term {
		n.contact[peer] = time.Now()
	}
	return reply.Term == args.Term
}
