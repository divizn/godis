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
)

// NotLeaderError carries the current leader's id, empty if unknown.
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

// Transport sends Raft RPCs to a peer, served there by HandleRequestVote and HandleAppendEntries.
type Transport interface {
	RequestVote(ctx context.Context, to string, args RequestVoteArgs) (RequestVoteReply, error)
	AppendEntries(ctx context.Context, to string, args AppendEntriesArgs) (AppendEntriesReply, error)
}

type Config struct {
	ID string
	// Peers lists every member, including ID.
	Peers     []string
	Dir       string
	Transport Transport
	// Apply gets committed commands in order, once per process lifetime (the whole log again after a restart).
	Apply func(index uint64, cmd []byte)
	// ElectionTimeout is randomized up to twice its value, default 150ms.
	ElectionTimeout time.Duration
	// HeartbeatInterval defaults to 50ms and must be below ElectionTimeout.
	HeartbeatInterval time.Duration
	// OnLeaderChange is called with the node's lock held each time the leader it knows changes to a known one,
	// so at most once per term; it must not block or call into the Node.
	OnLeaderChange func(term uint64, leader string)
}

type Status struct {
	ID          string
	Role        Role
	Term        uint64
	Leader      string
	CommitIndex uint64
}

type Node struct {
	id             string
	peers          []string
	dir            string
	tr             Transport
	apply          func(uint64, []byte)
	onLeaderChange func(uint64, string)
	election       time.Duration
	heartbeat      time.Duration
	wake           map[string]chan struct{}

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	// ponytail: one lock over all state, held across fsyncs; split it and add group commit if a benchmark asks
	mu          sync.Mutex
	applyCond   *sync.Cond
	wal         *wal.Log
	role        Role
	term        uint64
	votedFor    string
	leader      string
	log         []Entry
	commitIndex uint64
	lastApplied uint64
	deadline    time.Time
	votes       int
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	// contact is when the leader last had a reply in its term from each peer
	contact map[string]time.Time
	waiters map[uint64]chan error
	// applied is closed and replaced each time lastApplied advances or the node stops leading
	applied chan struct{}
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
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir: %w", err)
	}
	term, vote, err := readState(cfg.Dir)
	if err != nil {
		return nil, err
	}

	n := &Node{
		id:             cfg.ID,
		peers:          slices.DeleteFunc(slices.Clone(cfg.Peers), func(p string) bool { return p == cfg.ID }),
		dir:            cfg.Dir,
		tr:             cfg.Transport,
		apply:          cfg.Apply,
		onLeaderChange: cfg.OnLeaderChange,
		election:       cfg.ElectionTimeout,
		heartbeat:      cfg.HeartbeatInterval,
		wake:           map[string]chan struct{}{},
		term:           term,
		votedFor:       vote,
		log:            []Entry{{}},
		waiters:        map[uint64]chan error{},
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
	}
	return n, nil
}

// ponytail: no snapshots, so the log file, memory and restart replay grow with all history; add compaction when that hurts
func (n *Node) replay(rec []byte) error {
	start, entries, err := decodeBatch(rec)
	if err != nil {
		return fmt.Errorf("decode raft log batch: %w", err)
	}
	if start == 0 || start > uint64(len(n.log)) {
		return fmt.Errorf("raft log batch starts at %d but log ends at %d", start, len(n.log)-1)
	}
	n.log = append(n.log[:start], entries...)
	return nil
}

// Propose returns once cmd is committed and applied here; if ctx ends first it may still commit later.
func (n *Node) Propose(ctx context.Context, cmd []byte) error {
	if len(cmd) == 0 {
		return ErrEmptyCommand
	}
	if len(cmd) > maxCommandSize {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrCommandTooLarge, len(cmd), maxCommandSize)
	}
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
	idx := n.lastIndex() + 1
	if err := n.appendLog(idx, []Entry{{Term: n.term, Cmd: slices.Clone(cmd)}}); err != nil {
		n.mu.Unlock()
		return err
	}
	done := make(chan error, 1)
	n.waiters[idx] = done
	n.advanceCommit()
	n.wakeAll()
	n.mu.Unlock()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		if n.waiters[idx] == done {
			delete(n.waiters, idx)
		}
		n.mu.Unlock()
		return ctx.Err()
	case <-n.ctx.Done():
		return context.Cause(n.ctx)
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.id, Role: n.role, Term: n.term, Leader: n.leader, CommitIndex: n.commitIndex}
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
	upToDate := args.LastLogTerm > n.log[last].Term ||
		args.LastLogTerm == n.log[last].Term && args.LastLogIndex >= last
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
	if t := n.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		i := args.PrevLogIndex
		for i > 1 && n.log[i-1].Term == t {
			i--
		}
		reply.ConflictTerm, reply.ConflictIndex = t, i
		return reply, nil
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= n.lastIndex() && n.log[idx].Term == e.Term {
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

func (n *Node) lastIndex() uint64 { return uint64(len(n.log) - 1) }

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
			done <- ErrProposalDropped
			delete(n.waiters, i)
		}
	}
	n.log = append(n.log[:start], entries...)
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
		done <- &NotLeaderError{}
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
	args := RequestVoteArgs{Term: n.term, Candidate: n.id, LastLogIndex: last, LastLogTerm: n.log[last].Term}
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
	for i := n.lastIndex(); i > n.commitIndex && n.log[i].Term == n.term; i-- {
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

func (n *Node) replicate(peer string, wake chan struct{}) {
	t := time.NewTicker(n.heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		case <-wake:
		}

		n.mu.Lock()
		if n.ctx.Err() != nil || n.role != Leader {
			n.mu.Unlock()
			continue
		}
		next := n.nextIndex[peer]
		args := AppendEntriesArgs{
			Term:         n.term,
			Leader:       n.id,
			PrevLogIndex: next - 1,
			PrevLogTerm:  n.log[next-1].Term,
			Entries:      slices.Clone(n.batch(next)),
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(n.ctx, n.election)
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

// batch returns the entries from next, at most maxBatch of them and maxBatchBytes in total, but at least one if any remain; mu must be held
func (n *Node) batch(next uint64) []Entry {
	end, size := next, 0
	for end <= n.lastIndex() && end-next < maxBatch {
		size += len(n.log[end].Cmd)
		if end > next && size > maxBatchBytes {
			break
		}
		end++
	}
	return n.log[next:end]
}

func (n *Node) handleAppendReply(peer string, args AppendEntriesArgs, reply AppendEntriesReply) {
	if n.ctx.Err() != nil {
		return
	}
	if reply.Term > n.term {
		if err := n.becomeFollower(reply.Term); err != nil {
			return
		}
	}
	if n.role != Leader || n.term != args.Term {
		return
	}
	n.contact[peer] = time.Now()
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
		for i := n.lastIndex(); i > 0 && n.log[i].Term >= r.ConflictTerm; i-- {
			if n.log[i].Term == r.ConflictTerm {
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
		for n.ctx.Err() == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.ctx.Err() != nil {
			return
		}
		start := n.lastApplied + 1
		entries := slices.Clone(n.log[start : n.commitIndex+1])
		n.mu.Unlock()
		for i, e := range entries {
			if len(e.Cmd) > 0 {
				n.apply(start+uint64(i), e.Cmd)
			}
		}
		n.mu.Lock()
		n.lastApplied = start + uint64(len(entries)) - 1
		close(n.applied)
		n.applied = make(chan struct{})
		for i, done := range n.waiters {
			if i <= n.lastApplied {
				done <- nil
				delete(n.waiters, i)
			}
		}
	}
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
	if n.log[n.commitIndex].Term == n.term {
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
