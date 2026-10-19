package consensus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"
)

const catchUpRounds = 10

var (
	ErrChangeInProgress = errors.New("consensus: another membership change is in progress")
	ErrMemberExists     = errors.New("consensus: already a member")
	ErrNotMember        = errors.New("consensus: not a member")
	ErrLastMember       = errors.New("consensus: cannot remove the last member")
	ErrLeaderNotReady   = errors.New("consensus: leader has not committed an entry in its term")
	ErrCatchUpTimeout   = errors.New("consensus: new member did not catch up")
)

// Member is a node of the cluster. Addr is opaque to consensus.
type Member struct {
	ID, Addr string
}

// peer is a node the leader replicates to: a member other than this one, or the learner
type peer struct {
	id     string
	wake   chan struct{}
	cancel context.CancelFunc
	// next, match and contact are the leader's view of the peer in its term
	next, match uint64
	contact     time.Time
}

// Members is the configuration this node uses: the latest config entry in its log, committed or not.
func (n *Node) Members() []Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.members)
}

// AddMember adds m to the cluster. It must be called on the leader: m first catches up as a learner the leader
// replicates to but does not count, then joins the configuration. It returns once the new configuration commits;
// if ctx ends first the change may still commit.
func (n *Node) AddMember(ctx context.Context, m Member) error {
	if m.ID == "" {
		return errors.New("consensus: member id is empty")
	}
	n.mu.Lock()
	if err := n.checkChange(); err != nil {
		n.mu.Unlock()
		return err
	}
	if n.isMember(m.ID) {
		n.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrMemberExists, m.ID)
	}
	term := n.term
	n.learner = m.ID
	n.syncPeers()
	p := n.peers[m.ID]
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.learner == m.ID {
			n.learner = ""
			n.syncPeers()
		}
	}()

	if err := n.catchUp(ctx, p, term); err != nil {
		return err
	}
	n.mu.Lock()
	if err := n.leadingErr(term); err != nil {
		n.mu.Unlock()
		return err
	}
	done, err := n.appendConfig(append(slices.Clone(n.members), m))
	n.mu.Unlock()
	if err != nil {
		return err
	}
	return n.awaitCommit(ctx, done)
}

// RemoveMember removes the member id from the cluster. It must be called on the leader, which may remove itself:
// it then leads until the change commits and steps down. It returns once the new configuration commits;
// if ctx ends first the change may still commit.
func (n *Node) RemoveMember(ctx context.Context, id string) error {
	n.mu.Lock()
	if err := n.checkChange(); err != nil {
		n.mu.Unlock()
		return err
	}
	if !n.isMember(id) {
		n.mu.Unlock()
		return fmt.Errorf("%w: %q", ErrNotMember, id)
	}
	if len(n.members) == 1 {
		n.mu.Unlock()
		return ErrLastMember
	}
	rest := slices.DeleteFunc(slices.Clone(n.members), func(m Member) bool { return m.ID == id })
	done, err := n.appendConfig(rest)
	n.mu.Unlock()
	if err != nil {
		return err
	}
	return n.awaitCommit(ctx, done)
}

func (n *Node) awaitCommit(ctx context.Context, done chan outcome) error {
	select {
	case o := <-done:
		return o.err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ctx.Done():
		return context.Cause(n.ctx)
	}
}

// checkChange reports why this node cannot start a membership change; mu must be held
func (n *Node) checkChange() error {
	if n.ctx.Err() != nil {
		return context.Cause(n.ctx)
	}
	if n.role != Leader {
		return &NotLeaderError{Leader: n.leader}
	}
	if n.termAt(n.commitIndex) != n.term {
		return ErrLeaderNotReady
	}
	if n.cfgIndex > n.commitIndex || n.learner != "" {
		return ErrChangeInProgress
	}
	return nil
}

// leadingErr is nil while this node still leads in term; mu must be held
func (n *Node) leadingErr(term uint64) error {
	if n.ctx.Err() != nil {
		return context.Cause(n.ctx)
	}
	if n.role != Leader || n.term != term {
		return &NotLeaderError{Leader: n.leader}
	}
	return nil
}

// catchUp waits for the learner p to hold everything the leader's log held at the start of a round, rounds being
// at most an election timeout, so that the learner is close enough that counting it cannot stall commits
func (n *Node) catchUp(ctx context.Context, p *peer, term uint64) error {
	for range catchUpRounds {
		n.mu.Lock()
		err := n.leadingErr(term)
		target := n.lastIndex()
		n.mu.Unlock()
		if err != nil {
			return err
		}
		end := time.Now().Add(n.election)
		for {
			n.mu.Lock()
			err := n.leadingErr(term)
			caught := p.match >= target
			n.mu.Unlock()
			if err != nil {
				return err
			}
			if caught {
				return nil
			}
			if !time.Now().Before(end) {
				break
			}
			select {
			case <-time.After(n.heartbeat / 2):
			case <-ctx.Done():
				return ctx.Err()
			case <-n.ctx.Done():
				return context.Cause(n.ctx)
			}
		}
	}
	return ErrCatchUpTimeout
}

// appendConfig appends a config entry for members to the leader's log and returns the channel that answers when it is applied; mu must be held
func (n *Node) appendConfig(members []Member) (chan outcome, error) {
	idx := n.lastIndex() + 1
	if err := n.appendLog(idx, []Entry{{Term: n.term, Kind: EntryConfig, Cmd: encodeMembers(members)}}); err != nil {
		return nil, err
	}
	done := make(chan outcome, 1)
	n.waiters[idx] = done
	n.advanceCommit()
	n.wakeAll()
	return done, nil
}

func (n *Node) isMember(id string) bool {
	return slices.ContainsFunc(n.members, func(m Member) bool { return m.ID == id })
}

// selfVote is 1 if this node is in its configuration, so it counts toward a majority, else 0
func (n *Node) selfVote() int {
	if n.isMember(n.id) {
		return 1
	}
	return 0
}

// loadConfig uses the latest config entry in the log, else the configuration of the snapshot; mu must be held
func (n *Node) loadConfig() {
	members, idx := n.base, uint64(0)
	for i := n.lastIndex(); i > n.snapIndex; i-- {
		if e := n.log[i-n.snapIndex]; e.Kind == EntryConfig {
			members, idx = membersOf(e), i
			break
		}
	}
	n.useConfig(members, idx)
}

// configAt is the configuration as of log index i, which the snapshot covers up to snapIndex; mu must be held
func (n *Node) configAt(i uint64) []Member {
	for ; i > n.snapIndex; i-- {
		if e := n.log[i-n.snapIndex]; e.Kind == EntryConfig {
			return membersOf(e)
		}
	}
	return n.base
}

// useConfig switches to members, the configuration of the config entry at index idx, 0 for none; mu must be held
func (n *Node) useConfig(members []Member, idx uint64) {
	n.cfgIndex = idx
	if slices.Equal(members, n.members) {
		return
	}
	n.members = members
	n.syncPeers()
	if n.onMembership != nil {
		n.onMembership(slices.Clone(members))
	}
}

// syncPeers starts replication to the members and the learner that have none, and stops it to peers that are neither; mu must be held
func (n *Node) syncPeers() {
	if n.ctx.Err() != nil {
		return
	}
	want := map[string]bool{}
	for _, m := range n.members {
		want[m.ID] = m.ID != n.id
	}
	if n.learner != "" {
		want[n.learner] = true
	}
	for id, p := range n.peers {
		if !want[id] {
			p.cancel()
			delete(n.peers, id)
		}
	}
	for id, ok := range want {
		if !ok || n.peers[id] != nil {
			continue
		}
		ctx, cancel := context.WithCancel(n.ctx)
		p := &peer{id: id, wake: make(chan struct{}, 1), cancel: cancel, next: n.lastIndex() + 1, contact: time.Now()}
		n.peers[id] = p
		n.wg.Go(func() { n.replicate(ctx, p) })
		n.wg.Go(func() { n.heartbeats(ctx, p) })
		if n.role == Leader {
			n.wakeup(p)
		}
	}
}

// checkMembers rejects empty and duplicate ids
func checkMembers(members []Member) error {
	seen := map[string]bool{}
	for _, m := range members {
		if m.ID == "" {
			return errors.New("consensus: member with an empty id")
		}
		if seen[m.ID] {
			return fmt.Errorf("consensus: duplicate member %q", m.ID)
		}
		seen[m.ID] = true
	}
	return nil
}

// checkEntries rejects entries of an unknown kind and config entries that do not decode
func checkEntries(entries []Entry) error {
	for i, e := range entries {
		switch e.Kind {
		case EntryNormal:
		case EntryConfig:
			if _, err := decodeMembers(e.Cmd); err != nil {
				return fmt.Errorf("config entry %d of the batch: %w", i, err)
			}
		default:
			return fmt.Errorf("entry %d of the batch has unknown kind %d", i, e.Kind)
		}
	}
	return nil
}

// lastConfig is the position of the last config entry in entries, -1 for none
func lastConfig(entries []Entry) int {
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == EntryConfig {
			return i
		}
	}
	return -1
}

// membersOf decodes a config entry that checkEntries accepted
func membersOf(e Entry) []Member {
	m, err := decodeMembers(e.Cmd)
	if err != nil {
		panic(fmt.Sprintf("config entry in the log does not decode: %v", err))
	}
	return m
}

// members encoding: uvarint count | (uvarint id length | id | uvarint addr length | addr)*
func appendMembers(b []byte, members []Member) []byte {
	b = binary.AppendUvarint(b, uint64(len(members)))
	for _, m := range members {
		b = binary.AppendUvarint(b, uint64(len(m.ID)))
		b = append(b, m.ID...)
		b = binary.AppendUvarint(b, uint64(len(m.Addr)))
		b = append(b, m.Addr...)
	}
	return b
}

func encodeMembers(members []Member) []byte { return appendMembers(nil, members) }

func decodeMembers(b []byte) ([]Member, error) {
	members, rest, err := consumeMembers(b)
	if err != nil {
		return nil, err
	}
	if len(rest) > 0 {
		return nil, fmt.Errorf("%d bytes after the members", len(rest))
	}
	return members, nil
}

// consumeMembers decodes a members encoding from the front of b, returning the rest
func consumeMembers(b []byte) ([]Member, []byte, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 || count > uint64(len(b)) {
		return nil, nil, errors.New("bad member count")
	}
	b = b[n:]
	var members []Member
	for range count {
		var m Member
		var err error
		if m.ID, b, err = consumeString(b); err != nil {
			return nil, nil, fmt.Errorf("member id: %w", err)
		}
		if m.Addr, b, err = consumeString(b); err != nil {
			return nil, nil, fmt.Errorf("member addr: %w", err)
		}
		members = append(members, m)
	}
	return members, b, nil
}

func consumeString(b []byte) (string, []byte, error) {
	size, n := binary.Uvarint(b)
	if n <= 0 || size > uint64(len(b)-n) {
		return "", nil, errors.New("bad length")
	}
	b = b[n:]
	return string(b[:size]), b[size:], nil
}
