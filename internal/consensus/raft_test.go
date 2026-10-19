package consensus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/divizn/godis/internal/vfs"
	"github.com/divizn/godis/internal/wal"
)

func TestOneLeaderPerTerm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()

		c.net.setFaults(0, 5*time.Millisecond)

		for range 10 {
			old, term := c.waitLeader()
			rest := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == old })
			c.net.partition([]string{old}, rest)
			waitFor(t, 10*time.Second, "a new leader", func() bool {
				_, newTerm := c.leaderAmong(rest...)
				return newTerm > term
			})
			c.net.heal()
		}
		c.checkOneLeaderPerTerm()
		if n := c.net.leaderTerms(); n < 10 {
			t.Fatalf("saw leaders in %d terms, want at least 10", n)
		}
	})
}

func singleNode(t *testing.T, dir string) *Node {
	t.Helper()
	n, err := New(Config{
		ID:                "a",
		Members:           ids("a", "b", "c"),
		Dir:               dir,
		Transport:         memTransport{net: newMemNet(1), from: "a"},
		Apply:             func(uint64, []byte) []byte { return nil },
		ElectionTimeout:   time.Hour,
		HeartbeatInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func mustStop(t *testing.T, n *Node) {
	t.Helper()
	if err := n.Stop(); err != nil {
		t.Errorf("stop: %v", err)
	}
}

func vote(t *testing.T, n *Node, term uint64, candidate string) bool {
	t.Helper()
	r, err := n.HandleRequestVote(RequestVoteArgs{Term: term, Candidate: candidate})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	return r.Granted
}

func TestVoteOncePerTerm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		defer mustStop(t, n)
		if !vote(t, n, 5, "b") {
			t.Fatal("first vote in term 5 refused")
		}
		if vote(t, n, 5, "c") {
			t.Fatal("second vote in term 5 granted")
		}
		if !vote(t, n, 6, "c") {
			t.Fatal("vote in term 6 refused")
		}
	})
}

func TestVoteOncePerTermAcrossRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		n := singleNode(t, dir)
		if !vote(t, n, 5, "b") {
			t.Fatal("first vote in term 5 refused")
		}
		if err := n.Stop(); err != nil {
			t.Fatal(err)
		}

		n = singleNode(t, dir)
		defer mustStop(t, n)
		if got := n.Status().Term; got != 5 {
			t.Fatalf("term after restart: got %d, want 5", got)
		}
		if vote(t, n, 5, "c") {
			t.Fatal("second vote in term 5 granted after restart")
		}
	})
}

func appendEntries(t *testing.T, n *Node, args AppendEntriesArgs) AppendEntriesReply {
	t.Helper()
	r, err := n.HandleAppendEntries(args)
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}
	return r
}

func entries(term uint64, cmds ...string) []Entry {
	var es []Entry
	for _, c := range cmds {
		es = append(es, Entry{Term: term, Cmd: []byte(c)})
	}
	return es
}

func TestStaleAppendEntriesDoesNotTruncate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		defer mustStop(t, n)
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "x", "y", "z")})
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "x")})
		if r := appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 3, PrevLogTerm: 1}); !r.Success {
			t.Fatalf("entry 3 lost after a stale append: %+v", r)
		}
	})
}

func TestFollowerCommitsOnlyEntriesTheLeaderVouchedFor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		defer mustStop(t, n)
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "x", "y", "z")})
		appendEntries(t, n, AppendEntriesArgs{Term: 2, Leader: "c", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3})
		if got := n.Status().CommitIndex; got != 1 {
			t.Fatalf("commit index %d, want 1: entries 2 and 3 were never checked against the leader", got)
		}
	})
}

func TestSameTermVoteRequestKeepsLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		leader, term := c.waitLeader()
		other := c.ids[0]
		if other == leader {
			other = c.ids[1]
		}
		if _, err := c.node(leader).HandleRequestVote(RequestVoteArgs{Term: term, Candidate: other}); err != nil {
			t.Fatal(err)
		}
		if s := c.node(leader).Status(); s.Role != Leader || s.Term != term {
			t.Fatalf("leader of term %d became %v in term %d after a same-term vote request", term, s.Role, s.Term)
		}
	})
}

// heldVotes grants term 1 votes only once released, and refuses every later term
type heldVotes struct{ release chan struct{} }

func (h heldVotes) RequestVote(ctx context.Context, _ string, args RequestVoteArgs) (RequestVoteReply, error) {
	if args.Term != 1 {
		return RequestVoteReply{Term: args.Term}, nil
	}
	select {
	case <-h.release:
		return RequestVoteReply{Term: 1, Granted: true}, nil
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	}
}

func (heldVotes) AppendEntries(context.Context, string, AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, errUnreachable
}

func (heldVotes) InstallSnapshot(context.Context, string, InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, errUnreachable
}

func TestVotesFromAnEarlierElectionAreNotCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := heldVotes{release: make(chan struct{})}
		n, err := New(Config{
			ID:                "a",
			Members:           ids("a", "b", "c"),
			Dir:               t.TempDir(),
			Transport:         tr,
			Apply:             func(uint64, []byte) []byte { return nil },
			ElectionTimeout:   time.Hour,
			HeartbeatInterval: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer mustStop(t, n)
		for range 2 {
			n.mu.Lock()
			n.startElection()
			n.mu.Unlock()
			synctest.Wait()
		}
		close(tr.release)
		synctest.Wait()
		if s := n.Status(); s.Role == Leader {
			t.Fatalf("became leader of term %d on votes granted in term 1", s.Term)
		}
	})
}

func TestReelectionAfterLeaderStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		old, term := c.waitLeader()
		c.mustPropose(old, "a")
		c.stop(old)

		id, newTerm := c.waitLeader()
		if newTerm <= term {
			t.Fatalf("new leader term %d, want > %d", newTerm, term)
		}
		c.mustPropose(id, "b")

		c.start(old)
		c.waitApplied("b")
		if got, want := cmds(c.checkSame()), []string{"a", "b"}; !slices.Equal(got, want) {
			t.Fatalf("applied %v, want %v", got, want)
		}
	})
}

func TestLeaderWithoutQuorumStopsCommitting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()

		old, term := c.waitLeader()
		c.mustPropose(old, "a")
		var follower string
		var rest []string
		for _, id := range c.ids {
			switch {
			case id == old:
			case follower == "":
				follower = id
			default:
				rest = append(rest, id)
			}
		}
		c.net.partition([]string{old, follower}, rest)

		minority := make(chan error, 1)
		go func() { minority <- c.propose(old, "minority", time.Minute) }()
		id, newTerm := c.waitLeader(rest...)
		if newTerm <= term {
			t.Fatalf("new leader term %d, want > %d", newTerm, term)
		}
		c.mustPropose(id, "majority")
		if err := <-minority; !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("propose on leader without quorum: got %v, want ErrLeadershipLost", err)
		}

		c.net.heal()
		c.waitApplied("majority")
		if s := c.node(old).Status(); s.Role == Leader && s.Term == term {
			t.Fatalf("old leader still leads term %d after heal", term)
		}
		if got, want := cmds(c.checkSame()), []string{"a", "majority"}; !slices.Equal(got, want) {
			t.Fatalf("applied %v, want %v", got, want)
		}
	})
}

func TestMinorityCannotElectOrCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		c.mustPropose(leader, "a")
		c.waitApplied("a")
		var minority, majority []string
		for _, id := range c.ids {
			if id != leader && len(minority) < 2 {
				minority = append(minority, id)
			} else {
				majority = append(majority, id)
			}
		}
		commits := map[string]uint64{}
		for _, id := range minority {
			commits[id] = c.node(id).Status().CommitIndex
		}
		c.net.partition(minority, majority)

		for i := range 3 {
			c.mustPropose(leader, fmt.Sprint("m", i))
		}
		for range 100 {
			time.Sleep(testElection / 10)
			for _, id := range minority {
				s := c.node(id).Status()
				if s.Role == Leader {
					t.Fatalf("%s became leader of term %d in a minority", id, s.Term)
				}
				if s.CommitIndex != commits[id] {
					t.Fatalf("%s commit index moved from %d to %d in a minority", id, commits[id], s.CommitIndex)
				}
			}
		}

		c.net.heal()
		c.waitApplied(c.commitAny("end"))
		c.checkSame()
	})
}

// proposeIsolated appends cmds on a leader cut off from the rest, all at once so it has not yet stepped down
func proposeIsolated(t *testing.T, c *cluster, leader string, cmds ...string) {
	t.Helper()
	errs := make(chan error, len(cmds))
	for _, cmd := range cmds {
		go func() { errs <- c.propose(leader, cmd, 50*time.Millisecond) }()
	}
	for range cmds {
		if err := <-errs; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("propose on isolated leader %s: got %v, want deadline exceeded", leader, err)
		}
	}
}

// diverge leaves old leaders holding uncommitted entries from two terms, then heals
func diverge(t *testing.T, c *cluster) {
	t.Helper()
	l1, _ := c.waitLeader()
	c.mustPropose(l1, "a")
	others := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == l1 })
	c.net.partition([]string{l1}, others)
	proposeIsolated(t, c, l1, "x1", "x2", "x3")

	l2, _ := c.waitLeader(others...)
	c.mustPropose(l2, "b")
	rest := slices.DeleteFunc(slices.Clone(others), func(id string) bool { return id == l2 })
	c.net.partition([]string{l1}, []string{l2}, rest)
	proposeIsolated(t, c, l2, "y1", "y2")

	l3, _ := c.waitLeader(rest...)
	c.mustPropose(l3, "c")
	c.mustPropose(l3, "d")

	c.net.heal()
	c.waitApplied("d")
	if got, want := cmds(c.checkSame()), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
		t.Fatalf("applied %v, want %v", got, want)
	}
}

func TestDivergentLogsAreRepaired(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()
		diverge(t, c)

		id, _ := c.waitLeader()
		c.mustPropose(id, "e")
		c.waitApplied("e")
		c.checkSame()
	})
}

func TestFullRestartRestoresState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()
		diverge(t, c)

		before := c.checkSame()
		terms := map[string]uint64{}
		for _, id := range c.ids {
			terms[id] = c.node(id).Status().Term
		}
		c.stopAll()
		for _, id := range c.ids {
			c.start(id)
		}
		for _, id := range c.ids {
			if got := c.node(id).Status().Term; got < terms[id] {
				t.Fatalf("%s term after restart %d, want >= %d", id, got, terms[id])
			}
		}

		c.waitApplied(before[len(before)-1].cmd)
		id, _ := c.waitLeader()
		c.mustPropose(id, "e")
		c.waitApplied("e")
		after := c.checkSame()
		if want := append(before, after[len(after)-1]); !slices.Equal(after, want) {
			t.Fatalf("after restart applied %v, want %v", after, want)
		}
	})
}

func TestProposeOnFollowerNamesLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		follower := c.ids[0]
		if follower == leader {
			follower = c.ids[1]
		}
		waitFor(t, 10*time.Second, "follower to learn the leader", func() bool {
			return c.node(follower).Status().Leader == leader
		})

		err := c.propose(follower, "a", time.Second)
		var nle *NotLeaderError
		if !errors.As(err, &nle) {
			t.Fatalf("propose on follower: got %v, want *NotLeaderError", err)
		}
		if nle.Leader != leader {
			t.Fatalf("leader hint %q, want %q", nle.Leader, leader)
		}
	})
}

func TestLeaderCommitsOnlyCurrentTermByCounting(t *testing.T) {
	n := &Node{
		id:          "a",
		members:     ids("a", "b", "c", "d", "e"),
		peers:       map[string]*peer{"b": {match: 2}, "c": {match: 2}, "d": {match: 1}, "e": {match: 1}},
		role:        Leader,
		term:        4,
		log:         []Entry{{}, {Term: 1, Cmd: []byte("x")}, {Term: 2, Cmd: []byte("y")}, {Term: 4}},
		commitIndex: 1,
	}
	n.applyCond = sync.NewCond(&n.mu)

	n.advanceCommit()
	if n.commitIndex != 1 {
		t.Fatalf("committed index %d from term 2 by counting replicas in term 4", n.commitIndex)
	}
	n.peers["b"].match, n.peers["c"].match = 3, 3
	n.advanceCommit()
	if n.commitIndex != 3 {
		t.Fatalf("commit index %d, want 3 once a term 4 entry is on a majority", n.commitIndex)
	}
}

func TestStress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()
		c.net.setFaults(0.1, 3*time.Millisecond)

		var acked sync.Map
		var seq atomic.Int64
		var wg sync.WaitGroup
		for range 40 {
			switch r := rand.IntN(10); {
			case r < 3:
				ids := slices.Clone(c.ids)
				rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				cut := 1 + rand.IntN(len(ids)-1)
				c.net.partition(ids[:cut], ids[cut:])
			case r < 5:
				c.net.heal()
			case r < 7:
				id := c.ids[rand.IntN(len(c.ids))]
				c.stop(id)
				c.start(id)
			}
			for range 1 + rand.IntN(3) {
				wg.Go(func() {
					leader, _ := c.leaderAmong(c.live()...)
					if leader == "" {
						return
					}
					cmd := fmt.Sprint("c", seq.Add(1))
					if c.propose(leader, cmd, 300*time.Millisecond) == nil {
						acked.Store(cmd, true)
					}
				})
			}
			time.Sleep(time.Duration(rand.IntN(300)) * time.Millisecond)
		}
		wg.Wait()

		c.net.setFaults(0, 0)
		c.net.heal()
		c.waitApplied(c.commitAny("end"))

		final := cmds(c.checkSame())
		seen := map[string]bool{}
		for _, cmd := range final {
			if seen[cmd] {
				t.Fatalf("%q applied twice", cmd)
			}
			seen[cmd] = true
		}
		acked.Range(func(k, _ any) bool {
			if !seen[k.(string)] {
				t.Errorf("acknowledged %q never applied", k)
			}
			return true
		})
	})
}

func TestCatchUpBatchesStayUnderByteLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		lagging := c.ids[0]
		if lagging == leader {
			lagging = c.ids[1]
		}
		c.stop(lagging)
		third := strings.Repeat("x", maxBatchBytes/3+1)
		for i := range 6 {
			c.mustPropose(leader, fmt.Sprint(i, third))
		}
		oversized := strings.Repeat("y", maxBatchBytes+1)
		c.mustPropose(leader, oversized)

		c.start(lagging)
		c.waitApplied(oversized, lagging)
		c.net.mu.Lock()
		defer c.net.mu.Unlock()
		if c.net.oversized != "" {
			t.Fatal(c.net.oversized)
		}
	})
}

func TestProposeRejectsCommandTooLargeForTheLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 1)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := c.node(leader).Propose(ctx, make([]byte, wal.MaxRecordSize)); !errors.Is(err, ErrCommandTooLarge) {
			t.Fatalf("propose %d bytes: got %v, want ErrCommandTooLarge", wal.MaxRecordSize, err)
		}
		c.mustPropose(leader, "still serving")
	})
}

func TestBatchCarriesALoneOversizedEntry(t *testing.T) {
	n := &Node{log: []Entry{{}, {Term: 1, Cmd: make([]byte, maxBatchBytes+1)}, {Term: 1, Cmd: []byte("a")}}}
	if got := n.batch(1); len(got) != 1 {
		t.Fatalf("batch from an oversized entry has %d entries, want 1", len(got))
	}
}

func TestLeaderChangeHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		old, term := c.waitLeader()
		time.Sleep(10 * testElection)
		first := []leaderChange{{term, old}}
		for _, id := range c.ids {
			if got := c.leaderChanges(id); !slices.Equal(got, first) {
				t.Fatalf("%s saw leader changes %v after many heartbeats, want %v", id, got, first)
			}
		}

		c.stop(old)
		leader, newTerm := c.waitLeader()
		time.Sleep(10 * testElection)
		want := append(first, leaderChange{newTerm, leader})
		for _, id := range c.live() {
			if got := c.leaderChanges(id); !slices.Equal(got, want) {
				t.Fatalf("%s saw leader changes %v after failover, want %v", id, got, want)
			}
		}
	})
}

func TestProposeReturnsItsOwnResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		leader, _ := c.waitLeader()
		n := c.node(leader)
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Go(func() {
				cmd := fmt.Sprint("p", i)
				res, err := n.Propose(context.Background(), []byte(cmd))
				if err != nil {
					t.Errorf("propose %s: %v", cmd, err)
				} else if string(res) != "applied "+cmd {
					t.Errorf("propose %s returned %q, want the result of applying it", cmd, res)
				}
			})
		}
		wg.Wait()
	})
}

func TestOverwrittenProposalIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := heldVotes{release: make(chan struct{})}
		close(tr.release)
		n, err := New(Config{
			ID:                "a",
			Members:           ids("a", "b", "c"),
			Dir:               t.TempDir(),
			Transport:         tr,
			Apply:             func(uint64, []byte) []byte { return nil },
			ElectionTimeout:   time.Hour,
			HeartbeatInterval: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer mustStop(t, n)
		n.mu.Lock()
		n.startElection()
		n.mu.Unlock()
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		proposed := make(chan error, 1)
		go func() { _, err := n.Propose(ctx, []byte("x")); proposed <- err }()
		synctest.Wait()
		appendEntries(t, n, AppendEntriesArgs{Term: 2, Leader: "b", PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, "y")})
		if err := <-proposed; !errors.Is(err, ErrProposalDropped) {
			t.Fatalf("proposal overwritten by a newer leader: got %v, want ErrProposalDropped", err)
		}
	})
}

func TestIsolatedLeaderStepsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		defer close(c.gate)

		leader, _ := c.waitLeader()
		n := c.node(leader)
		c.mustPropose(leader, "a")
		commit := n.Status().CommitIndex

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		writes := make(chan error, 2)
		reads := make(chan error, 1)
		go func() { _, err := n.Propose(ctx, []byte("slow")); writes <- err }()
		waitFor(t, 10*time.Second, "slow to commit", func() bool { return n.Status().CommitIndex > commit })
		go func() { reads <- n.ReadIndex(ctx) }()
		synctest.Wait()

		start := time.Now()
		c.net.partition([]string{leader}, slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == leader }))
		go func() { _, err := n.Propose(ctx, []byte("p")); writes <- err }()
		for range 2 {
			if err := <-writes; !errors.Is(err, ErrLeadershipLost) {
				t.Fatalf("write waiting on an isolated leader: got %v, want ErrLeadershipLost", err)
			}
		}
		var nle *NotLeaderError
		if err := <-reads; !errors.As(err, &nle) || nle.Leader != "" {
			t.Fatalf("read waiting on an isolated leader: got %v, want NotLeaderError with no leader", err)
		}
		if d := time.Since(start); d > 2*testElection {
			t.Fatalf("calls waiting on an isolated leader failed after %v, want within %v", d, 2*testElection)
		}
		if _, err := n.Propose(ctx, []byte("q")); !errors.As(err, &nle) || nle.Leader != "" {
			t.Fatalf("write on a stepped-down leader: got %v, want NotLeaderError with no leader", err)
		}
		if s := n.Status(); s.Role == Leader || s.Leader != "" {
			t.Fatalf("isolated leader is %v with leader %q, want no leader", s.Role, s.Leader)
		}
	})
}

func TestLeaderWithOneOfTwoFollowersKeepsLeading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, term := c.waitLeader()
		others := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == leader })
		c.net.partition([]string{leader, others[0]}, others[1:])
		for range 20 {
			time.Sleep(testElection)
			if s := c.node(leader).Status(); s.Role != Leader || s.Term != term {
				t.Fatalf("leader of term %d with one follower became %v in term %d", term, s.Role, s.Term)
			}
		}
		c.mustPropose(leader, "a")
	})
}

func TestIdleClusterNeverStepsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		c.net.setFaults(0, 15*time.Millisecond)

		leader, term := c.waitLeader()
		for range 30 {
			time.Sleep(testElection)
			if s := c.node(leader).Status(); s.Role != Leader || s.Term != term {
				t.Fatalf("idle leader of term %d became %v in term %d", term, s.Role, s.Term)
			}
		}
		if n := c.net.leaderTerms(); n != 1 {
			t.Fatalf("saw leaders in %d terms in an idle cluster, want 1", n)
		}
	})
}

func TestSlowAppendDoesNotCauseElection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, term := c.waitLeader()
		c.mustPropose(leader, "a")
		c.waitApplied("a")
		c.net.setSlowAppend(3 * testElection)

		sameTerm := func() {
			t.Helper()
			for _, id := range c.ids {
				if s := c.node(id).Status(); s.Term != term {
					t.Fatalf("%s moved from term %d to %d while an append was slow", id, term, s.Term)
				}
			}
		}
		errs := make(chan error, 1)
		go func() { errs <- c.propose(leader, "b", 10*time.Second) }()
		for len(errs) == 0 {
			sameTerm()
			time.Sleep(testElection / 10)
		}
		if err := <-errs; err != nil {
			t.Fatalf("propose with slow appends: %v", err)
		}
		c.waitApplied("b")
		sameTerm()
		if n := c.net.leaderTerms(); n != 1 {
			t.Fatalf("saw leaders in %d terms, want 1", n)
		}
	})
}

func TestHeartbeatDoesNotCommitStaleEntries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5)
		defer c.stopAll()

		old, _ := c.waitLeader()
		c.mustPropose(old, "a")
		c.waitApplied("a")
		shared := c.node(old).Status().CommitIndex
		others := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == old })
		c.net.partition([]string{old}, others)
		proposeIsolated(t, c, old, "x1", "x2", "x3")
		// stopped before it campaigns, so it rejoins in its old term without disrupting the new leader
		c.stop(old)

		leader, term := c.waitLeader(others...)
		for _, cmd := range []string{"b", "c", "d"} {
			c.mustPropose(leader, cmd)
		}
		c.net.heal()
		c.net.setSlowAppend(time.Hour)
		c.start(old)

		waitFor(t, 10*time.Second, old+" to follow the new leader", func() bool {
			s := c.node(old).Status()
			return s.Term == term && s.Leader == leader
		})
		for range 50 {
			time.Sleep(testElection / 10)
			s := c.node(old).Status()
			if s.Term != term || s.Leader != leader {
				t.Fatalf("%s left leader %s of term %d for %q in term %d while only heartbeats reached it", old, leader, term, s.Leader, s.Term)
			}
			if s.CommitIndex > shared {
				t.Fatalf("%s committed up to %d through heartbeats, past its last entry %d shared with the leader", old, s.CommitIndex, shared)
			}
		}

		c.net.setSlowAppend(0)
		c.waitApplied("d", old)
		if got, want := cmds(c.checkSame()), []string{"a", "b", "c", "d"}; !slices.Equal(got, want) {
			t.Fatalf("applied %v, want %v", got, want)
		}
	})
}

func TestWipedFollowerCatchesUpInAnIdleCluster(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		c.mustPropose(leader, "a")
		c.waitApplied("a")
		follower := c.ids[0]
		if follower == leader {
			follower = c.ids[1]
		}
		c.stop(follower)
		if err := os.RemoveAll(filepath.Join(c.root, follower)); err != nil {
			t.Fatal(err)
		}
		c.start(follower)
		c.waitApplied("a", follower)
	})
}

func TestStaleTermAppendReplyIsIgnored(t *testing.T) {
	n := &Node{
		id:      "a",
		members: ids("a", "b", "c"),
		peers:   map[string]*peer{"b": {next: 4}, "c": {next: 4}},
		ctx:     context.Background(),
		role:    Leader,
		term:    4,
		log:     []Entry{{}, {Term: 1}, {Term: 2, Cmd: []byte("x")}, {Term: 4}},
	}
	n.applyCond = sync.NewCond(&n.mu)

	args := AppendEntriesArgs{Term: 2, Leader: "a", Entries: []Entry{{Term: 1}, {Term: 2, Cmd: []byte("x")}, {Term: 2}}}
	n.handleAppendReply(n.peers["b"], args, AppendEntriesReply{Term: 2, Success: true})
	if n.peers["b"].match != 0 || n.commitIndex != 0 {
		t.Fatalf("a term 2 reply moved the term 4 leader to matchIndex %d, commitIndex %d; want 0, 0", n.peers["b"].match, n.commitIndex)
	}
}

func TestFailedAppendPersistIsNotAcked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		n.mu.Lock()
		if err := n.wal.Close(); err != nil {
			t.Fatal(err)
		}
		n.mu.Unlock()

		r, err := n.HandleAppendEntries(AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "x")})
		if err == nil || r.Success {
			t.Fatalf("append entries after the log failed: got %+v, %v; want an error and no reply", r, err)
		}
		if n.Err() == nil {
			t.Fatal("node still running after failing to persist entries")
		}
		if err := n.Stop(); err == nil {
			t.Fatal("stop after a persistence failure: want the failure")
		}
	})
}

func TestFailedVotePersistIsNotGranted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		n := singleNode(t, dir)
		appendEntries(t, n, AppendEntriesArgs{Term: 5, Leader: "c"})
		n.mu.Lock()
		n.dir = filepath.Join(dir, "missing")
		n.heard = time.Time{}
		n.mu.Unlock()

		r, err := n.HandleRequestVote(RequestVoteArgs{Term: 5, Candidate: "b"})
		if err == nil || r.Granted {
			t.Fatalf("vote that could not be persisted: got %+v, %v; want an error and no reply", r, err)
		}
		if n.Err() == nil {
			t.Fatal("node still running after failing to persist its vote")
		}
		if err := n.Stop(); err == nil {
			t.Fatal("stop after a persistence failure: want the failure")
		}
	})
}

func TestLogWithGapFailsNew(t *testing.T) {
	dir := t.TempDir()
	l, err := wal.Open(vfs.OS, filepath.Join(dir, "log"), func([]byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range [][]byte{encodeBatch(1, entries(1, "a")), encodeBatch(3, entries(1, "c"))} {
		if err := l.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := New(Config{ID: "a", Members: ids("a"), Dir: dir, Transport: memTransport{net: newMemNet(1), from: "a"}, Apply: func(uint64, []byte) []byte { return nil }})
	if err == nil {
		mustStop(t, n)
		t.Fatal("new on a log missing entry 2: want error")
	}
}

// higherTerm grants every vote and answers AppendEntries with or without entries, as chosen, with term 1000; the others hang
type higherTerm struct{ withEntries bool }

func (higherTerm) RequestVote(_ context.Context, _ string, args RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{Term: args.Term, Granted: true}, nil
}

func (h higherTerm) AppendEntries(ctx context.Context, _ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	if (len(args.Entries) > 0) == h.withEntries {
		return AppendEntriesReply{Term: 1000}, nil
	}
	<-ctx.Done()
	return AppendEntriesReply{}, ctx.Err()
}

func (higherTerm) InstallSnapshot(ctx context.Context, _ string, _ InstallSnapshotArgs) (InstallSnapshotReply, error) {
	<-ctx.Done()
	return InstallSnapshotReply{}, ctx.Err()
}

func TestHigherTermAppendReplyStepsLeaderDown(t *testing.T) {
	for name, withEntries := range map[string]bool{"replicate": true, "heartbeats": false} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				n, err := New(Config{
					ID:                "a",
					Members:           ids("a", "b", "c"),
					Dir:               t.TempDir(),
					Transport:         higherTerm{withEntries: withEntries},
					Apply:             func(uint64, []byte) []byte { return nil },
					ElectionTimeout:   testElection,
					HeartbeatInterval: testHeartbeat,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer mustStop(t, n)
				waitFor(t, time.Second, "a to adopt term 1000 from an append reply", func() bool { return n.Status().Term >= 1000 })
			})
		})
	}
}
