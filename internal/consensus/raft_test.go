package consensus

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
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
		Peers:             []string{"a", "b", "c"},
		Dir:               dir,
		Transport:         memTransport{net: newMemNet(), from: "a"},
		Apply:             func(uint64, []byte) {},
		ElectionTimeout:   time.Hour,
		HeartbeatInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
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
		defer n.Stop()
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
		defer n.Stop()
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
		defer n.Stop()
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
		defer n.Stop()
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

func TestVotesFromAnEarlierElectionAreNotCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := heldVotes{release: make(chan struct{})}
		n, err := New(Config{
			ID:                "a",
			Peers:             []string{"a", "b", "c"},
			Dir:               t.TempDir(),
			Transport:         tr,
			Apply:             func(uint64, []byte) {},
			ElectionTimeout:   time.Hour,
			HeartbeatInterval: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Stop()
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
		time.Sleep(time.Second)
		select {
		case err := <-minority:
			t.Fatalf("propose on leader without quorum returned %v while partitioned", err)
		default:
		}

		c.net.heal()
		c.waitApplied("majority")
		if err := <-minority; !errors.Is(err, ErrProposalDropped) {
			t.Fatalf("overwritten proposal: got %v, want ErrProposalDropped", err)
		}
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

// diverge leaves old leaders holding uncommitted entries from two terms, then heals
func diverge(t *testing.T, c *cluster) {
	t.Helper()
	l1, _ := c.waitLeader()
	c.mustPropose(l1, "a")
	others := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == l1 })
	c.net.partition([]string{l1}, others)
	for _, cmd := range []string{"x1", "x2", "x3"} {
		if err := c.propose(l1, cmd, 50*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("propose %q on isolated leader: got %v, want deadline exceeded", cmd, err)
		}
	}

	l2, _ := c.waitLeader(others...)
	c.mustPropose(l2, "b")
	rest := slices.DeleteFunc(slices.Clone(others), func(id string) bool { return id == l2 })
	c.net.partition([]string{l1}, []string{l2}, rest)
	for _, cmd := range []string{"y1", "y2"} {
		if err := c.propose(l2, cmd, 50*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("propose %q on isolated leader: got %v, want deadline exceeded", cmd, err)
		}
	}

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
		peers:       []string{"b", "c", "d", "e"},
		role:        Leader,
		term:        4,
		log:         []Entry{{}, {Term: 1, Cmd: []byte("x")}, {Term: 2, Cmd: []byte("y")}, {Term: 4}},
		matchIndex:  map[string]uint64{"b": 2, "c": 2, "d": 1, "e": 1},
		commitIndex: 1,
	}
	n.applyCond = sync.NewCond(&n.mu)

	n.advanceCommit()
	if n.commitIndex != 1 {
		t.Fatalf("committed index %d from term 2 by counting replicas in term 4", n.commitIndex)
	}
	n.matchIndex["b"], n.matchIndex["c"] = 3, 3
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
