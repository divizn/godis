package consensus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func memberIDs(members []Member) []string {
	var out []string
	for _, m := range members {
		out = append(out, m.ID)
	}
	slices.Sort(out)
	return out
}

func (c *cluster) startBootstrap() {
	for _, id := range c.bootstrap {
		c.start(id)
	}
}

// change runs f on the leader once its first entry has committed, as the node layer does
func (c *cluster) change(f func(ctx context.Context, n *Node) error) error {
	leader, _ := c.waitLeader()
	c.mustPropose(leader, fmt.Sprint("sync", c.attempt))
	c.attempt++
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return f(ctx, c.node(leader))
}

func (c *cluster) addMember(id string) error {
	return c.change(func(ctx context.Context, n *Node) error { return n.AddMember(ctx, Member{ID: id, Addr: id}) })
}

func (c *cluster) removeMember(id string) error {
	return c.change(func(ctx context.Context, n *Node) error { return n.RemoveMember(ctx, id) })
}

func (c *cluster) waitMembers(id string, want ...string) {
	c.t.Helper()
	waitFor(c.t, 10*time.Second, fmt.Sprintf("%s to use members %v", id, want), func() bool {
		return slices.Equal(memberIDs(c.node(id).Members()), want)
	})
}

func followers(c *cluster, leader string) []string {
	return slices.DeleteFunc(slices.Clone(c.live()), func(id string) bool { return id == leader })
}

func TestAddMembersCatchUpThenCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newMemberCluster(t, 3, 2)
		c.startBootstrap()
		defer c.stopAll()
		leader, _ := c.waitLeader()
		want := []string{"a", "b", "c"}
		for _, cmd := range want {
			c.mustPropose(leader, cmd)
		}

		for _, id := range []string{"n4", "n5"} {
			c.start(id)
			if err := c.addMember(id); err != nil {
				t.Fatalf("add %s: %v", id, err)
			}
			want = append(want, fmt.Sprint("sync", c.attempt-1))
		}
		leader, _ = c.waitLeader()
		for _, id := range []string{"n4", "n5"} {
			c.waitApplied("c", id)
		}
		for _, id := range c.live() {
			c.waitMembers(id, "n1", "n2", "n3", "n4", "n5")
		}

		// three of five need the new nodes
		originals := slices.DeleteFunc([]string{"n1", "n2", "n3"}, func(id string) bool { return id == leader })
		c.stop(originals[0])
		c.stop(originals[1])
		if len(c.live()) != 3 {
			t.Fatalf("live nodes %v, want 3", c.live())
		}
		c.mustPropose(leader, "after")
		want = append(want, "after")
		c.waitApplied("after")

		for _, e := range c.checkSame() {
			if !slices.Contains(want, e.cmd) {
				t.Fatalf("state machine applied %q, which no client proposed", e.cmd)
			}
		}
	})
}

func TestRemoveFollowerShrinksTheQuorum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 4)
		defer c.stopAll()
		leader, _ := c.waitLeader()
		c.mustPropose(leader, "before")
		rest := followers(c, leader)

		if err := c.removeMember(rest[0]); err != nil {
			t.Fatalf("remove %s: %v", rest[0], err)
		}
		c.stop(rest[0])
		c.stop(rest[1])
		// leader and one follower are two of the three that remain, but only two of four
		c.mustPropose(leader, "after")
		c.waitApplied("after")
		if got := memberIDs(c.node(leader).Members()); len(got) != 3 || slices.Contains(got, rest[0]) {
			t.Fatalf("members %v after removing %s", got, rest[0])
		}
	})
}

func TestRemoveLeaderKeepsLeadingUntilCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		old, _ := c.waitLeader()
		c.mustPropose(old, "before")
		rest := followers(c, old)
		c.net.setSlowAppend(300 * time.Millisecond)

		errc := make(chan error, 1)
		go func() { errc <- c.removeMember(old) }()
		waitFor(t, 10*time.Second, "the leader to use the configuration without itself", func() bool {
			return !slices.Contains(memberIDs(c.node(old).Members()), old)
		})
		if role := c.node(old).Status().Role; role != Leader {
			t.Fatalf("leader removing itself is a %v before the change commits, want leader", role)
		}
		if err := <-errc; err != nil {
			t.Fatalf("remove leader: %v", err)
		}
		c.net.setSlowAppend(0)
		if role := c.node(old).Status().Role; role == Leader {
			t.Fatal("removed leader still leads after the change committed")
		}

		next, term := c.waitLeader(rest...)
		c.mustPropose(next, "after")
		c.waitApplied("after", rest...)
		time.Sleep(5 * time.Second)
		if st := c.node(old).Status(); st.Role == Leader || st.Term > term {
			t.Fatalf("removed node is %v in term %d, the cluster is in term %d", st.Role, st.Term, term)
		}
		if st := c.node(next).Status(); st.Role != Leader || st.Term != term {
			t.Fatalf("leader %s is %v in term %d, want leader in term %d", next, st.Role, st.Term, term)
		}
	})
}

func TestSecondChangeWhileOneIsUncommittedIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newMemberCluster(t, 3, 1)
		c.startBootstrap()
		c.start("n4")
		defer c.stopAll()
		leader, _ := c.waitLeader()
		c.mustPropose(leader, "before")
		rest := followers(c, leader)
		rest = slices.DeleteFunc(rest, func(id string) bool { return id == "n4" })
		c.net.setSlowAppend(500 * time.Millisecond)

		errc := make(chan error, 1)
		go func() { errc <- c.node(leader).RemoveMember(context.Background(), rest[0]) }()
		waitFor(t, 10*time.Second, "the first change to be appended", func() bool {
			return len(c.node(leader).Members()) == 2
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.node(leader).RemoveMember(ctx, rest[1]); !errors.Is(err, ErrChangeInProgress) {
			t.Fatalf("remove during an uncommitted change: got %v, want ErrChangeInProgress", err)
		}
		if err := c.node(leader).AddMember(ctx, Member{ID: "n4", Addr: "n4"}); !errors.Is(err, ErrChangeInProgress) {
			t.Fatalf("add during an uncommitted change: got %v, want ErrChangeInProgress", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("first change: %v", err)
		}
		c.net.setSlowAppend(0)
		if got := memberIDs(c.node(leader).Members()); len(got) != 2 {
			t.Fatalf("members %v, want the leader and one follower", got)
		}
	})
}

func TestChangeBeforeTheLeaderCommitsInItsTermIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newStoppedCluster(t, 3, 1)
		c.net.setSlowAppend(500 * time.Millisecond)
		for _, id := range c.ids {
			c.start(id)
		}
		defer c.stopAll()
		leader, _ := c.waitLeader()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := c.node(leader).RemoveMember(ctx, followers(c, leader)[0])
		if !errors.Is(err, ErrLeaderNotReady) {
			t.Fatalf("change before the no-op committed: got %v, want ErrLeaderNotReady", err)
		}
	})
}

func TestChangeValidation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		leader, _ := c.waitLeader()
		c.mustPropose(leader, "x")
		ctx := context.Background()
		n := c.node(leader)

		if err := n.AddMember(ctx, Member{ID: followers(c, leader)[0], Addr: "elsewhere"}); !errors.Is(err, ErrMemberExists) {
			t.Fatalf("add an existing member: got %v, want ErrMemberExists", err)
		}
		if err := n.RemoveMember(ctx, "nobody"); !errors.Is(err, ErrNotMember) {
			t.Fatalf("remove a stranger: got %v, want ErrNotMember", err)
		}
		var nle *NotLeaderError
		if err := c.node(followers(c, leader)[0]).RemoveMember(ctx, leader); !errors.As(err, &nle) || nle.Leader != leader {
			t.Fatalf("change on a follower: got %v, want a NotLeaderError naming %s", err, leader)
		}
		if got := memberIDs(n.Members()); len(got) != 3 {
			t.Fatalf("refused changes left members %v", got)
		}
	})
}

func TestLastMemberCannotBeRemoved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 1)
		defer c.stopAll()
		c.waitLeader()
		c.mustPropose("n1", "x")
		if err := c.node("n1").RemoveMember(context.Background(), "n1"); !errors.Is(err, ErrLastMember) {
			t.Fatalf("remove the only member: got %v, want ErrLastMember", err)
		}
	})
}

func TestConfigTakesEffectOnAppendAndRevertsOnTruncation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		defer mustStop(t, n)
		config := func(list ...string) Entry { return Entry{Term: 1, Kind: EntryConfig, Cmd: encodeMembers(ids(list...))} }
		want := func(what string, list ...string) {
			t.Helper()
			if got := memberIDs(n.Members()); !slices.Equal(got, list) {
				t.Fatalf("%s: members %v, want %v", what, got, list)
			}
		}
		want("at start", "a", "b", "c")

		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: []Entry{{Term: 1}, config("a", "b", "c", "d")}})
		if n.Status().CommitIndex != 0 {
			t.Fatal("entries committed with LeaderCommit 0")
		}
		want("after appending an uncommitted config", "a", "b", "c", "d")

		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{config("a", "b", "c", "d", "e")}})
		want("after a second config", "a", "b", "c", "d", "e")

		appendEntries(t, n, AppendEntriesArgs{Term: 2, Leader: "c", PrevLogIndex: 2, PrevLogTerm: 1, Entries: entries(2, "x")})
		want("after the second config was truncated", "a", "b", "c", "d")

		appendEntries(t, n, AppendEntriesArgs{Term: 3, Leader: "c", PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(3, "y")})
		want("after every config was truncated", "a", "b", "c")
	})
}

func TestMalformedConfigEntryIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := singleNode(t, t.TempDir())
		defer mustStop(t, n)
		for name, e := range map[string]Entry{
			"garbage members": {Term: 1, Kind: EntryConfig, Cmd: []byte{0xff, 0xff}},
			"empty members":   {Term: 1, Kind: EntryConfig},
			"unknown kind":    {Term: 1, Kind: 7, Cmd: []byte("x")},
		} {
			if _, err := n.HandleAppendEntries(AppendEntriesArgs{Term: 1, Leader: "b", Entries: []Entry{e}}); err == nil {
				t.Fatalf("%s: entry accepted", name)
			}
			if n.Err() != nil {
				t.Fatalf("%s: node halted: %v", name, n.Err())
			}
		}
		if got := memberIDs(n.Members()); len(got) != 3 {
			t.Fatalf("members %v after rejected entries", got)
		}
	})
}

func TestConfigSurvivesRestartFromTheLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newMemberCluster(t, 3, 1)
		c.startBootstrap()
		defer c.stopAll()
		c.start("n4")
		if err := c.addMember("n4"); err != nil {
			t.Fatal(err)
		}
		c.stopAll()
		for _, id := range []string{"n1", "n2", "n3", "n4"} {
			c.start(id)
			if got := memberIDs(c.node(id).Members()); len(got) != 4 {
				t.Fatalf("%s restarted with members %v, want all four", id, got)
			}
		}
		leader, _ := c.waitLeader()
		c.mustPropose(leader, "after")
	})
}

func TestConfigSurvivesRestartFromASnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newMemberCluster(t, 3, 1)
		c.snapEntries = 3
		c.startBootstrap()
		defer c.stopAll()
		leader, _ := c.waitLeader()
		for i := range 8 {
			c.mustPropose(leader, fmt.Sprint("before", i))
		}
		c.start("n4")
		if err := c.addMember("n4"); err != nil {
			t.Fatal(err)
		}
		if c.net.installCount() == 0 {
			t.Fatal("the new member caught up without a snapshot, which the leader's log no longer allows")
		}
		leader, _ = c.waitLeader()
		for i := range 8 {
			c.mustPropose(leader, fmt.Sprint("after", i))
		}
		c.waitApplied("after7")
		for _, id := range c.ids {
			n := c.node(id)
			waitFor(t, 10*time.Second, id+" to compact its config entry", func() bool {
				n.mu.Lock()
				defer n.mu.Unlock()
				return n.cfgIndex == 0 && n.snapIndex > 0
			})
		}
		c.stopAll()
		for _, id := range c.ids {
			c.start(id)
			if got := memberIDs(c.node(id).Members()); len(got) != 4 {
				t.Fatalf("%s restarted with members %v, want all four", id, got)
			}
		}
	})
}

func TestInstallSnapshotCarriesTheConfig(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		n := snapNode(t, dir, &applyLog{})
		install(t, n, InstallSnapshotArgs{Term: 1, Leader: "b", LastIndex: 2, LastTerm: 1, Members: ids("a", "b", "c", "d"), Data: snapshotOf(appliedN(2)...)})
		if got := memberIDs(n.Members()); len(got) != 4 {
			t.Fatalf("members %v after a snapshot of a four node cluster", got)
		}
		if err := n.Stop(); err != nil {
			t.Fatal(err)
		}
		n = snapNode(t, dir, &applyLog{})
		defer mustStop(t, n)
		if got := memberIDs(n.Members()); len(got) != 4 {
			t.Fatalf("members %v after restarting from the installed snapshot", got)
		}
	})
}

func TestJoiningNodeNeverCampaigns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newMemberCluster(t, 3, 1)
		c.startBootstrap()
		defer c.stopAll()
		c.start("n4")
		time.Sleep(20 * testElection)
		if st := c.node("n4").Status(); st.Term != 0 || st.Role != Follower {
			t.Fatalf("node without a configuration is a %v in term %d, want a follower in term 0", st.Role, st.Term)
		}
	})
}

func TestJoinerHoldsOffSnapshotsUntilItKnowsTheConfig(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		al := &applyLog{}
		cfg := snapConfig(t.TempDir(), al)
		cfg.Members, cfg.SnapshotEntries = nil, 2
		n, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer mustStop(t, n)
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", LeaderCommit: 3, Entries: entries(1, "x", "y", "z")})
		waitFor(t, 10*time.Second, "the entries to apply", func() bool { return al.has("z") })
		time.Sleep(time.Second)
		if idx := n.Status().SnapshotIndex; idx != 0 {
			t.Fatalf("snapshot at %d taken before the node knew any configuration", idx)
		}

		config := Entry{Term: 1, Kind: EntryConfig, Cmd: encodeMembers(ids("a", "b"))}
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 5, Entries: append([]Entry{config}, entries(1, "w")...)})
		waitFor(t, 10*time.Second, "a snapshot once the config is applied", func() bool { return n.Status().SnapshotIndex >= 4 })
	})
}

func TestRemovedNodeDoesNotUnseatTheLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		leader, term := c.waitLeader()
		c.mustPropose(leader, "x")
		rest := followers(c, leader)
		victim, kept := rest[0], rest[1]

		c.net.partition([]string{victim}, []string{leader, kept})
		time.Sleep(3 * time.Second)
		if err := c.removeMember(victim); err != nil {
			t.Fatalf("remove %s: %v", victim, err)
		}
		if st := c.node(victim).Status(); st.Term <= term {
			t.Fatalf("partitioned node is in term %d, want it campaigning past term %d", st.Term, term)
		}
		c.net.heal()

		for range 100 {
			time.Sleep(50 * time.Millisecond)
			if st := c.node(leader).Status(); st.Role != Leader || st.Term != term {
				t.Fatalf("leader is a %v in term %d after a removed node campaigned, want leader in term %d", st.Role, st.Term, term)
			}
		}
		c.mustPropose(leader, "after")
		c.waitApplied("after", leader, kept)
	})
}

func TestAddingAnUnreachableNodeFailsAndChangesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		for range 2 {
			err := c.addMember("ghost")
			if !errors.Is(err, ErrCatchUpTimeout) {
				t.Fatalf("add an unreachable node: got %v, want ErrCatchUpTimeout", err)
			}
		}
		leader, _ := c.waitLeader()
		if got := memberIDs(c.node(leader).Members()); len(got) != 3 {
			t.Fatalf("members %v after a failed add", got)
		}
		c.mustPropose(leader, "after")
		for _, id := range c.live() {
			if ghost := c.node(id).peers["ghost"]; ghost != nil {
				t.Fatalf("%s still replicates to the node that failed to join", id)
			}
		}
	})
}

func TestLeaderLosingLeadershipDuringCatchUpAbortsTheAdd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()
		leader, _ := c.waitLeader()
		c.mustPropose(leader, "x")
		errc := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			errc <- c.node(leader).AddMember(ctx, Member{ID: "ghost", Addr: "ghost"})
		}()
		waitFor(t, 10*time.Second, "the learner to be tracked", func() bool {
			n := c.node(leader)
			n.mu.Lock()
			defer n.mu.Unlock()
			return n.learner == "ghost"
		})
		c.net.partition([]string{leader}, followers(c, leader))
		var nle *NotLeaderError
		if err := <-errc; !errors.As(err, &nle) {
			t.Fatalf("add on a leader that lost its leadership: got %v, want a NotLeaderError", err)
		}
		n := c.node(leader)
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.learner != "" || len(n.members) != 3 || n.cfgIndex != 0 {
			t.Fatalf("aborted add left learner %q, members %v, config entry %d", n.learner, n.members, n.cfgIndex)
		}
	})
}
