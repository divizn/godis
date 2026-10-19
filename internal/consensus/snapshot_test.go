package consensus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/divizn/godis/internal/storage"
	"github.com/divizn/godis/internal/vfs"
	"github.com/divizn/godis/internal/wal"
)

func snapConfig(dir string, al *applyLog) Config {
	return Config{
		ID:                "a",
		Members:           ids("a", "b", "c"),
		Dir:               dir,
		Transport:         memTransport{net: newMemNet(1), from: "a"},
		Apply:             al.apply,
		Snapshot:          al.save,
		Restore:           al.restore,
		ElectionTimeout:   time.Hour,
		HeartbeatInterval: time.Minute,
	}
}

func snapNode(t *testing.T, dir string, al *applyLog) *Node {
	t.Helper()
	n, err := New(snapConfig(dir, al))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func snapshotOf(entries ...applied) []byte {
	return (&applyLog{entries: entries}).save()
}

func appliedN(n int) []applied {
	var out []applied
	for i := 1; i <= n; i++ {
		out = append(out, applied{uint64(i), fmt.Sprint("e", i)})
	}
	return out
}

func install(t *testing.T, n *Node, args InstallSnapshotArgs) InstallSnapshotReply {
	t.Helper()
	r, err := n.HandleInstallSnapshot(args)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}
	return r
}

// walIndexes lists the entry indexes the log file at path holds, in record order
func walIndexes(t *testing.T, path string) []uint64 {
	t.Helper()
	var idx []uint64
	l, err := wal.Open(vfs.OS, path, func(rec []byte) error {
		start, es, err := decodeBatch(rec)
		for i := range es {
			idx = append(idx, start+uint64(i))
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestSnapshotCompactsLogAndRestartSkipsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newStoppedCluster(t, 1, 1)
		c.snapEntries = 2
		c.start("n1")
		defer c.stopAll()
		leader, _ := c.waitLeader()
		n := c.node(leader)

		// slow holds the applier so a to d commit ahead of it while the first snapshot is taken
		errs := make(chan error, 5)
		go func() { errs <- c.propose(leader, "slow", 10*time.Second) }()
		waitFor(t, 10*time.Second, "slow to commit", func() bool { return n.Status().CommitIndex >= 2 })
		for _, cmd := range []string{"a", "b", "c", "d"} {
			go func() { errs <- c.propose(leader, cmd, 10*time.Second) }()
		}
		waitFor(t, 10*time.Second, "a to d to commit", func() bool { return n.Status().CommitIndex >= 6 })
		close(c.gate)
		for range 5 {
			if err := <-errs; err != nil {
				t.Fatalf("propose: %v", err)
			}
		}
		waitFor(t, 10*time.Second, "a snapshot at 6", func() bool { return n.Status().SnapshotIndex == 6 })
		c.mustPropose(leader, "e")
		if got := n.Status().SnapshotIndex; got != 6 {
			t.Fatalf("snapshot index %d after one more entry, want 6", got)
		}
		before := c.checkSame()
		c.stop(leader)

		dir := filepath.Join(c.root, leader)
		if idx, _, _, _, err := readSnapshot(vfs.OS, dir); err != nil || idx != 6 {
			t.Fatalf("snapshot file: index %d, %v; want 6", idx, err)
		}
		if got := walIndexes(t, filepath.Join(dir, "log")); !slices.Equal(got, []uint64{7}) {
			t.Fatalf("log file holds indexes %v after compaction, want [7]", got)
		}

		c.start(leader)
		c.waitLeader()
		c.mustPropose(leader, "f")
		c.waitApplied("f")
		for _, i := range c.log(leader).applyCalls() {
			if i <= 6 {
				t.Fatalf("restart applied index %d again, at or below the snapshot at 6", i)
			}
		}
		after := c.checkSame()
		if want := append(before, applied{9, "f"}); !slices.Equal(after, want) {
			t.Fatalf("after restart applied %v, want %v", after, want)
		}
	})
}

func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newStoppedCluster(t, 1, 1)
		c.snapEntries = 100
		dir := filepath.Join(c.root, "n1")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeState(vfs.OS, dir, 1, "n1"); err != nil {
			t.Fatal(err)
		}
		if err := writeSnapshot(vfs.OS, filepath.Join(dir, "snapshot"), 3, 1, ids("n1"), snapshotOf(applied{2, "a"}, applied{3, "b"})); err != nil {
			t.Fatal(err)
		}
		l, err := wal.Open(vfs.OS, filepath.Join(dir, "log"), func([]byte) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		// the full log the snapshot was cut from: one batch wholly below it, one straddling it
		for _, rec := range [][]byte{encodeBatch(1, []Entry{{Term: 1}, {Term: 1, Cmd: []byte("a")}}), encodeBatch(3, entries(1, "b", "c", "d"))} {
			if err := l.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}

		c.start("n1")
		defer c.stopAll()
		c.waitApplied("d")
		if got := c.log("n1").applyCalls(); !slices.Equal(got, []uint64{4, 5}) {
			t.Fatalf("applied indexes %v after restart, want [4 5]", got)
		}
		if got, want := c.log("n1").snapshot(), []applied{{2, "a"}, {3, "b"}, {4, "c"}, {5, "d"}}; !slices.Equal(got, want) {
			t.Fatalf("state %v, want %v", got, want)
		}
	})
}

func TestCorruptSnapshotFailsNew(t *testing.T) {
	for name, corrupt := range map[string]func([]byte) []byte{
		"index changed": func(b []byte) []byte { b[0] ^= 1; return b },
		"truncated":     func(b []byte) []byte { return b[:len(b)-1] },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "snapshot")
			if err := writeSnapshot(vfs.OS, path, 3, 1, nil, snapshotOf(applied{2, "a"})); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, corrupt(b), 0o644); err != nil {
				t.Fatal(err)
			}
			n, err := New(snapConfig(dir, &applyLog{}))
			if err == nil {
				mustStop(t, n)
				t.Fatal("new on a corrupt snapshot: want error")
			}
		})
	}
}

// laggingCluster has a stopped follower whose whole log the leader has compacted away
func laggingCluster(t *testing.T) (c *cluster, leader, follower string, set func(int) string) {
	t.Helper()
	c = newStoppedCluster(t, 3, rand.Uint64())
	c.kv = true
	c.snapEntries = 5
	for _, id := range c.ids {
		c.start(id)
	}
	set = func(i int) string {
		return string(storage.EncodeSet(1, uint64(i+1), 0, 0, fmt.Sprint("k", i%7), []byte(fmt.Sprint("v", i))))
	}
	leader, _ = c.waitLeader()
	follower = c.ids[0]
	if follower == leader {
		follower = c.ids[1]
	}
	c.mustPropose(leader, set(0))
	c.waitApplied(set(0))
	c.stop(follower)
	behind := c.node(leader).Status().CommitIndex
	for i := 1; i <= 30; i++ {
		c.mustPropose(leader, set(i))
	}
	if s := c.node(leader).Status(); s.SnapshotIndex <= behind {
		t.Fatalf("leader snapshot index %d, want past the stopped follower's %d", s.SnapshotIndex, behind)
	}
	return c, leader, follower, set
}

func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, leader, follower, set := laggingCluster(t)
		defer c.stopAll()

		c.start(follower)
		c.waitApplied(set(30), follower)
		if c.net.installCount() == 0 {
			t.Fatal("follower caught up without an InstallSnapshot")
		}
		for i := 31; i <= 33; i++ {
			c.mustPropose(leader, set(i))
		}
		c.waitApplied(set(33), follower)
		installs := c.net.installCount()
		time.Sleep(10 * testElection)
		if got := c.net.installCount(); got != installs {
			t.Fatalf("%d more installs in an idle cluster with every follower caught up", got-installs)
		}

		_, want := c.nodeStore(leader)
		for _, id := range c.live() {
			if _, s := c.nodeStore(id); !bytes.Equal(s.Snapshot(), want.Snapshot()) {
				t.Fatalf("%s store differs from the leader's", id)
			}
		}
		c.checkSame()
	})
}

func TestHeartbeatsReachAFollowerBehindTheSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, leader, follower, set := laggingCluster(t)
		defer c.stopAll()
		_, term := c.waitLeader()

		c.net.setSlowAppend(time.Hour)
		c.start(follower)
		for range 30 {
			time.Sleep(testElection)
			if s := c.node(follower).Status(); s.Term != term || s.Leader != leader {
				t.Fatalf("follower behind the snapshot moved to term %d with leader %q, want term %d under %s", s.Term, s.Leader, term, leader)
			}
		}
		c.net.setSlowAppend(0)
		c.waitApplied(set(30), follower)
	})
}

func TestInstallSnapshotTailRule(t *testing.T) {
	for name, tc := range map[string]struct {
		lastTerm uint64
		keep     bool
	}{
		"conflicting tail discarded": {lastTerm: 2, keep: false},
		"matching tail kept":         {lastTerm: 1, keep: true},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dir := t.TempDir()
				al := &applyLog{}
				n := snapNode(t, dir, al)
				appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "x", "y", "z")})
				install(t, n, InstallSnapshotArgs{Term: 2, Leader: "c", LastIndex: 2, LastTerm: tc.lastTerm, Data: snapshotOf(applied{1, "x"}, applied{2, "y"})})
				if idx, _, _, _, err := readSnapshot(vfs.OS, dir); err != nil || idx != 2 {
					t.Fatalf("snapshot on disk once the install was answered: index %d, %v; want 2", idx, err)
				}
				probe := func(n *Node) {
					t.Helper()
					if r := appendEntries(t, n, AppendEntriesArgs{Term: 2, Leader: "c", PrevLogIndex: 3, PrevLogTerm: 1}); r.Success != tc.keep {
						t.Fatalf("entry 3 after the install: present %v, want %v", r.Success, tc.keep)
					}
				}
				probe(n)
				synctest.Wait()
				if got, want := al.snapshot(), []applied{{1, "x"}, {2, "y"}}; !slices.Equal(got, want) {
					t.Fatalf("state after install %v, want %v", got, want)
				}
				mustStop(t, n)

				n = snapNode(t, dir, &applyLog{})
				defer mustStop(t, n)
				probe(n)
			})
		})
	}
}

func TestStaleInstallSnapshotIsRejected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		al := &applyLog{}
		n := snapNode(t, dir, al)
		defer mustStop(t, n)
		appendEntries(t, n, AppendEntriesArgs{Term: 5, Leader: "b"})
		if r := install(t, n, InstallSnapshotArgs{Term: 4, Leader: "c", LastIndex: 10, LastTerm: 4, Data: snapshotOf(appliedN(10)...)}); r.Term != 5 {
			t.Fatalf("stale install answered with term %d, want 5", r.Term)
		}
		synctest.Wait()
		if s := n.Status(); s.Term != 5 || s.Leader != "b" || s.CommitIndex != 0 || s.SnapshotIndex != 0 {
			t.Fatalf("stale install changed status to %+v", s)
		}
		if _, err := os.Stat(filepath.Join(dir, "snapshot")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("stale install left a snapshot file: %v", err)
		}
		if got := al.snapshot(); len(got) != 0 {
			t.Fatalf("stale install restored %v", got)
		}
	})
}

// behindPeer grants every vote, fails every append as a peer with an empty log would, and answers InstallSnapshot with term 1000
type behindPeer struct{}

func (behindPeer) RequestVote(_ context.Context, _ string, args RequestVoteArgs) (RequestVoteReply, error) {
	return RequestVoteReply{Term: args.Term, Granted: true}, nil
}

func (behindPeer) AppendEntries(_ context.Context, _ string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	return AppendEntriesReply{Term: args.Term, ConflictIndex: 1}, nil
}

func (behindPeer) InstallSnapshot(context.Context, string, InstallSnapshotArgs) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{Term: 1000}, nil
}

func TestHigherTermInstallReplyStepsLeaderDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		if err := writeState(vfs.OS, dir, 1, ""); err != nil {
			t.Fatal(err)
		}
		if err := writeSnapshot(vfs.OS, filepath.Join(dir, "snapshot"), 5, 1, ids("a", "b", "c"), snapshotOf(appliedN(5)...)); err != nil {
			t.Fatal(err)
		}
		cfg := snapConfig(dir, &applyLog{})
		cfg.Transport = behindPeer{}
		cfg.ElectionTimeout, cfg.HeartbeatInterval = testElection, testHeartbeat
		n, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer mustStop(t, n)
		waitFor(t, time.Second, "a to adopt term 1000 from an install reply", func() bool { return n.Status().Term >= 1000 })
	})
}

func TestOlderInstallSnapshotDoesNotRollBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		al := &applyLog{}
		n := snapNode(t, dir, al)
		defer mustStop(t, n)
		install(t, n, InstallSnapshotArgs{Term: 1, Leader: "b", LastIndex: 5, LastTerm: 1, Data: snapshotOf(appliedN(5)...)})
		synctest.Wait()
		install(t, n, InstallSnapshotArgs{Term: 1, Leader: "b", LastIndex: 3, LastTerm: 1, Data: snapshotOf(appliedN(3)...)})
		synctest.Wait()
		if got := al.snapshot(); !slices.Equal(got, appliedN(5)) {
			t.Fatalf("state after an older install %v, want %v", got, appliedN(5))
		}
		if s := n.Status(); s.CommitIndex != 5 || s.SnapshotIndex != 5 {
			t.Fatalf("commit index %d, snapshot index %d after an older install; want 5, 5", s.CommitIndex, s.SnapshotIndex)
		}
		if idx, _, _, _, err := readSnapshot(vfs.OS, dir); err != nil || idx != 5 {
			t.Fatalf("snapshot file index %d, %v; want 5", idx, err)
		}
	})
}

func TestInstallWhileApplyingRestoresAfterTheBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		al := &applyLog{gate: make(chan struct{})}
		n := snapNode(t, t.TempDir(), al)
		defer mustStop(t, n)
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", Entries: entries(1, "e1", "slow", "e3"), LeaderCommit: 3})
		synctest.Wait()
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 3, PrevLogTerm: 1, Entries: entries(1, "e4"), LeaderCommit: 4})
		install(t, n, InstallSnapshotArgs{Term: 1, Leader: "b", LastIndex: 6, LastTerm: 1, Data: snapshotOf(appliedN(6)...)})
		close(al.gate)
		synctest.Wait()
		if got := al.snapshot(); !slices.Equal(got, appliedN(6)) {
			t.Fatalf("state after an install during a batch %v, want %v", got, appliedN(6))
		}
		appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 6, PrevLogTerm: 1, Entries: entries(1, "e7"), LeaderCommit: 7})
		synctest.Wait()
		if got := al.snapshot(); !slices.Equal(got, appliedN(7)) {
			t.Fatalf("state after the next entry %v, want %v", got, appliedN(7))
		}
	})
}

func TestAppendBelowSnapshotIsAcceptedPastIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		al := &applyLog{}
		n := snapNode(t, t.TempDir(), al)
		defer mustStop(t, n)
		install(t, n, InstallSnapshotArgs{Term: 1, Leader: "b", LastIndex: 5, LastTerm: 1, Data: snapshotOf(appliedN(5)...)})
		r := appendEntries(t, n, AppendEntriesArgs{Term: 1, Leader: "b", PrevLogIndex: 3, PrevLogTerm: 1, Entries: entries(1, "e4", "e5", "e6", "e7"), LeaderCommit: 7})
		if !r.Success {
			t.Fatalf("append overlapping the snapshot rejected: %+v", r)
		}
		synctest.Wait()
		if got := al.applyCalls(); !slices.Equal(got, []uint64{6, 7}) {
			t.Fatalf("applied indexes %v, want [6 7]", got)
		}
		if got := al.snapshot(); !slices.Equal(got, appliedN(7)) {
			t.Fatalf("state %v, want %v", got, appliedN(7))
		}
	})
}
