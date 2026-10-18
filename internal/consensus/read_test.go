package consensus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func readIndex(c *cluster, id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return c.node(id).ReadIndex(ctx)
}

func TestReadIndexNeedsLeadershipConfirmed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		old, term := c.waitLeader()
		c.mustPropose(old, "a")
		if err := readIndex(c, old); err != nil {
			t.Fatalf("read index on a healthy leader: %v", err)
		}

		rest := slices.DeleteFunc(slices.Clone(c.ids), func(id string) bool { return id == old })
		c.net.partition([]string{old}, rest)
		if err := readIndex(c, old); err == nil {
			t.Fatal("leader partitioned alone confirmed a read")
		}

		waitFor(t, 10*time.Second, "a new leader", func() bool {
			_, newTerm := c.leaderAmong(rest...)
			return newTerm > term
		})
		id, _ := c.leaderAmong(rest...)
		c.mustPropose(id, "b")
		c.net.heal()
		if err := readIndex(c, old); err == nil {
			t.Fatal("deposed leader confirmed a read with replies from a newer term")
		}
	})
}

func TestReadIndexWaitsForApply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		n, err := New(Config{
			ID:        "a",
			Peers:     []string{"a"},
			Dir:       t.TempDir(),
			Transport: memTransport{net: newMemNet(1), from: "a"},
			Apply: func(_ uint64, cmd []byte) []byte {
				if string(cmd) == "slow" {
					<-release
				}
				return nil
			},
			ElectionTimeout:   testElection,
			HeartbeatInterval: testHeartbeat,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer mustStop(t, n)
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}()

		waitFor(t, 10*time.Second, "leader", func() bool { return n.Status().Role == Leader })
		proposed := make(chan error, 1)
		go func() { _, err := n.Propose(context.Background(), []byte("slow")); proposed <- err }()
		waitFor(t, 10*time.Second, "slow to commit", func() bool { return n.Status().CommitIndex >= 2 })

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := n.ReadIndex(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read index while a committed entry is unapplied: got %v, want deadline exceeded", err)
		}
		close(release)
		if err := n.ReadIndex(context.Background()); err != nil {
			t.Fatalf("read index after apply: %v", err)
		}
		if err := <-proposed; err != nil {
			t.Fatalf("propose: %v", err)
		}
	})
}

func TestReadIndexWaitsForNewLeaderToCommitInItsTerm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 3)
		defer c.stopAll()

		leader, _ := c.waitLeader()
		c.mustPropose(leader, "a")
		c.waitApplied("a")
		c.stopAll()
		c.net.setSlowAppend(time.Hour)
		for _, id := range c.ids {
			c.start(id)
		}

		leader, _ = c.waitLeader()
		if err := readIndex(c, leader); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read index on a new leader that has not committed in its term: got %v with a applied %v, want deadline exceeded", err, c.log(leader).has("a"))
		}
		c.net.setSlowAppend(0)
		if err := readIndex(c, leader); err != nil {
			t.Fatalf("read index once appends flow: %v", err)
		}
		if !c.log(leader).has("a") {
			t.Fatal("read index returned before the leader applied a")
		}
	})
}
