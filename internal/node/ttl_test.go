package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/divizn/godis/client"
	"github.com/divizn/godis/internal/storage"
)

var testCtx = context.Background()

func setTTL(key, value string, ttl time.Duration) func(*client.Client) error {
	return func(cl *client.Client) error { return cl.SetTTL(testCtx, key, []byte(value), ttl) }
}

// restartWithSweep restarts every node with the given sweep interval
func (c *testCluster) restartWithSweep(interval time.Duration) {
	c.stopAll()
	c.sweep = interval
	for _, p := range c.peers {
		c.start(p.ID)
	}
}

func TestSetTTLExpiresAndReportsRemaining(t *testing.T) {
	c := newTestCluster(t, 3)
	c.restartWithSweep(time.Hour)
	leader := c.leader()
	follower := c.other(leader)

	const ttl = 1500 * time.Millisecond
	start := time.Now()
	if err := c.via(follower, setTTL("k", "v", ttl)); err != nil {
		t.Fatalf("set with ttl through follower: %v", err)
	}
	if err := c.via(leader, set("plain", "v")); err != nil {
		t.Fatal(err)
	}
	err := c.via(leader, func(cl *client.Client) error {
		if d, found, err := cl.TTL(testCtx, "k"); err != nil || !found || d <= 0 || d > ttl {
			t.Errorf("ttl of k: %v, %v, %v; want within (0, %v]", d, found, err, ttl)
		}
		if d, found, err := cl.TTL(testCtx, "plain"); err != nil || !found || d != 0 {
			t.Errorf("ttl of a key with no deadline: %v, %v, %v", d, found, err)
		}
		if _, found, err := cl.TTL(testCtx, "missing"); err != nil || found {
			t.Errorf("ttl of a missing key: %v, %v", found, err)
		}
		return get(t, "k", "v", true)(cl)
	})
	if err != nil {
		t.Fatal(err)
	}

	waitFor(t, "k to expire", func() error {
		return c.via(follower, func(cl *client.Client) error {
			if _, ok, err := cl.Get(testCtx, "k"); err != nil || ok {
				return fmt.Errorf("get k: %v, %v", ok, err)
			}
			return nil
		})
	})
	if d := time.Since(start); d < ttl {
		t.Fatalf("k read as missing after %v, before its %v ttl", d, ttl)
	}
	if err := c.via(leader, get(t, "plain", "v", true)); err != nil {
		t.Fatal(err)
	}
	if err := c.via(leader, func(cl *client.Client) error {
		if _, found, err := cl.TTL(testCtx, "k"); err != nil || found {
			return fmt.Errorf("ttl of an expired key: %v, %v", found, err)
		}
		if ok, err := cl.Expire(testCtx, "k", time.Hour); err != nil || ok {
			return fmt.Errorf("expire of an expired key: %v, %v", ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExpireThroughTheCluster(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	if err := c.via(leader, func(cl *client.Client) error {
		if err := cl.Set(testCtx, "k", []byte("v")); err != nil {
			return err
		}
		if ok, err := cl.Expire(testCtx, "k", time.Hour); err != nil || !ok {
			return fmt.Errorf("expire of a live key: %v, %v", ok, err)
		}
		if d, found, err := cl.TTL(testCtx, "k"); err != nil || !found || d < 59*time.Minute || d > time.Hour {
			return fmt.Errorf("ttl after expire: %v, %v, %v", d, found, err)
		}
		if ok, err := cl.Expire(testCtx, "k", 0); err != nil || !ok {
			return fmt.Errorf("expire with ttl 0: %v, %v", ok, err)
		}
		if ok, err := cl.Expire(testCtx, "k", time.Hour); err != nil || ok {
			return fmt.Errorf("expire of a deleted key: %v, %v", ok, err)
		}
		return get(t, "k", "", false)(cl)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSweepDropsExpiredKeysOnEveryNode(t *testing.T) {
	c := newTestCluster(t, 3)
	c.restartWithSweep(50 * time.Millisecond)
	leader := c.leader()

	if err := c.via(leader, set("keep", "v")); err != nil {
		t.Fatal(err)
	}
	if err := c.via(leader, setTTL("short", "v", 200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "every node to drop the expired key", func() error {
		for id, n := range c.nodes {
			if got := n.store.Len(); got != 1 {
				return fmt.Errorf("%s holds %d keys", id, got)
			}
			if got := metric(t, c.regs[id], "godis_expired_keys_total"); got != 1 {
				return fmt.Errorf("%s counted %v expired keys", id, got)
			}
		}
		return nil
	})
	if err := c.via(leader, get(t, "keep", "v", true)); err != nil {
		t.Fatal(err)
	}
}

func TestNothingDueProposesNoSweep(t *testing.T) {
	c := newTestCluster(t, 3)
	c.restartWithSweep(20 * time.Millisecond)
	leader := c.leader()
	if err := c.via(leader, set("plain", "v")); err != nil {
		t.Fatal(err)
	}
	if err := c.via(leader, setTTL("later", "v", time.Hour)); err != nil {
		t.Fatal(err)
	}
	before := c.nodes[leader].Status().CommitIndex
	time.Sleep(500 * time.Millisecond)
	if after := c.nodes[leader].Status().CommitIndex; after != before {
		t.Fatalf("commit index moved from %d to %d with nothing due", before, after)
	}
}

func TestDeadlineSurvivesALeaderKill(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	survivor := c.other(leader)

	const ttl = 3 * time.Second
	start := time.Now()
	if err := c.via(leader, setTTL("k", "v", ttl)); err != nil {
		t.Fatal(err)
	}
	c.stop(leader)
	if err := c.via(survivor, get(t, "k", "v", true)); err != nil {
		t.Fatalf("get after the leader died: %v", err)
	}
	waitFor(t, "k to expire on the new leader", func() error {
		return c.via(survivor, func(cl *client.Client) error {
			if _, ok, err := cl.Get(testCtx, "k"); err != nil || ok {
				return fmt.Errorf("get k: %v, %v", ok, err)
			}
			return nil
		})
	})
	if d := time.Since(start); d < ttl {
		t.Fatalf("k expired after %v, before its %v ttl", d, ttl)
	}
}

func TestStampIsNeverBehindTheStoreClock(t *testing.T) {
	st := storage.New()
	future := uint64(time.Now().Add(time.Hour).UnixMilli())
	if _, err := st.Apply(1, storage.EncodeSet(1, 1, future, 0, "k", nil)); err != nil {
		t.Fatal(err)
	}
	if got := (kvStore{store: st}).now(); got != future {
		t.Fatalf("stamp %d with the store clock at %d", got, future)
	}
}
