package node

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/divizn/godis/client"
	"github.com/divizn/godis/internal/consensus"
)

// join starts a node with no configuration
func (c *testCluster) join(id string) Peer {
	c.t.Helper()
	cl, rl := listen(c.t, "127.0.0.1:0"), listen(c.t, "127.0.0.1:0")
	p := Peer{ID: id, RaftAddr: rl.Addr().String(), ClientAddr: cl.Addr().String()}
	c.joined = append(c.joined, p)
	c.startOn(id, cl, rl)
	return p
}

func (c *testCluster) add(via string, p Peer) error {
	return c.via(via, func(cl *client.Client) error {
		return cl.AddNode(context.Background(), p.ID, p.RaftAddr, p.ClientAddr)
	})
}

func (c *testCluster) remove(via, id string) error {
	return c.via(via, func(cl *client.Client) error { return cl.RemoveNode(context.Background(), id) })
}

func (c *testCluster) nodeIDs(via string) []string {
	c.t.Helper()
	var ids []string
	err := c.via(via, func(cl *client.Client) error {
		nodes, err := cl.Nodes(context.Background())
		for _, n := range nodes {
			ids = append(ids, n.ID)
		}
		return err
	})
	if err != nil {
		c.t.Fatalf("nodes via %s: %v", via, err)
	}
	slices.Sort(ids)
	return ids
}

func TestAddNodeThroughTheClient(t *testing.T) {
	c := newTestClusterOpts(t, 3, 150*time.Millisecond, true)
	leader := c.leader()
	if err := c.via(leader, set("before", "1")); err != nil {
		t.Fatalf("set: %v", err)
	}

	p := c.join("node-4")
	if err := c.add(c.other(leader), p); err != nil {
		t.Fatalf("add node-4: %v", err)
	}
	waitFor(t, "every node to list four members", func() error {
		for id := range c.nodes {
			if got := c.nodeIDs(id); len(got) != 4 {
				return fmt.Errorf("%s lists %v", id, got)
			}
		}
		return nil
	})
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if got := c.nodes["node-4"].tr.addrs[id]; got != c.peer(id).RaftAddr {
			t.Fatalf("new node dials %s at %q, want %q", id, got, c.peer(id).RaftAddr)
		}
	}

	if err := c.via("node-4", get(t, "before", "1", true)); err != nil {
		t.Fatalf("get through the new node: %v", err)
	}
	if err := c.via("node-4", set("through-new", "2")); err != nil {
		t.Fatalf("set through the new node: %v", err)
	}
	store := c.nodes["node-4"].store
	waitFor(t, "node-4 to apply both writes", func() error {
		a, _ := store.Get("before", 0)
		b, _ := store.Get("through-new", 0)
		if string(a) != "1" || string(b) != "2" {
			return fmt.Errorf("before = %q, through-new = %q", a, b)
		}
		return nil
	})

	// three of four are needed, so the new node counts
	var stopped string
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if id != leader {
			stopped = id
			break
		}
	}
	c.stop(stopped)
	if err := c.via("node-4", set("after", "3")); err != nil {
		t.Fatalf("set with a member down: %v", err)
	}
	if got := metric(t, c.regs["node-4"], "godis_raft_members"); got != 4 {
		t.Fatalf("godis_raft_members on the new node = %v, want 4", got)
	}
}

func TestRemoveNodeThroughTheClient(t *testing.T) {
	c := newTestClusterOpts(t, 4, 150*time.Millisecond, true)
	leader := c.leader()
	victim := c.other(leader)
	if err := c.remove(leader, victim); err != nil {
		t.Fatalf("remove %s: %v", victim, err)
	}
	for _, p := range c.peers {
		if p.ID == victim {
			continue
		}
		waitFor(t, p.ID+" to list three members", func() error {
			if got := c.nodeIDs(p.ID); len(got) != 3 || slices.Contains(got, victim) {
				return fmt.Errorf("lists %v", got)
			}
			return nil
		})
	}
	if got := metric(t, c.regs[leader], "godis_raft_members"); got != 3 {
		t.Fatalf("godis_raft_members = %v after a removal, want 3", got)
	}

	// two of three: the leader and one more, with the removed node and another down
	c.stop(victim)
	var other string
	for _, p := range c.peers {
		if p.ID != leader && p.ID != victim {
			other = p.ID
			break
		}
	}
	for _, p := range c.peers {
		if p.ID != leader && p.ID != victim && p.ID != other {
			c.stop(p.ID)
		}
	}
	if err := c.via(leader, set("after", "1")); err != nil {
		t.Fatalf("set with two of the three remaining members: %v", err)
	}
}

func TestRemoveTheLeaderThroughTheClient(t *testing.T) {
	c := newTestClusterOpts(t, 3, 150*time.Millisecond, true)
	old := c.leader()
	if err := c.remove(old, old); err != nil {
		t.Fatalf("remove the leader: %v", err)
	}
	rest := slices.DeleteFunc([]string{"node-1", "node-2", "node-3"}, func(id string) bool { return id == old })
	waitFor(t, "a new leader among the rest", func() error {
		for _, id := range rest {
			if c.nodes[id].Status().Role == consensus.Leader {
				return nil
			}
		}
		return fmt.Errorf("no leader among %v", rest)
	})
	if err := c.via(rest[0], set("after", "1")); err != nil {
		t.Fatalf("set after removing the leader: %v", err)
	}
	if got := c.nodeIDs(rest[0]); !slices.Equal(got, rest) {
		t.Fatalf("members %v, want %v", got, rest)
	}
}

func TestMembershipOpsAreRefusedByDefault(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	p := c.join("node-4")

	ctx := context.Background()
	for name, f := range map[string]func(*client.Client) error{
		"add":    func(cl *client.Client) error { return cl.AddNode(ctx, p.ID, p.RaftAddr, p.ClientAddr) },
		"remove": func(cl *client.Client) error { return cl.RemoveNode(ctx, c.other(leader)) },
		"nodes":  func(cl *client.Client) error { _, err := cl.Nodes(ctx); return err },
	} {
		err := c.via(leader, f)
		if err == nil || !strings.Contains(err.Error(), "--membership-ops") {
			t.Fatalf("%s without --membership-ops: got %v, want it refused", name, err)
		}
	}
	if got := len(c.nodes[leader].raft.Members()); got != 3 {
		t.Fatalf("refused operations left %d members", got)
	}
}
