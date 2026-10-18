package node

import (
	"fmt"
	"testing"

	"github.com/divizn/godis/internal/network"
	"github.com/prometheus/client_golang/prometheus"
)

// metric is the value of a counter or gauge, or the sample count of a histogram, whose labels include want; -1 if absent
func metric(t *testing.T, reg *prometheus.Registry, name string, want ...string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metrics:
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			for i := 0; i < len(want); i += 2 {
				if labels[want[i]] != want[i+1] {
					continue metrics
				}
			}
			switch {
			case m.Counter != nil:
				return m.Counter.GetValue()
			case m.Gauge != nil:
				return m.Gauge.GetValue()
			case m.Histogram != nil:
				return float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return -1
}

func TestMetrics(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.leader()
	follower := c.other(leader)
	waitFor(t, follower+" to learn the leader", func() error {
		if s := c.nodes[follower].Status(); s.Leader != leader {
			return fmt.Errorf("%s follows %q", follower, s.Leader)
		}
		return nil
	})

	if err := c.via(follower, set("a", "1")); err != nil {
		t.Fatalf("set through follower: %v", err)
	}
	if err := c.via(leader, set("b", "2")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := c.via(leader, get(t, "b", "2", true)); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := c.via(leader, get(t, "missing", "", false)); err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if err := c.via(leader, del("b")); err != nil {
		t.Fatalf("del: %v", err)
	}

	lr, fr := c.regs[leader], c.regs[follower]
	for _, m := range []struct {
		reg    *prometheus.Registry
		name   string
		labels []string
		want   float64
	}{
		{fr, "godis_requests_total", []string{"op", "set", "result", "redirect"}, 1},
		{lr, "godis_requests_total", []string{"op", "set", "result", "ok"}, 2},
		{lr, "godis_requests_total", []string{"op", "get", "result", "ok"}, 1},
		{lr, "godis_requests_total", []string{"op", "get", "result", "not_found"}, 1},
		{lr, "godis_requests_total", []string{"op", "del", "result", "ok"}, 1},
		{lr, "godis_request_duration_seconds", []string{"op", "set"}, 2},
		{lr, "godis_request_duration_seconds", []string{"op", "get"}, 2},
		{lr, "godis_raft_leader", nil, 1},
		{fr, "godis_raft_leader", nil, 0},
		{lr, "godis_raft_term", nil, float64(c.nodes[leader].Status().Term)},
		{lr, "godis_raft_commit_index", nil, float64(c.nodes[leader].Status().CommitIndex)},
		{lr, "godis_duplicate_writes_total", nil, 0},
	} {
		if got := metric(t, m.reg, m.name, m.labels...); got != m.want {
			t.Errorf("%s%v = %v, want %v", m.name, m.labels, got, m.want)
		}
	}
	for _, reg := range []*prometheus.Registry{lr, fr} {
		if got := metric(t, reg, "godis_raft_leader_changes_total"); got < 1 {
			t.Errorf("godis_raft_leader_changes_total = %v after an election, want at least 1", got)
		}
	}

	for _, p := range c.peers {
		if p.ID != leader {
			c.stop(p.ID)
		}
	}
	cl, err := network.Dial(c.peer(leader).ClientAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// the client retries the get for 10s, so watch the metric rather than wait for it
	go cl.Get("a")
	waitFor(t, "a get without a quorum to count as unknown", func() error {
		if got := metric(t, lr, "godis_requests_total", "op", "get", "result", "unknown"); got < 1 {
			return fmt.Errorf("counted %v", got)
		}
		return nil
	})
}
