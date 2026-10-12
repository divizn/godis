package node

import (
	"errors"
	"time"

	"github.com/divizn/godis/internal/consensus"
	"github.com/divizn/godis/internal/network"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	requests      *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	leaderChanges prometheus.Counter
}

// newMetrics builds the node's collectors, registering them only if reg is not nil
func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "godis_requests_total",
			Help: "Client requests by operation and result.",
		}, []string{"op", "result"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "godis_request_duration_seconds",
			Help:    "Time to serve a client request, Raft round trip included.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		leaderChanges: f.NewCounter(prometheus.CounterOpts{
			Name: "godis_raft_leader_changes_total",
			Help: "Times this node learned of a new leader.",
		}),
	}
}

func registerRaftGauges(reg prometheus.Registerer, raft *consensus.Node) {
	f := promauto.With(reg)
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "godis_raft_term", Help: "Current Raft term."},
		func() float64 { return float64(raft.Status().Term) })
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "godis_raft_commit_index", Help: "Highest Raft log index known to be committed."},
		func() float64 { return float64(raft.Status().CommitIndex) })
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "godis_raft_leader", Help: "1 if this node is the Raft leader, else 0."},
		func() float64 {
			if raft.Status().Role == consensus.Leader {
				return 1
			}
			return 0
		})
}

func (m *metrics) observe(op string, start time.Time, found bool, err error) {
	var re *network.RedirectError
	result := "ok"
	switch {
	case errors.As(err, &re):
		result = "redirect"
	case err != nil:
		result = "error"
	case !found:
		result = "not_found"
	}
	m.requests.WithLabelValues(op, result).Inc()
	m.duration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}
