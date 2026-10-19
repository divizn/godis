package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/divizn/godis/internal/node"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	port := flag.Int("port", 8080, "client port to listen on")
	raftPort := flag.Int("raft-port", 7000, "raft port to listen on")
	metricsPort := flag.Int("metrics-port", 2112, "port serving prometheus metrics at /metrics")
	id := flag.String("id", "node-1", "node id")
	dataDir := flag.String("data-dir", "data", "directory for the raft state and log")
	election := flag.Duration("election-timeout", time.Second, "raft election timeout, randomized up to twice this; heartbeats are a tenth of it")
	snapshotEntries := flag.Uint64("snapshot-entries", 10000, "applied log entries that trigger a snapshot and log compaction")
	cluster := flag.String("cluster", "", "every member as id=raft-addr/client-addr, comma separated; empty means a cluster of one")
	join := flag.Bool("join", false, "start with no cluster configuration, to be added to a running cluster with CLUSTER ADD")
	membershipOps := flag.Bool("membership-ops", false, "let clients add, remove and list nodes; these requests are not authenticated")
	flag.Parse()

	peers, err := bootstrapPeers(*cluster, *join, *id, *raftPort, *port)
	if err != nil {
		log.Fatal(err)
	}

	cl, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen on port %d: %v", *port, err)
	}
	rl, err := net.Listen("tcp", fmt.Sprintf(":%d", *raftPort))
	if err != nil {
		log.Fatalf("listen on raft port %d: %v", *raftPort, err)
	}
	ml, err := net.Listen("tcp", fmt.Sprintf(":%d", *metricsPort))
	if err != nil {
		log.Fatalf("listen on metrics port %d: %v", *metricsPort, err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	n, err := node.Start(node.Config{ID: *id, Peers: peers, Dir: *dataDir, ClientListener: cl, RaftListener: rl, ElectionTimeout: *election, Metrics: reg, SnapshotEntries: *snapshotEntries, MembershipOps: *membershipOps})
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metrics := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metrics.Serve(ml); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server stopped", "node", *id, "err", err)
		}
	}()
	slog.Info("listening", "node", *id, "client", cl.Addr().String(), "raft", rl.Addr().String(), "metrics", ml.Addr().String(), "data", *dataDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		slog.Info("shutting down", "node", *id)
	case <-n.Done():
		slog.Error("raft halted, shutting down", "node", *id, "err", n.Err())
	}
	if err := n.Stop(); err != nil {
		log.Fatalf("stop node: %v", err)
	}
	if err := metrics.Shutdown(context.Background()); err != nil {
		log.Fatalf("stop metrics server: %v", err)
	}
}

// bootstrapPeers is the configuration a node starts with: none to join a running cluster, the --cluster members, or itself alone
func bootstrapPeers(cluster string, join bool, id string, raftPort, port int) ([]node.Peer, error) {
	switch {
	case join && cluster != "":
		return nil, errors.New("--join starts a node with no configuration, so it cannot be combined with --cluster")
	case join:
		return nil, nil
	case cluster == "":
		cluster = fmt.Sprintf("%s=localhost:%d/localhost:%d", id, raftPort, port)
	}
	peers, err := parseCluster(cluster)
	if err != nil {
		return nil, fmt.Errorf("parse --cluster: %w", err)
	}
	return peers, nil
}

func parseCluster(s string) ([]node.Peer, error) {
	var peers []node.Peer
	for member := range strings.SplitSeq(s, ",") {
		member = strings.TrimSpace(member)
		id, addrs, ok := strings.Cut(member, "=")
		raft, client, ok2 := strings.Cut(addrs, "/")
		if !ok || !ok2 || id == "" || raft == "" || client == "" {
			return nil, fmt.Errorf("member %q is not id=raft-addr/client-addr", member)
		}
		peers = append(peers, node.Peer{ID: id, RaftAddr: raft, ClientAddr: client})
	}
	return peers, nil
}
