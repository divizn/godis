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
	cluster := flag.String("cluster", "", "every member as id=raft-addr/client-addr, comma separated; empty means a cluster of one")
	flag.Parse()

	if *cluster == "" {
		*cluster = fmt.Sprintf("%s=localhost:%d/localhost:%d", *id, *raftPort, *port)
	}
	peers, err := parseCluster(*cluster)
	if err != nil {
		log.Fatalf("parse --cluster: %v", err)
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
	n, err := node.Start(node.Config{ID: *id, Peers: peers, Dir: *dataDir, ClientListener: cl, RaftListener: rl, ElectionTimeout: *election, Metrics: reg})
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
