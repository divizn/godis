package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/divizn/godis/internal/node"
)

func main() {
	port := flag.Int("port", 8080, "client port to listen on")
	raftPort := flag.Int("raft-port", 7000, "raft port to listen on")
	id := flag.String("id", "node-1", "node id")
	dataDir := flag.String("data-dir", "data", "directory for the raft state and log")
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
	n, err := node.Start(node.Config{ID: *id, Peers: peers, Dir: *dataDir, ClientListener: cl, RaftListener: rl})
	if err != nil {
		log.Fatalf("start node: %v", err)
	}
	slog.Info("listening", "node", *id, "client", cl.Addr().String(), "raft", rl.Addr().String(), "data", *dataDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	slog.Info("shutting down", "node", *id)
	if err := n.Stop(); err != nil {
		slog.Error("stop node", "err", err)
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
