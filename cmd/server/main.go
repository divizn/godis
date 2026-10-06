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
	"path/filepath"
	"syscall"

	"github.com/divizn/godis/internal/storage"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	id := flag.String("id", "node-1", "node id")
	dataDir := flag.String("data-dir", "data", "directory for the write-ahead log")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}
	walPath := filepath.Join(*dataDir, "wal")
	db, err := storage.Open(walPath)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	slog.Info("storage ready", "node", *id, "wal", walPath)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen on port %d: %v", *port, err)
	}
	slog.Info("listening", "node", *id, "addr", ln.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	ln.Close()
	if err := db.Close(); err != nil {
		slog.Error("close storage", "err", err)
	}
	slog.Info("shutting down", "node", *id)
}
