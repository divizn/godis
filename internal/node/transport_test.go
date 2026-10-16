package node

import (
	"context"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/divizn/godis/internal/consensus"
)

type ackRaft struct{}

func (ackRaft) AppendEntries(_ consensus.AppendEntriesArgs, reply *consensus.AppendEntriesReply) error {
	reply.Success = true
	return nil
}

// stallConn stops reading once more than limit bytes have arrived, as a slow link would, until release is closed
type stallConn struct {
	net.Conn
	limit   int
	read    int
	stalled chan<- struct{}
	release <-chan struct{}
}

func (c *stallConn) Read(p []byte) (int, error) {
	if c.read > c.limit {
		select {
		case c.stalled <- struct{}{}:
		default:
		}
		<-c.release
	}
	n, err := c.Conn.Read(p)
	c.read += n
	return n, err
}

func TestHeartbeatIsNotQueuedBehindABigAppend(t *testing.T) {
	ln := listen(t, "127.0.0.1:0")
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", ackRaft{}); err != nil {
		t.Fatal(err)
	}
	stalled, release := make(chan struct{}, 1), make(chan struct{})
	var wg sync.WaitGroup
	defer wg.Wait()
	defer ln.Close()
	defer close(release)
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer conn.Close()
				srv.ServeConn(&stallConn{Conn: conn, limit: 1 << 20, stalled: stalled, release: release})
			})
		}
	})

	tr := &transport{addrs: map[string]string{"peer": ln.Addr().String()}, clients: map[conn]*rpc.Client{}}
	defer tr.close()
	big := consensus.AppendEntriesArgs{Term: 1, Entries: []consensus.Entry{{Term: 1, Cmd: make([]byte, 4<<20)}}}
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = tr.AppendEntries(ctx, "peer", big)
	})
	<-stalled

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := tr.AppendEntries(ctx, "peer", consensus.AppendEntriesArgs{Term: 1}); err != nil {
		t.Fatalf("heartbeat while a big append is stuck in transfer: %v", err)
	}
}
