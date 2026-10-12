package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"

	"github.com/divizn/godis/internal/consensus"
)

// transport sends Raft RPCs over net/rpc, one lazily dialed connection per peer, redialed after any connection error
type transport struct {
	addrs map[string]string

	mu      sync.Mutex
	clients map[string]*rpc.Client
}

func (t *transport) RequestVote(ctx context.Context, to string, args consensus.RequestVoteArgs) (consensus.RequestVoteReply, error) {
	reply := new(consensus.RequestVoteReply)
	if err := t.call(ctx, to, "Raft.RequestVote", args, reply); err != nil {
		return consensus.RequestVoteReply{}, err
	}
	return *reply, nil
}

func (t *transport) AppendEntries(ctx context.Context, to string, args consensus.AppendEntriesArgs) (consensus.AppendEntriesReply, error) {
	reply := new(consensus.AppendEntriesReply)
	if err := t.call(ctx, to, "Raft.AppendEntries", args, reply); err != nil {
		return consensus.AppendEntriesReply{}, err
	}
	return *reply, nil
}

// call must not touch reply after returning an error, since net/rpc may still decode into it
func (t *transport) call(ctx context.Context, to, method string, args, reply any) error {
	c, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if call.Error != nil {
		var se rpc.ServerError
		if !errors.As(call.Error, &se) {
			t.drop(to, c)
		}
		return fmt.Errorf("%s to %s: %w", method, to, call.Error)
	}
	return nil
}

func (t *transport) client(ctx context.Context, to string) (*rpc.Client, error) {
	t.mu.Lock()
	c := t.clients[to]
	t.mu.Unlock()
	if c != nil {
		return c, nil
	}
	addr, ok := t.addrs[to]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", to)
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", to, err)
	}
	c = rpc.NewClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	if existing := t.clients[to]; existing != nil {
		c.Close()
		return existing, nil
	}
	t.clients[to] = c
	return c, nil
}

func (t *transport) drop(to string, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *transport) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for to, c := range t.clients {
		c.Close()
		delete(t.clients, to)
	}
}

// raftService serves a node's Raft RPCs; a handler error makes net/rpc send an error instead of a reply
type raftService struct {
	raft *consensus.Node
}

func (s raftService) RequestVote(args consensus.RequestVoteArgs, reply *consensus.RequestVoteReply) (err error) {
	*reply, err = s.raft.HandleRequestVote(args)
	return err
}

func (s raftService) AppendEntries(args consensus.AppendEntriesArgs, reply *consensus.AppendEntriesReply) (err error) {
	*reply, err = s.raft.HandleAppendEntries(args)
	return err
}
