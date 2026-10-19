// Package network is the godis client protocol server.
package network

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/divizn/godis/internal/protocol"
)

// Store is the key-value store a Server serves. Each write carries the client's session, (client, seq), and must
// take effect at most once per session. An error wrapping protocol.ErrOutcomeUnknown says the write may still apply,
// a *protocol.RedirectError that it never ran.
type Store interface {
	Get(key string) ([]byte, bool, error)
	// Set stores value with a deadline ttl from now, or none if ttl is 0.
	Set(client, seq uint64, key string, value []byte, ttl time.Duration) error
	Delete(client, seq uint64, key string) error
	Cas(client, seq uint64, key string, expected []byte, expectFound bool, value []byte) (swapped bool, err error)
	Incr(client, seq uint64, key string, delta int64) (int64, error)
	// Expire gives a live key a deadline ttl from now, deleting it if ttl is 0 or less, and reports whether the key was live.
	Expire(client, seq uint64, key string, ttl time.Duration) (found bool, err error)
	// TTL reports the time left on a key, 0 for no deadline, found false if the key is missing or expired.
	TTL(key string) (remaining time.Duration, found bool, err error)
	// AddNode adds a node to the cluster; it returns once the change has committed.
	AddNode(id, raftAddr, clientAddr string) error
	RemoveNode(id string) error
	Nodes() ([]protocol.Node, error)
}

type Server struct {
	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

// Serve runs until ctx is done (returning nil) or Accept fails, then closes idle connections, lets in-flight requests answer, and waits for handlers, so the store is idle once it returns.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var (
		mu     sync.Mutex
		busy   = map[net.Conn]bool{}
		closed bool
		wg     sync.WaitGroup
	)
	shutdown := func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		closed = true
		// ponytail: busy conns are left open, so a peer that stops reading its response stalls shutdown; add a write deadline here if it bites
		for c, b := range busy {
			if !b {
				c.Close()
			}
		}
	}
	// setBusy reports false once shutdown has begun, telling the handler to stop
	setBusy := func(c net.Conn, b bool) bool {
		mu.Lock()
		defer mu.Unlock()
		if closed {
			return false
		}
		busy[c] = b
		return true
	}
	stop := context.AfterFunc(ctx, shutdown)
	defer func() {
		stop()
		shutdown()
		wg.Wait()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// ponytail: any Accept error (EMFILE included) ends Serve; add backoff and retry like net/http if it bites
			return fmt.Errorf("accept: %w", err)
		}
		mu.Lock()
		if closed {
			mu.Unlock()
			conn.Close()
			continue
		}
		busy[conn] = false
		wg.Add(1)
		mu.Unlock()

		go func() {
			defer wg.Done()
			s.handle(conn, setBusy)
			mu.Lock()
			delete(busy, conn)
			mu.Unlock()
			conn.Close()
		}()
	}
}

// ponytail: no read deadlines, an idle or slow peer holds a goroutine and at most one frame of buffer until shutdown
func (s *Server) handle(conn net.Conn, setBusy func(net.Conn, bool) bool) {
	r := bufio.NewReader(conn)
	for {
		req, err := protocol.ReadFrame(r)
		if err != nil || !setBusy(conn, true) {
			return
		}
		if err := protocol.WriteFrame(conn, s.exec(req)); err != nil || !setBusy(conn, false) {
			return
		}
	}
}

func (s *Server) exec(p []byte) []byte {
	req, err := protocol.ParseRequest(p)
	if err != nil {
		return errorResponse(fmt.Errorf("malformed request: %w", err))
	}
	var body []byte
	switch req.Op {
	case protocol.OpGet:
		v, ok, err := s.store.Get(req.Key)
		if err != nil {
			return errorResponse(err)
		}
		if !ok {
			return []byte{protocol.StatusNotFound}
		}
		body = v
	case protocol.OpSet:
		err = s.store.Set(req.Client, req.Seq, req.Key, req.Value, time.Duration(req.TTL)*time.Millisecond)
	case protocol.OpDel:
		err = s.store.Delete(req.Client, req.Seq, req.Key)
	case protocol.OpCas:
		var swapped bool
		swapped, err = s.store.Cas(req.Client, req.Seq, req.Key, req.Expected, req.ExpectFound, req.Value)
		body = []byte{0}
		if swapped {
			body[0] = 1
		}
	case protocol.OpExpire:
		var found bool
		found, err = s.store.Expire(req.Client, req.Seq, req.Key, time.Duration(req.TTL)*time.Millisecond)
		body = []byte{0}
		if found {
			body[0] = 1
		}
	case protocol.OpTTL:
		remaining, found, err := s.store.TTL(req.Key)
		if err != nil {
			return errorResponse(err)
		}
		if !found {
			return []byte{protocol.StatusNotFound}
		}
		body = binary.AppendUvarint(nil, uint64(remaining.Milliseconds()))
	case protocol.OpIncr:
		var n int64
		n, err = s.store.Incr(req.Client, req.Seq, req.Key, req.Delta)
		body = strconv.AppendInt(nil, n, 10)
	case protocol.OpAddNode:
		err = s.store.AddNode(req.Key, req.RaftAddr, req.ClientAddr)
	case protocol.OpRemoveNode:
		err = s.store.RemoveNode(req.Key)
	case protocol.OpNodes:
		nodes, err := s.store.Nodes()
		if err != nil {
			return errorResponse(err)
		}
		body = protocol.AppendNodes(nil, nodes)
	}
	if err != nil {
		return errorResponse(err)
	}
	return append([]byte{protocol.StatusOK}, body...)
}

func errorResponse(err error) []byte {
	var re *protocol.RedirectError
	switch {
	case errors.As(err, &re):
		return append([]byte{protocol.StatusRedirect}, re.Addr...)
	case errors.Is(err, protocol.ErrOutcomeUnknown):
		return append([]byte{protocol.StatusUnknown}, err.Error()...)
	}
	return append([]byte{protocol.StatusError}, err.Error()...)
}
