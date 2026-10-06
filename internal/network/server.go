package network

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
)

// Store is the key-value store a Server serves.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte) error
	Delete(key string) error
}

type Server struct {
	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

// Serve runs until ctx is done (returning nil) or Accept fails, then closes every connection and waits for handlers, so the store is idle once it returns.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	var (
		mu     sync.Mutex
		conns  = map[net.Conn]struct{}{}
		closed bool
		wg     sync.WaitGroup
	)
	shutdown := func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		closed = true
		for c := range conns {
			c.Close()
		}
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
		conns[conn] = struct{}{}
		wg.Add(1)
		mu.Unlock()

		go func() {
			defer wg.Done()
			s.handle(conn)
			mu.Lock()
			delete(conns, conn)
			mu.Unlock()
			conn.Close()
		}()
	}
}

// ponytail: no read deadlines, an idle or slow peer holds a goroutine and at most one frame of buffer until shutdown
func (s *Server) handle(conn net.Conn) {
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		req, err := readFrame(r)
		if err != nil {
			return
		}
		if err := writeFrame(w, s.exec(req)); err != nil {
			return
		}
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) exec(req []byte) []byte {
	op, key, value, err := parseRequest(req)
	if err != nil {
		return errorResponse(fmt.Errorf("malformed request: %w", err))
	}
	switch op {
	case opGet:
		v, ok := s.store.Get(key)
		if !ok {
			return []byte{statusNotFound}
		}
		return append([]byte{statusOK}, v...)
	case opSet:
		err = s.store.Set(key, value)
	case opDel:
		err = s.store.Delete(key)
	}
	if err != nil {
		return errorResponse(err)
	}
	return []byte{statusOK}
}

func errorResponse(err error) []byte {
	return append([]byte{statusError}, err.Error()...)
}
