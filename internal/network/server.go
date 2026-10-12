package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// Store is the key-value store a Server serves.
type Store interface {
	Get(key string) ([]byte, bool, error)
	Set(key string, value []byte) error
	Delete(key string) error
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
		req, err := readFrame(r)
		if err != nil || !setBusy(conn, true) {
			return
		}
		if err := writeFrame(conn, s.exec(req)); err != nil || !setBusy(conn, false) {
			return
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
		v, ok, err := s.store.Get(key)
		if err != nil {
			return errorResponse(err)
		}
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
	var re *RedirectError
	if errors.As(err, &re) {
		return append([]byte{statusRedirect}, re.Addr...)
	}
	return append([]byte{statusError}, err.Error()...)
}
