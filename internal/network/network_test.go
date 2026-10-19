package network

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/client"
	"github.com/divizn/godis/internal/protocol"
	"github.com/divizn/godis/internal/storage"
)

// memStore applies writes to a storage.Store as a one-node log would, so retried writes are deduplicated
type memStore struct {
	s     *storage.Store
	mu    sync.Mutex
	index uint64
	nodes []protocol.Node
}

func (m *memStore) AddNode(id, raftAddr, clientAddr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slices.ContainsFunc(m.nodes, func(n protocol.Node) bool { return n.ID == id }) {
		return fmt.Errorf("node %s is already a member", id)
	}
	m.nodes = append(m.nodes, protocol.Node{ID: id, RaftAddr: raftAddr, ClientAddr: clientAddr})
	return nil
}

func (m *memStore) RemoveNode(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := slices.IndexFunc(m.nodes, func(n protocol.Node) bool { return n.ID == id })
	if i < 0 {
		return fmt.Errorf("node %s is not a member", id)
	}
	m.nodes = slices.Delete(m.nodes, i, i+1)
	return nil
}

func (m *memStore) Nodes() ([]protocol.Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.nodes), nil
}

func newMemStore() *memStore { return &memStore{s: storage.New()} }

func now() uint64 { return uint64(time.Now().UnixMilli()) }

func deadline(ttl time.Duration) uint64 {
	if ttl == 0 {
		return 0
	}
	return now() + uint64(ttl.Milliseconds())
}

func (m *memStore) Get(key string) ([]byte, bool, error) {
	v, ok := m.s.Get(key, now())
	return v, ok, nil
}

func (m *memStore) Expire(client, seq uint64, key string, ttl time.Duration) (bool, error) {
	d := now()
	if ttl > 0 {
		d += uint64(ttl.Milliseconds())
	}
	r, err := m.apply(storage.EncodeExpire(client, seq, now(), key, d))
	return len(r) == 1 && r[0] == 1, err
}

func (m *memStore) TTL(key string) (time.Duration, bool, error) {
	ms, ok := m.s.TTL(key, now())
	return time.Duration(ms) * time.Millisecond, ok, nil
}

func (m *memStore) apply(cmd []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.index++
	r, err := m.s.Apply(m.index, cmd)
	if err != nil {
		return nil, err
	}
	return storage.ParseResult(r)
}

func (m *memStore) Set(client, seq uint64, key string, value []byte, ttl time.Duration) error {
	_, err := m.apply(storage.EncodeSet(client, seq, now(), deadline(ttl), key, value))
	return err
}

func (m *memStore) Delete(client, seq uint64, key string) error {
	_, err := m.apply(storage.EncodeDelete(client, seq, now(), key))
	return err
}

func (m *memStore) Cas(client, seq uint64, key string, expected []byte, expectFound bool, value []byte) (bool, error) {
	r, err := m.apply(storage.EncodeCas(client, seq, now(), 0, key, expected, expectFound, value))
	return len(r) == 1 && r[0] == 1, err
}

func (m *memStore) Incr(client, seq uint64, key string, delta int64) (int64, error) {
	r, err := m.apply(storage.EncodeIncr(client, seq, now(), key, delta))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(r), 10, 64)
}

// nopWrites gives a fake store the write methods it does not exercise
type nopWrites struct{}

func (nopWrites) Delete(uint64, uint64, string) error { return nil }
func (nopWrites) Cas(uint64, uint64, string, []byte, bool, []byte) (bool, error) {
	return false, nil
}
func (nopWrites) Incr(uint64, uint64, string, int64) (int64, error) { return 0, nil }
func (nopWrites) Expire(uint64, uint64, string, time.Duration) (bool, error) {
	return false, nil
}
func (nopWrites) TTL(string) (time.Duration, bool, error) { return 0, false, nil }
func (nopWrites) AddNode(string, string, string) error    { return nil }
func (nopWrites) RemoveNode(string) error                 { return nil }
func (nopWrites) Nodes() ([]protocol.Node, error)         { return nil, nil }

func listen(t testing.TB) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

// serve runs a server on 127.0.0.1:0; stop cancels it and waits for Serve
func serve(t testing.TB, store Store) (string, func()) {
	t.Helper()
	return serveOn(t, listen(t), store)
}

func serveOn(t testing.TB, ln net.Listener, store Store) (string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewServer(store).Serve(ctx, ln) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("serve: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

func dial(t testing.TB, addrs ...string) *client.Client {
	t.Helper()
	c, err := client.Dial(addrs...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// within is a context that ends after d, bounding the client's retries
func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

func mustGet(t *testing.T, c *client.Client, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := c.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return v, ok
}

func TestRoundTripValues(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	c := dial(t, addr)

	large := bytes.Repeat([]byte("0123456789abcdef"), 1<<16)
	cases := map[string][]byte{
		"empty":         {},
		"bin\x00\xff\n": {0, 0xff, 1, '\n', 0},
		"large":         large,
		"":              []byte("empty key"),
	}
	for k, v := range cases {
		if err := c.Set(t.Context(), k, v); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	for k, want := range cases {
		got, ok := mustGet(t, c, k)
		if !ok || !bytes.Equal(got, want) {
			t.Errorf("get %q: got %d bytes, %v; want %d bytes", k, len(got), ok, len(want))
		}
	}
}

func TestEndToEnd(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	c := dial(t, addr)

	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}} {
		if err := c.Set(t.Context(), kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("set %s: %v", kv[0], err)
		}
	}
	if err := c.Del(t.Context(), "b"); err != nil {
		t.Fatalf("del: %v", err)
	}
	if v, ok := mustGet(t, c, "a"); !ok || string(v) != "3" {
		t.Fatalf("get a: got %q, %v; want 3", v, ok)
	}
	if v, ok := mustGet(t, c, "b"); ok {
		t.Fatalf("get deleted b: got %q", v)
	}
	if v, ok := mustGet(t, c, "never"); ok {
		t.Fatalf("get missing key: got %q", v)
	}
}

func TestPipelinedRequestsAnsweredInOrder(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	const n = 50
	var out bytes.Buffer
	for i := range n {
		set := protocol.Request{Op: protocol.OpSet, Client: 1, Seq: uint64(i + 1), Key: "k", Value: fmt.Appendf(nil, "%d", i)}.Append(nil)
		if err := protocol.WriteFrame(&out, set); err != nil {
			t.Fatal(err)
		}
		if err := protocol.WriteFrame(&out, protocol.Request{Op: protocol.OpGet, Key: "k"}.Append(nil)); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		if _, err := conn.Write(out.Bytes()); err != nil {
			t.Error(err)
		}
	}()

	r := bufio.NewReader(conn)
	for i := range n {
		for _, want := range []string{"", fmt.Sprint(i)} {
			resp, err := protocol.ReadFrame(r)
			if err != nil {
				t.Fatalf("request %d: %v", i, err)
			}
			if resp[0] != protocol.StatusOK || string(resp[1:]) != want {
				t.Fatalf("request %d: got status %d body %q, want OK %q", i, resp[0], resp[1:], want)
			}
		}
	}
}

func TestConcurrentClients(t *testing.T) {
	addr, _ := serve(t, newMemStore())

	const clients, ops = 16, 30
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		wg.Go(func() {
			c, err := client.Dial(addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			for j := range ops {
				key := fmt.Sprintf("c%d-%d", i, j)
				want := strings.Repeat(key, j+1)
				if err := c.Set(t.Context(), key, []byte(want)); err != nil {
					errs <- err
					return
				}
				got, ok, err := c.Get(t.Context(), key)
				if err != nil || !ok || string(got) != want {
					errs <- fmt.Errorf("get %s: got %q, %v, %v; want %q", key, got, ok, err, want)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestTTLOps(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	do := func(req protocol.Request) []byte {
		t.Helper()
		if err := protocol.WriteFrame(conn, req.Append(nil)); err != nil {
			t.Fatal(err)
		}
		resp, err := protocol.ReadFrame(r)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	remaining := func(resp []byte) uint64 {
		t.Helper()
		if resp[0] != protocol.StatusOK {
			t.Fatalf("ttl answered status %d %q", resp[0], resp[1:])
		}
		ms, n := binary.Uvarint(resp[1:])
		if n != len(resp)-1 {
			t.Fatalf("ttl body %v is not one uvarint", resp[1:])
		}
		return ms
	}

	if resp := do(protocol.Request{Op: protocol.OpTTL, Key: "k"}); resp[0] != protocol.StatusNotFound {
		t.Fatalf("ttl of a missing key: status %d", resp[0])
	}
	if resp := do(protocol.Request{Op: protocol.OpExpire, Client: 1, Seq: 1, Key: "k", TTL: 5000}); !bytes.Equal(resp, []byte{protocol.StatusOK, 0}) {
		t.Fatalf("expire of a missing key: %v", resp)
	}
	do(protocol.Request{Op: protocol.OpSet, Client: 1, Seq: 2, Key: "k", Value: []byte("v"), TTL: 60_000})
	if ms := remaining(do(protocol.Request{Op: protocol.OpTTL, Key: "k"})); ms == 0 || ms > 60_000 {
		t.Fatalf("ttl after set with 60s: %d ms", ms)
	}
	if resp := do(protocol.Request{Op: protocol.OpExpire, Client: 1, Seq: 3, Key: "k", TTL: 120_000}); !bytes.Equal(resp, []byte{protocol.StatusOK, 1}) {
		t.Fatalf("expire of a live key: %v", resp)
	}
	if ms := remaining(do(protocol.Request{Op: protocol.OpTTL, Key: "k"})); ms <= 60_000 || ms > 120_000 {
		t.Fatalf("ttl after expire with 120s: %d ms", ms)
	}
	do(protocol.Request{Op: protocol.OpSet, Client: 1, Seq: 4, Key: "k", Value: []byte("v")})
	if ms := remaining(do(protocol.Request{Op: protocol.OpTTL, Key: "k"})); ms != 0 {
		t.Fatalf("ttl of a key with no deadline: %d", ms)
	}
	do(protocol.Request{Op: protocol.OpExpire, Client: 1, Seq: 5, Key: "k", TTL: 0})
	if resp := do(protocol.Request{Op: protocol.OpGet, Key: "k"}); resp[0] != protocol.StatusNotFound {
		t.Fatalf("get after expire with ttl 0: status %d", resp[0])
	}
}

func TestMalformedRequests(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	good := dial(t, addr)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	bad := map[string][]byte{
		"empty payload":         {},
		"unknown op":            {0xee, 1, 'k'},
		"missing key":           {protocol.OpGet},
		"field past end":        {protocol.OpSet, 1, 1, 1, 'k', 9, 'v'},
		"bad uvarint":           {protocol.OpGet, 0xff},
		"missing value":         {protocol.OpSet, 1, 1, 1, 'k'},
		"trailing bytes":        {protocol.OpGet, 1, 'k', 'x'},
		"ping with field":       {protocol.OpPing, 0},
		"set without session":   {protocol.OpSet, 1, 'k', 1, 'v'},
		"del without seq":       {protocol.OpDel, 1},
		"seq zero":              {protocol.OpDel, 1, 0, 1, 'k'},
		"cas without flag":      {protocol.OpCas, 1, 1, 1, 'k'},
		"cas bad flag":          {protocol.OpCas, 1, 1, 1, 'k', 2, 0, 0},
		"cas without value":     {protocol.OpCas, 1, 1, 1, 'k', 1, 1, 'v'},
		"cas trailing bytes":    {protocol.OpCas, 1, 1, 1, 'k', 0, 0, 0, 'x'},
		"incr without delta":    {protocol.OpIncr, 1, 1, 1, 'k'},
		"incr bad delta":        {protocol.OpIncr, 1, 1, 1, 'k', 0xff},
		"incr trailing bytes":   {protocol.OpIncr, 1, 1, 1, 'k', 2, 0},
		"incr without session":  {protocol.OpIncr, 1, 'k', 2},
		"set without ttl":       {protocol.OpSet, 1, 1, 1, 'k', 1, 'v'},
		"expire without ttl":    {protocol.OpExpire, 1, 1, 1, 'k'},
		"ttl trailing bytes":    {protocol.OpTTL, 1, 'k', 0},
		"addnode without addrs": {protocol.OpAddNode, 1, 'n'},
		"removenode trailing":   {protocol.OpRemoveNode, 1, 'n', 0},
		"nodes with a key":      {protocol.OpNodes, 1, 'n'},
	}
	for name, payload := range bad {
		if err := protocol.WriteFrame(conn, payload); err != nil {
			t.Fatalf("%s: write: %v", name, err)
		}
		resp, err := protocol.ReadFrame(r)
		if err != nil {
			t.Fatalf("%s: read: %v", name, err)
		}
		if resp[0] != protocol.StatusError || len(resp) == 1 {
			t.Fatalf("%s: got status %d body %q, want ERROR with a message", name, resp[0], resp[1:])
		}
		if err := good.Ping(t.Context()); err != nil {
			t.Fatalf("%s: other client: %v", name, err)
		}
	}
	if err := protocol.WriteFrame(conn, []byte{protocol.OpPing}); err != nil {
		t.Fatal(err)
	}
	if resp, err := protocol.ReadFrame(r); err != nil || resp[0] != protocol.StatusOK {
		t.Fatalf("ping after malformed requests: %v, %v", resp, err)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], protocol.MaxFrameSize+1)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("after oversize frame: got %v, want connection closed", err)
	}
	if err := good.Set(t.Context(), "still", []byte("alive")); err != nil {
		t.Fatalf("other client after oversize frame: %v", err)
	}
}

func TestShutdownClosesIdleConnections(t *testing.T) {
	addr, stop := serve(t, newMemStore())
	for range 5 {
		if err := dial(t, addr).Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return with idle connections open")
	}
}

type blockingStore struct {
	nopWrites
	entered, release chan struct{}
}

func (s *blockingStore) Get(string) ([]byte, bool, error) { return nil, false, nil }
func (s *blockingStore) Set(uint64, uint64, string, []byte, time.Duration) error {
	close(s.entered)
	<-s.release
	return nil
}

func TestShutdownWaitsForInFlightRequest(t *testing.T) {
	store := &blockingStore{entered: make(chan struct{}), release: make(chan struct{})}
	addr, stop := serve(t, store)
	c := dial(t, addr)
	setDone := make(chan error, 1)
	go func() { setDone <- c.Set(t.Context(), "k", []byte("v")) }()
	<-store.entered

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("serve returned while a set was still running in the store")
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not return after the set finished")
	}
	select {
	case err := <-setDone:
		if err != nil {
			t.Errorf("in-flight set during shutdown: %v, want OK", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client set hung after shutdown")
	}
}

type failingStore struct {
	nopWrites
	calls atomic.Int64
}

func (*failingStore) Get(string) ([]byte, bool, error) { return nil, false, nil }
func (s *failingStore) Set(uint64, uint64, string, []byte, time.Duration) error {
	s.calls.Add(1)
	return errors.New("disk on fire")
}

func TestStoreErrorReachesClient(t *testing.T) {
	store := &failingStore{}
	addr, _ := serve(t, store)
	c := dial(t, addr)
	err := c.Set(t.Context(), "k", []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "disk on fire") || errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("got %v, want the store's error as a definite failure", err)
	}
	if n := store.calls.Load(); n != 1 {
		t.Fatalf("store saw %d attempts, want 1: a definite error is not retried", n)
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("connection unusable after server error: %v", err)
	}
}

// redirectStore answers the first unknown requests with an unknown leader, the next stale with a redirect to addr, then serves from next if set, else redirects forever
type redirectStore struct {
	addr    string
	unknown int64
	stale   int64
	calls   atomic.Int64
	next    Store
}

func (r *redirectStore) redirect() error {
	n := r.calls.Add(1)
	if n <= r.unknown {
		return &protocol.RedirectError{}
	}
	if r.next != nil && n > r.unknown+r.stale {
		return nil
	}
	return &protocol.RedirectError{Addr: r.addr}
}

func (r *redirectStore) Get(key string) ([]byte, bool, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return nil, false, err
	}
	return r.next.Get(key)
}

func (r *redirectStore) Set(client, seq uint64, key string, value []byte, ttl time.Duration) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.Set(client, seq, key, value, ttl)
}

func (r *redirectStore) Expire(client, seq uint64, key string, ttl time.Duration) (bool, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return false, err
	}
	return r.next.Expire(client, seq, key, ttl)
}

func (r *redirectStore) TTL(key string) (time.Duration, bool, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return 0, false, err
	}
	return r.next.TTL(key)
}

func (r *redirectStore) Delete(client, seq uint64, key string) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.Delete(client, seq, key)
}

func (r *redirectStore) Cas(client, seq uint64, key string, expected []byte, expectFound bool, value []byte) (bool, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return false, err
	}
	return r.next.Cas(client, seq, key, expected, expectFound, value)
}

func (r *redirectStore) AddNode(id, raftAddr, clientAddr string) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.AddNode(id, raftAddr, clientAddr)
}

func (r *redirectStore) RemoveNode(id string) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.RemoveNode(id)
}

func (r *redirectStore) Nodes() ([]protocol.Node, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return nil, err
	}
	return r.next.Nodes()
}

func (r *redirectStore) Incr(client, seq uint64, key string, delta int64) (int64, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return 0, err
	}
	return r.next.Incr(client, seq, key, delta)
}

func TestClientFollowsRedirect(t *testing.T) {
	leader, _ := serve(t, newMemStore())
	follower := &redirectStore{addr: leader}
	addr, _ := serve(t, follower)
	c := dial(t, addr)

	if err := c.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatalf("set via follower: %v", err)
	}
	if v, ok := mustGet(t, c, "k"); !ok || string(v) != "v" {
		t.Fatalf("get after redirected set: got %q, %v", v, ok)
	}
	if err := c.Del(t.Context(), "k"); err != nil {
		t.Fatalf("del: %v", err)
	}
	if _, ok := mustGet(t, c, "k"); ok {
		t.Fatal("key still present after redirected del")
	}
	if n := follower.calls.Load(); n != 1 {
		t.Fatalf("follower served %d requests, want 1: the client should stay on the leader", n)
	}
}

func TestRedirectLoopIsBounded(t *testing.T) {
	lnA, lnB := listen(t), listen(t)
	a, b := &redirectStore{addr: lnB.Addr().String()}, &redirectStore{addr: lnA.Addr().String()}
	serveOn(t, lnA, a)
	serveOn(t, lnB, b)
	c := dial(t, lnA.Addr().String())

	var re *protocol.RedirectError
	if err := c.Set(t.Context(), "k", []byte("v")); !errors.As(err, &re) {
		t.Fatalf("set in a redirect loop: got %v, want a *RedirectError", err)
	}
	if n := a.calls.Load() + b.calls.Load(); n != 11 {
		t.Fatalf("stores saw %d attempts, want 11: the first plus 10 redirects", n)
	}
}

func TestUnknownLeaderIsRetriedThroughAnElection(t *testing.T) {
	store := &redirectStore{unknown: 15, next: newMemStore()}
	addr, _ := serve(t, store)
	c := dial(t, addr)

	if err := c.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatalf("set while the leader is unknown for 1.5s: %v", err)
	}
	if n := store.calls.Load(); n != 16 {
		t.Fatalf("store saw %d attempts, want 16", n)
	}
}

func TestRedirectToDeadLeaderIsRetried(t *testing.T) {
	dead := listen(t)
	deadAddr := dead.Addr().String()
	if err := dead.Close(); err != nil {
		t.Fatal(err)
	}
	store := &redirectStore{addr: deadAddr, stale: 3, next: newMemStore()}
	addr, _ := serve(t, store)
	c := dial(t, addr)

	if err := c.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatalf("set while followers still point at a dead leader: %v", err)
	}
	if n := store.calls.Load(); n != 4 {
		t.Fatalf("store saw %d attempts, want 4", n)
	}
}

func TestNeverElectedLeaderGivesUp(t *testing.T) {
	addr, _ := serve(t, &redirectStore{unknown: math.MaxInt64})
	c := dial(t, addr)

	var re *protocol.RedirectError
	if err := c.Set(within(t, 500*time.Millisecond), "k", []byte("v")); !errors.As(err, &re) || re.Addr != "" {
		t.Fatalf("set with no leader ever elected: got %v, want a *RedirectError with no address", err)
	}
}

func BenchmarkGet(b *testing.B) {
	addr, _ := serve(b, newMemStore())
	c := dial(b, addr)
	if err := c.Set(b.Context(), "k", bytes.Repeat([]byte("v"), 128)); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, _, err := c.Get(b.Context(), "k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	addr, _ := serve(b, newMemStore())
	c := dial(b, addr)
	v := bytes.Repeat([]byte("v"), 128)
	for b.Loop() {
		if err := c.Set(b.Context(), "k", v); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCasAndIncr(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	c := dial(t, addr)

	for _, step := range []struct {
		expected    string
		expectFound bool
		value       string
		want        bool
	}{
		{"", true, "a", false},
		{"", false, "a", true},
		{"", false, "b", false},
		{"x", true, "b", false},
		{"a", true, "", true},
		{"", false, "c", false},
		{"", true, "c", true},
	} {
		swapped, err := c.CAS(t.Context(), "k", []byte(step.expected), step.expectFound, []byte(step.value))
		if err != nil || swapped != step.want {
			t.Fatalf("cas %+v: got %v, %v; want %v", step, swapped, err, step.want)
		}
	}
	if v, ok := mustGet(t, c, "k"); !ok || string(v) != "c" {
		t.Fatalf("get after cas: got %q, %v", v, ok)
	}

	for _, step := range []struct{ delta, want int64 }{{1, 1}, {41, 42}, {-50, -8}} {
		if n, err := c.Incr(t.Context(), "n", step.delta); err != nil || n != step.want {
			t.Fatalf("incr %d: got %d, %v; want %d", step.delta, n, err, step.want)
		}
	}
	if _, err := c.Incr(t.Context(), "k", 1); err == nil || errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("incr of a non-integer: got %v, want a definite error", err)
	}
}

// flakyServer serves store, answering each request whose number fail picks with no reply (closing the
// connection) or statusUnknown, after running it; it records every request
type flakyServer struct {
	srv   *Server
	fail  func(n int) (cut, unknown bool)
	mu    sync.Mutex
	reqs  []protocol.Request
	conns sync.WaitGroup
}

func serveFlaky(t *testing.T, store Store, fail func(n int) (cut, unknown bool)) (string, *flakyServer) {
	t.Helper()
	ln := listen(t)
	f := &flakyServer{srv: NewServer(store), fail: fail}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns.Go(func() { f.handle(conn) })
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		f.conns.Wait()
	})
	return ln.Addr().String(), f
}

func (f *flakyServer) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		p, err := protocol.ReadFrame(r)
		if err != nil {
			return
		}
		req, err := protocol.ParseRequest(p)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.reqs = append(f.reqs, req)
		n := len(f.reqs)
		f.mu.Unlock()
		resp := f.srv.exec(p)
		cut, unknown := f.fail(n)
		if cut {
			return
		}
		if unknown {
			resp = append([]byte{protocol.StatusUnknown}, "leadership lost"...)
		}
		if err := protocol.WriteFrame(conn, resp); err != nil {
			return
		}
	}
}

func (f *flakyServer) requests() []protocol.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reqs)
}

func TestWriteRetriedWithTheSameSeq(t *testing.T) {
	for name, fail := range map[string]func(int) (bool, bool){
		"unknown":        func(n int) (bool, bool) { return false, n == 1 },
		"connection cut": func(n int) (bool, bool) { return n == 1, false },
	} {
		t.Run(name, func(t *testing.T) {
			addr, f := serveFlaky(t, newMemStore(), fail)
			c := dial(t, addr)
			if n, err := c.Incr(t.Context(), "n", 1); err != nil || n != 1 {
				t.Fatalf("incr whose first attempt applied but failed: got %d, %v; want 1", n, err)
			}
			if n, err := c.Incr(t.Context(), "n", 1); err != nil || n != 2 {
				t.Fatalf("next incr: got %d, %v; want 2", n, err)
			}
			reqs := f.requests()
			if len(reqs) != 3 {
				t.Fatalf("server saw %d requests, want 3", len(reqs))
			}
			if reqs[0].Client != reqs[1].Client || reqs[0].Seq != reqs[1].Seq {
				t.Fatalf("retry sent session (%d, %d), first attempt (%d, %d)", reqs[1].Client, reqs[1].Seq, reqs[0].Client, reqs[0].Seq)
			}
			if reqs[2].Client != reqs[0].Client || reqs[2].Seq != reqs[0].Seq+1 {
				t.Fatalf("next write sent session (%d, %d) after (%d, %d)", reqs[2].Client, reqs[2].Seq, reqs[0].Client, reqs[0].Seq)
			}
		})
	}
}

func TestWriteGivesUpWithOutcomeUnknown(t *testing.T) {
	addr, f := serveFlaky(t, newMemStore(), func(int) (bool, bool) { return false, true })
	c := dial(t, addr)
	const limit = 500 * time.Millisecond
	start := time.Now()
	err := c.Set(within(t, limit), "k", []byte("v"))
	if !errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("write never answered definitely: got %v, want ErrOutcomeUnknown", err)
	}
	if d := time.Since(start); d < limit-200*time.Millisecond || d > limit+time.Second {
		t.Fatalf("gave up after %v, want about %v", d, limit)
	}
	reqs := f.requests()
	if len(reqs) < 2 {
		t.Fatalf("server saw %d attempts, want retries", len(reqs))
	}
	for _, r := range reqs {
		if r.Seq != reqs[0].Seq {
			t.Fatalf("retries sent seqs %d and %d", reqs[0].Seq, r.Seq)
		}
	}

	if err := c.Set(within(t, limit), "k", []byte("w")); !errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("second write: got %v", err)
	}
	// nothing applied yet, so a new session rather than seq 2
	if last := f.requests()[len(f.requests())-1]; last.Client == reqs[0].Client || last.Seq != 1 {
		t.Fatalf("write after giving up sent session (%d, %d) after (%d, %d), want a new client at seq 1", last.Client, last.Seq, reqs[0].Client, reqs[0].Seq)
	}
}

func TestReadGivesUpWithTheLastError(t *testing.T) {
	addr, _ := serve(t, &redirectStore{unknown: math.MaxInt64})
	c := dial(t, addr)
	var re *protocol.RedirectError
	if _, _, err := c.Get(within(t, 300*time.Millisecond), "k"); !errors.As(err, &re) || errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("get with no leader: got %v, want the redirect, not an unknown outcome", err)
	}
}

func TestClientRedialsTheSeeds(t *testing.T) {
	first, stopFirst := serve(t, newMemStore())
	store := newMemStore()
	second, _ := serve(t, store)
	c := dial(t, first, second)
	if err := c.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}

	stopFirst()
	if err := c.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatalf("set after the connected server stopped: %v", err)
	}
	if v, ok := store.s.Get("k", now()); !ok || string(v) != "v" {
		t.Fatalf("second seed has k = %q, %v; want the retried write", v, ok)
	}
}

func TestOutcomeUnknownSentAsStatusUnknown(t *testing.T) {
	if resp := errorResponse(fmt.Errorf("propose: %w", protocol.ErrOutcomeUnknown)); resp[0] != protocol.StatusUnknown {
		t.Fatalf("an unknown outcome was answered with status %d, want protocol.StatusUnknown", resp[0])
	}
	if resp := errorResponse(errors.New("value is not an integer")); resp[0] != protocol.StatusError {
		t.Fatalf("a failed write was answered with status %d, want ERROR", resp[0])
	}
}

// unknownStore answers writes with an unknown outcome without applying them while fail is positive
type unknownStore struct {
	*memStore
	fail atomic.Bool
}

func (s *unknownStore) Set(client, seq uint64, key string, value []byte, ttl time.Duration) error {
	if s.fail.Load() {
		return fmt.Errorf("propose: %w", protocol.ErrOutcomeUnknown)
	}
	return s.memStore.Set(client, seq, key, value, ttl)
}

func TestWriteAfterAnAbandonedFirstWriteSucceeds(t *testing.T) {
	store := &unknownStore{memStore: newMemStore()}
	store.fail.Store(true)
	addr, _ := serve(t, store)
	c := dial(t, addr)
	if err := c.Set(within(t, 200*time.Millisecond), "k", []byte("lost")); !errors.Is(err, protocol.ErrOutcomeUnknown) {
		t.Fatalf("first write: got %v, want ErrOutcomeUnknown", err)
	}
	store.fail.Store(false)
	if err := c.Set(t.Context(), "k", []byte("v")); err != nil {
		t.Fatalf("write after the first was abandoned without applying: %v", err)
	}
	if err := c.Set(t.Context(), "k", []byte("w")); err != nil {
		t.Fatalf("third write: %v", err)
	}
}

func TestClientIsSafeForConcurrentUse(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	c := dial(t, addr)
	const goroutines, incrs = 16, 50
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range incrs {
				if _, err := c.Incr(t.Context(), "n", 1); err != nil {
					t.Errorf("incr: %v", err)
					return
				}
				if _, _, err := c.Get(t.Context(), "n"); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
	if v, _ := mustGet(t, c, "n"); string(v) != strconv.Itoa(goroutines*incrs) {
		t.Fatalf("n = %s after %d concurrent incrs", v, goroutines*incrs)
	}
}

// countingListener counts the connections it accepts
type countingListener struct {
	net.Listener
	n atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.n.Add(1)
	}
	return conn, err
}

func TestSequentialCallsReuseOneConnection(t *testing.T) {
	ln := &countingListener{Listener: listen(t)}
	serveOn(t, ln, newMemStore())
	c := dial(t, ln.Addr().String())
	for i := range 20 {
		if err := c.Set(t.Context(), "k", []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
		mustGet(t, c, "k")
	}
	if n := ln.n.Load(); n != 1 {
		t.Fatalf("server accepted %d connections for 40 sequential calls, want 1", n)
	}
}

func TestNodeOps(t *testing.T) {
	store := newMemStore()
	addr, _ := serve(t, store)
	c := dial(t, addr)
	ctx := t.Context()

	if err := c.AddNode(ctx, "n4", "h4:7000", "h4:8080"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.AddNode(ctx, "n5", "h5:7000", "h5:8080"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.RemoveNode(ctx, "n4"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	got, err := c.Nodes(ctx)
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	if want := []client.Node{{ID: "n5", RaftAddr: "h5:7000", ClientAddr: "h5:8080"}}; !slices.Equal(got, want) {
		t.Fatalf("nodes %v, want %v", got, want)
	}

	err = c.RemoveNode(ctx, "nobody")
	if err == nil || !strings.Contains(err.Error(), "not a member") || errors.Is(err, client.ErrOutcomeUnknown) {
		t.Fatalf("remove a stranger: got %v, want the store's error as a definite failure", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("connection unusable after a refused change: %v", err)
	}
}

func TestNodeOpsFollowRedirects(t *testing.T) {
	store := newMemStore()
	leader, _ := serve(t, store)
	follower, _ := serve(t, &redirectStore{addr: leader})
	c := dial(t, follower)
	if err := c.AddNode(t.Context(), "n4", "h4:7000", "h4:8080"); err != nil {
		t.Fatalf("add through a follower: %v", err)
	}
	if err := c.RemoveNode(t.Context(), "n4"); err != nil {
		t.Fatalf("remove through a follower: %v", err)
	}
	if nodes, err := c.Nodes(t.Context()); err != nil || len(nodes) != 0 {
		t.Fatalf("nodes after add and remove: %v, %v", nodes, err)
	}
}
