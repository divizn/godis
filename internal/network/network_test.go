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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/internal/storage"
)

type memStore struct{ s *storage.Store }

func newMemStore() memStore { return memStore{storage.New()} }

func (m memStore) Get(key string) ([]byte, bool, error) {
	v, ok := m.s.Get(key)
	return v, ok, nil
}

func (m memStore) Set(key string, value []byte) error {
	m.s.Set(key, value)
	return nil
}

func (m memStore) Delete(key string) error {
	m.s.Delete(key)
	return nil
}

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

func dial(t testing.TB, addr string) *Client {
	t.Helper()
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustGet(t *testing.T, c *Client, key string) ([]byte, bool) {
	t.Helper()
	v, ok, err := c.Get(key)
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
		if err := c.Set(k, v); err != nil {
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

func TestOversizeFrameRejectedWithoutAllocating(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFFF)
	r := io.MultiReader(bytes.NewReader(hdr[:]), bytes.NewReader(make([]byte, 64)))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := readFrame(r)
	runtime.ReadMemStats(&after)

	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("got %v, want errFrameTooLarge", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("allocated %d bytes rejecting an oversize frame", n)
	}
}

func TestTruncatedFrame(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrameSize)
	r := io.MultiReader(bytes.NewReader(hdr[:]), strings.NewReader("only a few bytes"))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := readFrame(r)
	runtime.ReadMemStats(&after)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("allocated %d bytes for a frame that sent 16", n)
	}

	if _, err := readFrame(bytes.NewReader(hdr[:2])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header: got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteFrameRefusesOversize(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFrame(&buf, make([]byte, maxFrameSize+1)); err == nil {
		t.Fatal("want error writing an oversize frame")
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes of an oversize frame", buf.Len())
	}
}

func TestEndToEnd(t *testing.T) {
	addr, _ := serve(t, newMemStore())
	c := dial(t, addr)

	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"a", "3"}} {
		if err := c.Set(kv[0], []byte(kv[1])); err != nil {
			t.Fatalf("set %s: %v", kv[0], err)
		}
	}
	if err := c.Del("b"); err != nil {
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
		set := appendRequest(nil, opSet, "k", fmt.Appendf(nil, "%d", i))
		if err := writeFrame(&out, set); err != nil {
			t.Fatal(err)
		}
		if err := writeFrame(&out, appendRequest(nil, opGet, "k", nil)); err != nil {
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
			resp, err := readFrame(r)
			if err != nil {
				t.Fatalf("request %d: %v", i, err)
			}
			if resp[0] != statusOK || string(resp[1:]) != want {
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
			c, err := Dial(addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			for j := range ops {
				key := fmt.Sprintf("c%d-%d", i, j)
				want := strings.Repeat(key, j+1)
				if err := c.Set(key, []byte(want)); err != nil {
					errs <- err
					return
				}
				got, ok, err := c.Get(key)
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
		"empty payload":   {},
		"unknown op":      {0xee, 1, 'k'},
		"missing key":     {opGet},
		"field past end":  {opSet, 1, 'k', 9, 'v'},
		"bad uvarint":     {opGet, 0xff},
		"missing value":   {opSet, 1, 'k'},
		"trailing bytes":  {opGet, 1, 'k', 'x'},
		"ping with field": {opPing, 0},
	}
	for name, payload := range bad {
		if err := writeFrame(conn, payload); err != nil {
			t.Fatalf("%s: write: %v", name, err)
		}
		resp, err := readFrame(r)
		if err != nil {
			t.Fatalf("%s: read: %v", name, err)
		}
		if resp[0] != statusError || len(resp) == 1 {
			t.Fatalf("%s: got status %d body %q, want ERROR with a message", name, resp[0], resp[1:])
		}
		if err := good.Ping(); err != nil {
			t.Fatalf("%s: other client: %v", name, err)
		}
	}
	if err := writeFrame(conn, []byte{opPing}); err != nil {
		t.Fatal(err)
	}
	if resp, err := readFrame(r); err != nil || resp[0] != statusOK {
		t.Fatalf("ping after malformed requests: %v, %v", resp, err)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], maxFrameSize+1)
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("after oversize frame: got %v, want connection closed", err)
	}
	if err := good.Set("still", []byte("alive")); err != nil {
		t.Fatalf("other client after oversize frame: %v", err)
	}
}

func TestShutdownClosesIdleConnections(t *testing.T) {
	addr, stop := serve(t, newMemStore())
	for range 5 {
		if err := dial(t, addr).Ping(); err != nil {
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
	entered, release chan struct{}
}

func (s *blockingStore) Get(string) ([]byte, bool, error) { return nil, false, nil }
func (s *blockingStore) Delete(string) error              { return nil }
func (s *blockingStore) Set(string, []byte) error {
	close(s.entered)
	<-s.release
	return nil
}

func TestShutdownWaitsForInFlightRequest(t *testing.T) {
	store := &blockingStore{entered: make(chan struct{}), release: make(chan struct{})}
	addr, stop := serve(t, store)
	c := dial(t, addr)
	setDone := make(chan error, 1)
	go func() { setDone <- c.Set("k", []byte("v")) }()
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

type failingStore struct{}

func (failingStore) Get(string) ([]byte, bool, error) { return nil, false, nil }
func (failingStore) Delete(string) error              { return nil }
func (failingStore) Set(string, []byte) error         { return errors.New("disk on fire") }

func TestStoreErrorReachesClient(t *testing.T) {
	addr, _ := serve(t, failingStore{})
	c := dial(t, addr)
	err := c.Set("k", []byte("v"))
	if err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Fatalf("got %v, want the store's error", err)
	}
	if err := c.Ping(); err != nil {
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
		return &RedirectError{}
	}
	if r.next != nil && n > r.unknown+r.stale {
		return nil
	}
	return &RedirectError{Addr: r.addr}
}

func (r *redirectStore) Get(key string) ([]byte, bool, error) {
	if err := r.redirect(); err != nil || r.next == nil {
		return nil, false, err
	}
	return r.next.Get(key)
}

func (r *redirectStore) Set(key string, value []byte) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.Set(key, value)
}

func (r *redirectStore) Delete(key string) error {
	if err := r.redirect(); err != nil || r.next == nil {
		return err
	}
	return r.next.Delete(key)
}

func TestClientFollowsRedirect(t *testing.T) {
	leader, _ := serve(t, newMemStore())
	follower := &redirectStore{addr: leader}
	addr, _ := serve(t, follower)
	c := dial(t, addr)

	if err := c.Set("k", []byte("v")); err != nil {
		t.Fatalf("set via follower: %v", err)
	}
	if v, ok := mustGet(t, c, "k"); !ok || string(v) != "v" {
		t.Fatalf("get after redirected set: got %q, %v", v, ok)
	}
	if err := c.Del("k"); err != nil {
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

	var re *RedirectError
	if err := c.Set("k", []byte("v")); !errors.As(err, &re) {
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

	if err := c.Set("k", []byte("v")); err != nil {
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

	if err := c.Set("k", []byte("v")); err != nil {
		t.Fatalf("set while followers still point at a dead leader: %v", err)
	}
	if n := store.calls.Load(); n != 4 {
		t.Fatalf("store saw %d attempts, want 4", n)
	}
}

func TestNeverElectedLeaderGivesUp(t *testing.T) {
	t.Parallel()
	addr, _ := serve(t, &redirectStore{unknown: math.MaxInt64})
	c := dial(t, addr)

	var re *RedirectError
	if err := c.Set("k", []byte("v")); !errors.As(err, &re) || re.Addr != "" {
		t.Fatalf("set with no leader ever elected: got %v, want a *RedirectError with no address", err)
	}
}

func BenchmarkGet(b *testing.B) {
	addr, _ := serve(b, newMemStore())
	c := dial(b, addr)
	if err := c.Set("k", bytes.Repeat([]byte("v"), 128)); err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, _, err := c.Get("k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	addr, _ := serve(b, newMemStore())
	c := dial(b, addr)
	v := bytes.Repeat([]byte("v"), 128)
	for b.Loop() {
		if err := c.Set("k", v); err != nil {
			b.Fatal(err)
		}
	}
}
