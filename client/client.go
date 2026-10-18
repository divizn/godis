// Package client is the Go client for a godis cluster.
package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/divizn/godis/internal/protocol"
)

const (
	maxRedirects = 10
	// outlasts an election at the server's default timeout, randomized up to 2s, and a server's 5s request timeout
	retryTimeout = 10 * time.Second
	retryWait    = 100 * time.Millisecond
	dialTimeout  = time.Second
)

// ErrOutcomeUnknown means a write may or may not have taken effect.
var ErrOutcomeUnknown = protocol.ErrOutcomeUnknown

// RedirectError reports that a node is not the leader. Addr is the leader's client address, empty if unknown.
type RedirectError = protocol.RedirectError

// aLongTimeAgo is a deadline in the past, set to interrupt a blocked read or write
var aLongTimeAgo = time.Unix(1, 0)

// Client is a session with a godis cluster, not safe for concurrent use. Writes carry the session's client id and
// a sequence number, so a write retried after an unknown outcome takes effect at most once.
//
// A request is retried until ctx is done or 10s pass, whichever is first: after a redirect (up to 10 per request, staying on the leader), when the
// server reports an unknown outcome, and after a connection failure, redialing the last leader and then the seed
// addresses. Definite errors are returned at once: an error sent by the server (a malformed request, Incr of a
// value that is not an integer, a session that expired), which means the write did not apply. A write that gives
// up returns an error wrapping ErrOutcomeUnknown: it may or may not have taken effect, and is never applied later
// on its own. A read that gives up returns the last error. Either wraps ctx.Err() if ctx stopped it.
type Client struct {
	seeds  []string
	leader string
	conn   net.Conn
	r      *bufio.Reader
	id     uint64
	seq    uint64
	// confirmed is whether a write under id has applied, so the cluster holds a session for it
	confirmed bool
}

// Dial connects to the first reachable address; the client redials any of them later.
func Dial(addrs ...string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("connect to godis: no address")
	}
	c := &Client{seeds: addrs}
	if err := c.newSession(); err != nil {
		return nil, err
	}
	if err := c.connectAny(context.Background()); err != nil {
		return nil, fmt.Errorf("connect to godis: %w", err)
	}
	return c, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, _, err := c.do(ctx, protocol.Request{Op: protocol.OpPing})
	return err
}

// Get reports ok false if key does not exist.
func (c *Client) Get(ctx context.Context, key string) (value []byte, ok bool, err error) {
	status, body, err := c.do(ctx, protocol.Request{Op: protocol.OpGet, Key: key})
	return body, status == protocol.StatusOK, err
}

func (c *Client) Set(ctx context.Context, key string, value []byte) error {
	_, _, err := c.write(ctx, protocol.Request{Op: protocol.OpSet, Key: key, Value: value})
	return err
}

func (c *Client) Del(ctx context.Context, key string) error {
	_, _, err := c.write(ctx, protocol.Request{Op: protocol.OpDel, Key: key})
	return err
}

// CAS sets key to value if key is absent and expectFound is false, or holds exactly expected and expectFound is true.
func (c *Client) CAS(ctx context.Context, key string, expected []byte, expectFound bool, value []byte) (swapped bool, err error) {
	_, body, err := c.write(ctx, protocol.Request{Op: protocol.OpCas, Key: key, Expected: expected, ExpectFound: expectFound, Value: value})
	if err != nil {
		return false, err
	}
	if len(body) != 1 || body[0] > 1 {
		return false, fmt.Errorf("malformed cas response %q", body)
	}
	return body[0] == 1, nil
}

// Incr adds delta to the base-10 int64 at key, a missing key counting as 0, and returns the new value.
func (c *Client) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	_, body, err := c.write(ctx, protocol.Request{Op: protocol.OpIncr, Key: key, Delta: delta})
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(string(body), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed incr response: %w", err)
	}
	return n, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// newSession picks a random client id and starts its seqs at 1
func (c *Client) newSession() error {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("pick client id: %w", err)
	}
	c.id, c.seq, c.confirmed = binary.LittleEndian.Uint64(id[:]), 0, false
	return nil
}

// write sends req with the next seq, the same on every retry; until one applies, a write that fails starts a new session, since the cluster would refuse seq 2 from a client it has no session for
func (c *Client) write(ctx context.Context, req protocol.Request) (byte, []byte, error) {
	c.seq++
	req.Client, req.Seq = c.id, c.seq
	status, body, err := c.do(ctx, req)
	if err == nil {
		c.confirmed = true
	} else if !c.confirmed {
		if serr := c.newSession(); serr != nil {
			err = errors.Join(err, serr)
		}
	}
	var se *serverError
	if err != nil && !errors.As(err, &se) {
		return 0, nil, fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}
	return status, body, err
}

// serverError is a definite error sent by the server
type serverError struct{ msg string }

func (e *serverError) Error() string { return e.msg }

// do sends req until it gets a definite answer, retrying redirects, unknown outcomes and connection failures until ctx is done or retryTimeout passes
func (c *Client) do(ctx context.Context, req protocol.Request) (byte, []byte, error) {
	p := req.Append(nil)
	start := time.Now()
	deadline := start.Add(retryTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	for redirects := 0; ; {
		status, body, err := c.roundTrip(ctx, p, deadline)
		switch {
		case err != nil:
		case status == protocol.StatusOK || status == protocol.StatusNotFound:
			return status, body, nil
		case status == protocol.StatusError:
			return 0, nil, &serverError{string(body)}
		case status == protocol.StatusUnknown:
			err = fmt.Errorf("server: %s", body)
		case status == protocol.StatusRedirect:
			re := &RedirectError{Addr: string(body)}
			err = re
			if re.Addr != "" {
				if redirects == maxRedirects {
					return 0, nil, fmt.Errorf("gave up after %d redirects: %w", maxRedirects, re)
				}
				dialErr := c.connect(ctx, re.Addr)
				if dialErr == nil {
					redirects++
					continue
				}
				err = errors.Join(re, dialErr)
			}
		default:
			err = fmt.Errorf("unknown response status %d", status)
		}
		if ctx.Err() != nil {
			return 0, nil, fmt.Errorf("%w: %w", ctx.Err(), err)
		}
		// the leader is unknown, a dead one is still named until followers time out, or the cluster is unreachable: ask again shortly
		if time.Now().Add(retryWait).After(deadline) {
			return 0, nil, fmt.Errorf("gave up after %v: %w", time.Since(start).Round(time.Millisecond), err)
		}
		t := time.NewTimer(retryWait)
		select {
		case <-ctx.Done():
			t.Stop()
			return 0, nil, fmt.Errorf("%w: %w", ctx.Err(), err)
		case <-t.C:
		}
	}
}

// roundTrip sends one request, (re)connecting first if needed; a connection error, or ctx stopping the exchange, closes the connection
func (c *Client) roundTrip(ctx context.Context, p []byte, deadline time.Time) (byte, []byte, error) {
	if c.conn == nil {
		if err := c.connectAny(ctx); err != nil {
			return 0, nil, err
		}
	}
	conn := c.conn
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(aLongTimeAgo) })
	resp, err := c.exchange(p, deadline)
	// once the AfterFunc has run, its past deadline would fail the next request on conn
	if !stop() || err != nil {
		conn.Close()
		c.conn = nil
	}
	if err != nil {
		return 0, nil, err
	}
	return resp[0], resp[1:], nil
}

func (c *Client) exchange(p []byte, deadline time.Time) ([]byte, error) {
	if err := c.conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}
	if err := protocol.WriteFrame(c.conn, p); err != nil {
		return nil, err
	}
	resp, err := protocol.ReadFrame(c.r)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(resp) == 0 {
		return nil, errors.New("empty response")
	}
	return resp, nil
}

// connectAny dials the last known leader, then each seed in order
func (c *Client) connectAny(ctx context.Context) error {
	var errs []error
	for _, addr := range append([]string{c.leader}, c.seeds...) {
		if addr == "" {
			continue
		}
		err := c.connect(ctx, addr)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// connect replaces the connection with one to addr, keeping the old one if the dial fails
func (c *Client) connect(ctx context.Context, addr string) error {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn, c.r, c.leader = conn, bufio.NewReader(conn), addr
	return nil
}
