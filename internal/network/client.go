package network

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	maxAttempts       = 10
	unknownLeaderWait = 100 * time.Millisecond
)

// Client is one connection, not safe for concurrent use; any error not sent by the server means it is broken.
// It follows redirects to the leader and stays connected there, waiting out a leader election, up to 10 attempts per request.
type Client struct {
	conn net.Conn
	r    *bufio.Reader
}

func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to godis: %w", err)
	}
	return &Client{conn: conn, r: bufio.NewReader(conn)}, nil
}

func (c *Client) Ping() error {
	_, _, err := c.do(opPing, "", nil)
	return err
}

// Get reports ok false if key does not exist.
func (c *Client) Get(key string) (value []byte, ok bool, err error) {
	return c.do(opGet, key, nil)
}

func (c *Client) Set(key string, value []byte) error {
	_, _, err := c.do(opSet, key, value)
	return err
}

func (c *Client) Del(key string) error {
	_, _, err := c.do(opDel, key, nil)
	return err
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// do retries on a redirect, which is safe because a node that redirects has not run the request
func (c *Client) do(op byte, key string, value []byte) ([]byte, bool, error) {
	var re *RedirectError
	for range maxAttempts {
		body, ok, err := c.roundTrip(op, key, value)
		if !errors.As(err, &re) {
			return body, ok, err
		}
		if re.Addr == "" {
			time.Sleep(unknownLeaderWait)
			continue
		}
		if err := c.redial(re.Addr); err != nil {
			return nil, false, err
		}
	}
	return nil, false, fmt.Errorf("gave up after %d attempts: %w", maxAttempts, re)
}

func (c *Client) redial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("follow redirect to %s: %w", addr, err)
	}
	c.conn.Close()
	c.conn, c.r = conn, bufio.NewReader(conn)
	return nil
}

func (c *Client) roundTrip(op byte, key string, value []byte) ([]byte, bool, error) {
	if err := writeFrame(c.conn, appendRequest(nil, op, key, value)); err != nil {
		return nil, false, err
	}
	resp, err := readFrame(c.r)
	if err != nil {
		return nil, false, fmt.Errorf("read response: %w", err)
	}
	if len(resp) == 0 {
		return nil, false, errors.New("empty response")
	}
	switch status, body := resp[0], resp[1:]; status {
	case statusOK:
		return body, true, nil
	case statusNotFound:
		return nil, false, nil
	case statusError:
		return nil, false, errors.New(string(body))
	case statusRedirect:
		return nil, false, &RedirectError{Addr: string(body)}
	default:
		return nil, false, fmt.Errorf("unknown response status %d", status)
	}
}
