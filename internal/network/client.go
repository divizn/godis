package network

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

// Client is one connection, not safe for concurrent use; any error not sent by the server means it is broken.
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

func (c *Client) do(op byte, key string, value []byte) ([]byte, bool, error) {
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
	default:
		return nil, false, fmt.Errorf("unknown response status %d", status)
	}
}
