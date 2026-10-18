// Package network implements the godis TCP protocol, its server and its client.
package network

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// frame layout: uint32 payload length | payload, big endian
const maxFrameSize = 16 << 20

// request payload: op | session (writes only: uvarint client | uvarint seq) | uvarint-length-prefixed key | rest
// set rest: value field; cas rest: byte expectFound | expected field | value field; incr rest: varint delta
const (
	opPing byte = iota + 1
	opGet
	opSet
	opDel
	opCas
	opIncr
)

// response payload: status | body
// OK bodies: the value for get, 1 or 0 for whether a cas swapped, the new value in decimal for incr
const (
	statusOK byte = iota
	statusNotFound
	statusError
	statusRedirect
	// statusUnknown means the write may or may not apply: retry it with the same seq
	statusUnknown
)

var errFrameTooLarge = errors.New("frame too large")

// ErrOutcomeUnknown means a write may or may not have taken effect. A Store returns it, wrapped, for the server to
// answer statusUnknown; the client returns it once it gives up retrying.
var ErrOutcomeUnknown = errors.New("outcome unknown")

// RedirectError reports that a node is not the leader. Addr is the leader's client address, empty if unknown.
type RedirectError struct {
	Addr string
}

func (e *RedirectError) Error() string {
	if e.Addr == "" {
		return "not the leader, leader unknown"
	}
	return "not the leader, leader is at " + e.Addr
}

// readFrame grows its buffer as bytes arrive, never allocating a length the peer has not sent.
func readFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", errFrameTooLarge, n)
	}
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, r, int64(n)); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("read %d byte frame: %w", n, err)
	}
	return buf.Bytes(), nil
}

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("%w: %d bytes", errFrameTooLarge, len(payload))
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(payload)), uint32(len(payload)))
	if _, err := w.Write(append(frame, payload...)); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// request is one client request; writes carry the client's session, (client, seq)
type request struct {
	op          byte
	client, seq uint64
	key         string
	value       []byte
	expected    []byte
	expectFound bool
	delta       int64
}

func (r request) isWrite() bool {
	return r.op == opSet || r.op == opDel || r.op == opCas || r.op == opIncr
}

func (r request) append(b []byte) []byte {
	b = append(b, r.op)
	if r.op == opPing {
		return b
	}
	if r.isWrite() {
		b = binary.AppendUvarint(b, r.client)
		b = binary.AppendUvarint(b, r.seq)
	}
	b = appendField(b, []byte(r.key))
	switch r.op {
	case opSet:
		b = appendField(b, r.value)
	case opCas:
		flag := byte(0)
		if r.expectFound {
			flag = 1
		}
		b = appendField(append(b, flag), r.expected)
		b = appendField(b, r.value)
	case opIncr:
		b = binary.AppendVarint(b, r.delta)
	}
	return b
}

func appendField(b, f []byte) []byte {
	return append(binary.AppendUvarint(b, uint64(len(f))), f...)
}

func parseRequest(p []byte) (request, error) {
	var r request
	if len(p) == 0 {
		return r, errors.New("empty request")
	}
	r.op = p[0]
	rest := p[1:]
	if r.op < opPing || r.op > opIncr {
		return r, fmt.Errorf("unknown op %d", r.op)
	}
	if r.op == opPing {
		if len(rest) != 0 {
			return r, fmt.Errorf("%d trailing bytes", len(rest))
		}
		return r, nil
	}
	var err error
	if r.isWrite() {
		for _, f := range []struct {
			v    *uint64
			what string
		}{{&r.client, "client"}, {&r.seq, "seq"}} {
			n, w := binary.Uvarint(rest)
			if w <= 0 {
				return r, fmt.Errorf("%s: missing or malformed", f.what)
			}
			*f.v, rest = n, rest[w:]
		}
		if r.seq == 0 {
			return r, errors.New("seq: must start at 1")
		}
	}
	var k []byte
	if k, rest, err = cutField(rest); err != nil {
		return r, fmt.Errorf("key: %w", err)
	}
	r.key = string(k)
	switch r.op {
	case opSet:
		if r.value, rest, err = cutField(rest); err != nil {
			return r, fmt.Errorf("value: %w", err)
		}
	case opCas:
		if len(rest) == 0 || rest[0] > 1 {
			return r, errors.New("expect flag: missing or not 0 or 1")
		}
		r.expectFound = rest[0] == 1
		if r.expected, rest, err = cutField(rest[1:]); err != nil {
			return r, fmt.Errorf("expected value: %w", err)
		}
		if r.value, rest, err = cutField(rest); err != nil {
			return r, fmt.Errorf("value: %w", err)
		}
	case opIncr:
		d, w := binary.Varint(rest)
		if w <= 0 {
			return r, errors.New("delta: missing or malformed")
		}
		r.delta, rest = d, rest[w:]
	}
	if len(rest) != 0 {
		return r, fmt.Errorf("%d trailing bytes", len(rest))
	}
	return r, nil
}

func cutField(p []byte) (field, rest []byte, err error) {
	n, w := binary.Uvarint(p)
	if w <= 0 {
		return nil, nil, errors.New("missing or malformed length")
	}
	p = p[w:]
	if n > uint64(len(p)) {
		return nil, nil, fmt.Errorf("length %d runs past end of request", n)
	}
	return p[:n], p[n:], nil
}
