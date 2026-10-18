// Package protocol is the godis client protocol: framing, requests, and response statuses.
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// frame layout: uint32 payload length | payload, big endian
const MaxFrameSize = 16 << 20

// Request payload: op | session (writes only: uvarint client | uvarint seq) | uvarint-length-prefixed key | rest
// set rest: value field | uvarint ttl ms, 0 for none; cas rest: byte expectFound | expected field | value field
// incr rest: varint delta; expire rest: varint ttl ms; ttl has no rest
const (
	OpPing byte = iota + 1
	OpGet
	OpSet
	OpDel
	OpCas
	OpIncr
	OpExpire
	OpTTL
)

// MaxTTL is the longest ttl a request may carry, in ms: about 100 years
const MaxTTL = 100 * 365 * 24 * 3600 * 1000

// response payload: status | body
// OK bodies: the value for get, 1 or 0 for whether a cas swapped or an expire found a live key, the new value in decimal
// for incr, the uvarint ms left for ttl with 0 for no deadline (NotFound if the key is missing or expired)
const (
	StatusOK byte = iota
	StatusNotFound
	StatusError
	StatusRedirect
	// StatusUnknown means the write may or may not apply: retry it with the same seq
	StatusUnknown
)

var ErrFrameTooLarge = errors.New("frame too large")

// ErrOutcomeUnknown means a write may or may not have taken effect. A Store returns it, wrapped, for the server to
// answer StatusUnknown; the client returns it once it gives up retrying.
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

// ReadFrame reads one frame, growing its buffer as bytes arrive, never allocating a length the peer has not sent.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
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

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > MaxFrameSize {
		return fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(payload))
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(payload)), uint32(len(payload)))
	if _, err := w.Write(append(frame, payload...)); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// Request is one client Request; writes carry the client's session, (client, seq)
type Request struct {
	Op          byte
	Client, Seq uint64
	Key         string
	Value       []byte
	Expected    []byte
	ExpectFound bool
	Delta       int64
	// TTL is in ms: for a set 0 means none, for an expire a value at or below 0 deletes the key
	TTL int64
}

func (r Request) IsWrite() bool {
	return r.Op == OpSet || r.Op == OpDel || r.Op == OpCas || r.Op == OpIncr || r.Op == OpExpire
}

func (r Request) Append(b []byte) []byte {
	b = append(b, r.Op)
	if r.Op == OpPing {
		return b
	}
	if r.IsWrite() {
		b = binary.AppendUvarint(b, r.Client)
		b = binary.AppendUvarint(b, r.Seq)
	}
	b = appendField(b, []byte(r.Key))
	switch r.Op {
	case OpSet:
		b = binary.AppendUvarint(appendField(b, r.Value), uint64(r.TTL))
	case OpExpire:
		b = binary.AppendVarint(b, r.TTL)
	case OpCas:
		flag := byte(0)
		if r.ExpectFound {
			flag = 1
		}
		b = appendField(append(b, flag), r.Expected)
		b = appendField(b, r.Value)
	case OpIncr:
		b = binary.AppendVarint(b, r.Delta)
	}
	return b
}

func appendField(b, f []byte) []byte {
	return append(binary.AppendUvarint(b, uint64(len(f))), f...)
}

func ParseRequest(p []byte) (Request, error) {
	var r Request
	if len(p) == 0 {
		return r, errors.New("empty Request")
	}
	r.Op = p[0]
	rest := p[1:]
	if r.Op < OpPing || r.Op > OpTTL {
		return r, fmt.Errorf("unknown op %d", r.Op)
	}
	if r.Op == OpPing {
		if len(rest) != 0 {
			return r, fmt.Errorf("%d trailing bytes", len(rest))
		}
		return r, nil
	}
	var err error
	if r.IsWrite() {
		for _, f := range []struct {
			v    *uint64
			what string
		}{{&r.Client, "client"}, {&r.Seq, "seq"}} {
			n, w := binary.Uvarint(rest)
			if w <= 0 {
				return r, fmt.Errorf("%s: missing or malformed", f.what)
			}
			*f.v, rest = n, rest[w:]
		}
		if r.Seq == 0 {
			return r, errors.New("seq: must start at 1")
		}
	}
	var k []byte
	if k, rest, err = cutField(rest); err != nil {
		return r, fmt.Errorf("key: %w", err)
	}
	r.Key = string(k)
	switch r.Op {
	case OpSet:
		if r.Value, rest, err = cutField(rest); err != nil {
			return r, fmt.Errorf("value: %w", err)
		}
		ttl, w := binary.Uvarint(rest)
		if w <= 0 || ttl > MaxTTL {
			return r, fmt.Errorf("ttl: missing, malformed or over %d", MaxTTL)
		}
		r.TTL, rest = int64(ttl), rest[w:]
	case OpExpire:
		ttl, w := binary.Varint(rest)
		if w <= 0 || ttl > MaxTTL || ttl < -MaxTTL {
			return r, fmt.Errorf("ttl: missing, malformed or beyond %d", MaxTTL)
		}
		r.TTL, rest = ttl, rest[w:]
	case OpCas:
		if len(rest) == 0 || rest[0] > 1 {
			return r, errors.New("expect flag: missing or not 0 or 1")
		}
		r.ExpectFound = rest[0] == 1
		if r.Expected, rest, err = cutField(rest[1:]); err != nil {
			return r, fmt.Errorf("expected value: %w", err)
		}
		if r.Value, rest, err = cutField(rest); err != nil {
			return r, fmt.Errorf("value: %w", err)
		}
	case OpIncr:
		d, w := binary.Varint(rest)
		if w <= 0 {
			return r, errors.New("delta: missing or malformed")
		}
		r.Delta, rest = d, rest[w:]
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
		return nil, nil, fmt.Errorf("length %d runs past end of Request", n)
	}
	return p[:n], p[n:], nil
}
