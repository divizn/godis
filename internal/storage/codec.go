package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// command layout: op | uvarint client | uvarint seq | uvarint now | uvarint key length | key | rest
// now is the proposer's clock in Unix ms, a deadline is absolute in Unix ms with 0 for none
// set rest: uvarint deadline | value; delete rest: empty; incr rest: varint delta; expire rest: uvarint deadline
// cas rest: uvarint deadline | byte expectFound | uvarint expected length | expected | new
// sweep layout: op | uvarint now, with no client
const (
	opSet byte = iota + 1
	opDelete
	opCas
	opIncr
	opExpire
	opSweep
)

// result layout: status | body
const (
	resultOK byte = iota
	resultFailed
)

// EncodeSet sets key to value, replacing any deadline with deadline.
func EncodeSet(client, seq, now, deadline uint64, key string, value []byte) []byte {
	b := header(opSet, client, seq, now, key, binary.MaxVarintLen64+len(value))
	return append(binary.AppendUvarint(b, deadline), value...)
}

func EncodeDelete(client, seq, now uint64, key string) []byte {
	return header(opDelete, client, seq, now, key, 0)
}

// EncodeCas swaps in value and deadline if key is absent when expectFound is false, or holds exactly expected when it is true.
func EncodeCas(client, seq, now, deadline uint64, key string, expected []byte, expectFound bool, value []byte) []byte {
	b := header(opCas, client, seq, now, key, 2*binary.MaxVarintLen64+1+len(expected)+len(value))
	b = binary.AppendUvarint(b, deadline)
	flag := byte(0)
	if expectFound {
		flag = 1
	}
	b = append(b, flag)
	b = binary.AppendUvarint(b, uint64(len(expected)))
	b = append(b, expected...)
	return append(b, value...)
}

// EncodeIncr adds delta to the base-10 int64 stored at key, a missing key counting as 0.
func EncodeIncr(client, seq, now uint64, key string, delta int64) []byte {
	return binary.AppendVarint(header(opIncr, client, seq, now, key, binary.MaxVarintLen64), delta)
}

// EncodeExpire sets the deadline of a live key, or deletes it if deadline is not after the command's time.
func EncodeExpire(client, seq, now uint64, key string, deadline uint64) []byte {
	return binary.AppendUvarint(header(opExpire, client, seq, now, key, binary.MaxVarintLen64), deadline)
}

// EncodeSweep deletes every key whose deadline is at or before now. It has no session, so it is never deduplicated.
func EncodeSweep(now uint64) []byte {
	return binary.AppendUvarint([]byte{opSweep}, now)
}

func header(op byte, client, seq, now uint64, key string, rest int) []byte {
	b := make([]byte, 0, 1+4*binary.MaxVarintLen64+len(key)+rest)
	b = append(b, op)
	b = binary.AppendUvarint(b, client)
	b = binary.AppendUvarint(b, seq)
	b = binary.AppendUvarint(b, now)
	b = binary.AppendUvarint(b, uint64(len(key)))
	return append(b, key...)
}

// ParseResult splits a result of Apply into its body, or the error of a command that failed.
// The body is empty for Set and Delete, 1 or 0 for whether a Cas swapped or an Expire found a live key, the new value
// for Incr, and the uvarint count of keys deleted for Sweep.
func ParseResult(r []byte) ([]byte, error) {
	if len(r) == 0 {
		return nil, errors.New("empty result")
	}
	switch r[0] {
	case resultOK:
		return r[1:], nil
	case resultFailed:
		return nil, errors.New(string(r[1:]))
	}
	return nil, fmt.Errorf("unknown result status %d", r[0])
}

func okResult(body ...byte) []byte { return append([]byte{resultOK}, body...) }

func failedResult(msg string) []byte { return append([]byte{resultFailed}, msg...) }

type command struct {
	op          byte
	client, seq uint64
	now         uint64
	deadline    uint64
	key         string
	value       []byte
	expected    []byte
	expectFound bool
	delta       int64
}

func decode(cmd []byte) (command, error) {
	var c command
	if len(cmd) == 0 {
		return c, errors.New("empty command")
	}
	c.op = cmd[0]
	rest := cmd[1:]
	uvarint := func(what string) (uint64, error) {
		v, n := binary.Uvarint(rest)
		if n <= 0 {
			return 0, fmt.Errorf("malformed %s", what)
		}
		rest = rest[n:]
		return v, nil
	}
	field := func(what string) ([]byte, error) {
		n, err := uvarint(what + " length")
		if err != nil {
			return nil, err
		}
		if n > uint64(len(rest)) {
			return nil, fmt.Errorf("%s length %d runs past end of command", what, n)
		}
		f := rest[:n]
		rest = rest[n:]
		return f, nil
	}
	var err error
	if c.op == opSweep {
		if c.now, err = uvarint("now"); err != nil {
			return c, err
		}
		if len(rest) != 0 {
			return c, fmt.Errorf("sweep carries %d stray bytes", len(rest))
		}
		return c, nil
	}
	if c.client, err = uvarint("client"); err != nil {
		return c, err
	}
	if c.seq, err = uvarint("seq"); err != nil {
		return c, err
	}
	if c.seq == 0 {
		return c, errors.New("seq 0")
	}
	if c.now, err = uvarint("now"); err != nil {
		return c, err
	}
	key, err := field("key")
	if err != nil {
		return c, err
	}
	c.key = string(key)

	switch c.op {
	case opSet:
		if c.deadline, err = uvarint("deadline"); err != nil {
			return c, err
		}
		c.value = rest
	case opDelete:
		if len(rest) != 0 {
			return c, fmt.Errorf("delete of %q carries %d stray bytes", c.key, len(rest))
		}
	case opCas:
		if c.deadline, err = uvarint("deadline"); err != nil {
			return c, err
		}
		if len(rest) == 0 || rest[0] > 1 {
			return c, errors.New("malformed cas expect flag")
		}
		c.expectFound, rest = rest[0] == 1, rest[1:]
		if c.expected, err = field("cas expected value"); err != nil {
			return c, err
		}
		c.value = rest
	case opIncr:
		d, n := binary.Varint(rest)
		if n <= 0 || n != len(rest) {
			return c, errors.New("malformed incr delta")
		}
		c.delta = d
	case opExpire:
		if c.deadline, err = uvarint("deadline"); err != nil {
			return c, err
		}
		if len(rest) != 0 {
			return c, fmt.Errorf("expire of %q carries %d stray bytes", c.key, len(rest))
		}
	default:
		return c, fmt.Errorf("unknown op %d", c.op)
	}
	return c, nil
}

// exec runs c against the data at time t and returns its result; mu must be held
func (s *Store) exec(c command, t uint64) []byte {
	cur, found := s.live(c.key, t)
	switch c.op {
	case opSet:
		s.put(c.key, c.value, c.deadline)
	case opDelete:
		s.remove(c.key)
	case opCas:
		if found != c.expectFound || found && !bytes.Equal(cur, c.expected) {
			return okResult(0)
		}
		s.put(c.key, c.value, c.deadline)
		return okResult(1)
	case opIncr:
		var n int64
		if found {
			var err error
			if n, err = strconv.ParseInt(string(cur), 10, 64); err != nil {
				return failedResult(fmt.Sprintf("value of %q is not an integer", c.key))
			}
		}
		if c.delta > 0 && n > math.MaxInt64-c.delta || c.delta < 0 && n < math.MinInt64-c.delta {
			return failedResult(fmt.Sprintf("incr of %q would overflow", c.key))
		}
		v := strconv.AppendInt(nil, n+c.delta, 10)
		var deadline uint64
		if found {
			deadline = s.deadlines[c.key]
		}
		s.put(c.key, v, deadline)
		return okResult(v...)
	case opExpire:
		switch {
		case !found:
			return okResult(0)
		case c.deadline <= t:
			s.remove(c.key)
		default:
			s.deadlines[c.key] = c.deadline
		}
		return okResult(1)
	}
	return okResult()
}

// sweep deletes every key due at t and returns how many; mu must be held
func (s *Store) sweep(t uint64) []byte {
	var n uint64
	for k, d := range s.deadlines {
		if d <= t {
			s.remove(k)
			n++
		}
	}
	s.expired.Add(n)
	return okResult(binary.AppendUvarint(nil, n)...)
}
