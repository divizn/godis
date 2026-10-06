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

// request payload: op | uvarint-length-prefixed fields
const (
	opPing byte = iota + 1
	opGet
	opSet
	opDel
)

// response payload: status | body
const (
	statusOK byte = iota
	statusNotFound
	statusError
)

var errFrameTooLarge = errors.New("frame too large")

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

func appendRequest(b []byte, op byte, key string, value []byte) []byte {
	b = append(b, op)
	if op == opPing {
		return b
	}
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = append(b, key...)
	if op == opSet {
		b = binary.AppendUvarint(b, uint64(len(value)))
		b = append(b, value...)
	}
	return b
}

func parseRequest(p []byte) (op byte, key string, value []byte, err error) {
	if len(p) == 0 {
		return 0, "", nil, errors.New("empty request")
	}
	op, rest := p[0], p[1:]
	switch op {
	case opPing:
	case opGet, opDel, opSet:
		var k []byte
		if k, rest, err = cutField(rest); err != nil {
			return 0, "", nil, fmt.Errorf("key: %w", err)
		}
		key = string(k)
		if op == opSet {
			if value, rest, err = cutField(rest); err != nil {
				return 0, "", nil, fmt.Errorf("value: %w", err)
			}
		}
	default:
		return 0, "", nil, fmt.Errorf("unknown op %d", op)
	}
	if len(rest) != 0 {
		return 0, "", nil, fmt.Errorf("%d trailing bytes", len(rest))
	}
	return op, key, value, nil
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
