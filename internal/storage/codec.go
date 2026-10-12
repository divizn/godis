package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// command layout: op | uvarint key length | key | value
const (
	opSet byte = iota + 1
	opDelete
)

func EncodeSet(key string, value []byte) []byte {
	return encode(opSet, key, value)
}

func EncodeDelete(key string) []byte {
	return encode(opDelete, key, nil)
}

func encode(op byte, key string, value []byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(key)+len(value))
	buf = append(buf, op)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	return append(buf, value...)
}

// Apply decodes an encoded command and applies it; a malformed command changes nothing.
func (s *Store) Apply(cmd []byte) error {
	if len(cmd) == 0 {
		return errors.New("empty command")
	}
	op, rest := cmd[0], cmd[1:]
	n, w := binary.Uvarint(rest)
	if w <= 0 {
		return errors.New("malformed key length")
	}
	rest = rest[w:]
	if n > uint64(len(rest)) {
		return fmt.Errorf("key length %d runs past end of command", n)
	}
	key, value := string(rest[:n]), rest[n:]

	switch op {
	case opSet:
		s.Set(key, value)
	case opDelete:
		if len(value) != 0 {
			return fmt.Errorf("delete of %q carries %d stray bytes", key, len(value))
		}
		s.Delete(key)
	default:
		return fmt.Errorf("unknown op %d", op)
	}
	return nil
}
