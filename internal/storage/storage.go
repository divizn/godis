// Package storage implements the in-memory key-value engine.
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Get returns the stored slice itself; callers must not modify it.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key string, value []byte) {
	v := bytes.Clone(value)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = v
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot encodes the whole store, keys sorted so equal stores give equal bytes:
// uvarint count | (uvarint key length | key | uvarint value length | value)*
// ponytail: holds the read lock while encoding, so writes wait for the whole snapshot; copy-on-write if that stalls the store
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := binary.AppendUvarint(nil, uint64(len(s.data)))
	for _, k := range slices.Sorted(maps.Keys(s.data)) {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, uint64(len(s.data[k])))
		b = append(b, s.data[k]...)
	}
	return b
}

// Restore replaces the store's contents with a Snapshot, leaving them unchanged if b is malformed.
func (s *Store) Restore(b []byte) error {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return errors.New("snapshot: bad key count")
	}
	b = b[n:]
	data := make(map[string][]byte, min(count, uint64(len(b))))
	field := func() ([]byte, error) {
		size, n := binary.Uvarint(b)
		if n <= 0 || size > uint64(len(b)-n) {
			return nil, errors.New("snapshot: bad length")
		}
		f := b[n : n+int(size)]
		b = b[n+int(size):]
		return f, nil
	}
	for range count {
		k, err := field()
		if err != nil {
			return err
		}
		v, err := field()
		if err != nil {
			return err
		}
		data[string(k)] = bytes.Clone(v)
	}
	if len(b) != 0 {
		return fmt.Errorf("snapshot: %d trailing bytes", len(b))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
	return nil
}
