// Package storage implements the in-memory key-value engine.
package storage

import (
	"bytes"
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
