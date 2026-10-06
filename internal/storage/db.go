package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/divizn/godis/internal/wal"
)

// record layout: op | uvarint key length | key | value
const (
	opSet byte = iota + 1
	opDelete
)

// DB is a Store whose writes are logged and fsynced before they are applied.
type DB struct {
	mu    sync.Mutex
	store *Store
	log   *wal.Log
}

// Open replays the log at path into memory and opens it for appending.
func Open(path string) (*DB, error) {
	store := New()
	log, err := wal.Open(path, func(rec []byte) error { return apply(store, rec) })
	if err != nil {
		return nil, fmt.Errorf("open storage: %w", err)
	}
	return &DB{store: store, log: log}, nil
}

// Get returns the stored slice itself; callers must not modify it.
func (db *DB) Get(key string) ([]byte, bool) {
	return db.store.Get(key)
}

// ponytail: one write lock held across each fsync caps writes at one fsync apiece; group commit lifts it
func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.log.Append(encode(opSet, key, value)); err != nil {
		return fmt.Errorf("set %q: %w", key, err)
	}
	db.store.Set(key, value)
	return nil
}

func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.log.Append(encode(opDelete, key, nil)); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	db.store.Delete(key)
	return nil
}

func (db *DB) Close() error {
	return db.log.Close()
}

func encode(op byte, key string, value []byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(key)+len(value))
	buf = append(buf, op)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	return append(buf, value...)
}

func apply(s *Store, rec []byte) error {
	op, rest := rec[0], rec[1:]
	n, w := binary.Uvarint(rest)
	if w <= 0 {
		return errors.New("malformed key length")
	}
	rest = rest[w:]
	if n > uint64(len(rest)) {
		return fmt.Errorf("key length %d runs past end of record", n)
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
