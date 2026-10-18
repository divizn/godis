// Package storage implements the in-memory key-value engine.
package storage

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxSessions = 100_000
	snapshotVersion    = 2
)

// Store is the state machine: the data plus a session per client, so a retried write applies once.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
	// deadlines holds the keys of data that expire, in Unix ms
	// ponytail: scanned whole by every sweep; a min-heap if keys with TTLs get numerous
	deadlines map[string]uint64
	// clock is the highest command time applied, which time in the state machine never goes behind
	clock       uint64
	sessions    map[uint64]session
	maxSessions int
	duplicates  atomic.Uint64
	expired     atomic.Uint64
}

// session is a client's last applied write, its log index and its result
type session struct {
	seq, index uint64
	result     []byte
}

func New() *Store {
	return &Store{data: map[string][]byte{}, deadlines: map[string]uint64{}, sessions: map[uint64]session{}, maxSessions: defaultMaxSessions}
}

// Apply applies the command at log index once per (client, seq) and returns its result, for ParseResult.
// The command runs at the later of its time and the highest time applied before it.
// A sweep has no session and runs every time.
// A duplicate, seq at or below the client's last, returns the last result and changes nothing.
// A seq above 1 from a client without a session fails with "session expired".
// The error is only for a malformed command, which changes nothing.
func (s *Store) Apply(index uint64, cmd []byte) ([]byte, error) {
	c, err := decode(cmd)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock = max(s.clock, c.now)
	if c.op == opSweep {
		return s.sweep(s.clock), nil
	}
	sess, ok := s.sessions[c.client]
	if ok && c.seq <= sess.seq {
		s.duplicates.Add(1)
		return sess.result, nil
	}
	var result []byte
	if !ok && c.seq > 1 {
		// ponytail: an evicted client retrying its seq 1 write can still apply it twice; expire sessions by leader time to close that
		result = failedResult("session expired")
	} else {
		result = s.exec(c, s.clock)
	}
	s.sessions[c.client] = session{seq: c.seq, index: index, result: result}
	if len(s.sessions) > s.maxSessions {
		s.evict()
	}
	return result, nil
}

// Duplicates counts the writes Apply dropped as duplicates since New.
func (s *Store) Duplicates() uint64 {
	return s.duplicates.Load()
}

// evict drops the tenth of sessions with the oldest last index, which every replica agrees on; mu must be held
// ponytail: sorts every session on each eviction, amortized by evicting a tenth at once; keep an index-ordered list if it shows in profiles
func (s *Store) evict() {
	clients := slices.SortedFunc(maps.Keys(s.sessions), func(a, b uint64) int {
		return cmp.Compare(s.sessions[a].index, s.sessions[b].index)
	})
	for _, c := range clients[:max(1, s.maxSessions/10)] {
		delete(s.sessions, c)
	}
}

// Expired counts the keys sweeps deleted since New.
func (s *Store) Expired() uint64 {
	return s.expired.Load()
}

// Clock is the highest command time applied, in Unix ms.
func (s *Store) Clock() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clock
}

// Len counts the keys held, expired ones a sweep has not yet deleted included.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Get reads key at the later of now, in Unix ms, and the store's clock. It returns the stored slice itself; callers must not modify it.
func (s *Store) Get(key string, now uint64) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.live(key, max(now, s.clock))
}

// TTL reports the ms left before key expires, read like Get. Zero means no deadline, found false a missing or expired key.
func (s *Store) TTL(key string, now uint64) (remaining uint64, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t := max(now, s.clock)
	if _, found = s.live(key, t); !found {
		return 0, false
	}
	if d := s.deadlines[key]; d != 0 {
		remaining = d - t
	}
	return remaining, true
}

// due is whether a key with deadline d, 0 for none, is expired at t
func due(d, t uint64) bool { return d != 0 && d <= t }

// live returns key's value unless it is missing or expired at t; mu must be held
func (s *Store) live(key string, t uint64) ([]byte, bool) {
	v, ok := s.data[key]
	if due(s.deadlines[key], t) {
		return nil, false
	}
	return v, ok
}

// put stores a copy of value with deadline, 0 for none; mu must be held
func (s *Store) put(key string, value []byte, deadline uint64) {
	s.data[key] = bytes.Clone(value)
	if deadline == 0 {
		delete(s.deadlines, key)
	} else {
		s.deadlines[key] = deadline
	}
}

// remove deletes key and its deadline; mu must be held
func (s *Store) remove(key string) {
	delete(s.data, key)
	delete(s.deadlines, key)
}

// Set stores value with no deadline.
func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.put(key, value, 0)
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.remove(key)
}

// Snapshot encodes the whole store, keys and clients sorted so equal stores give equal bytes:
// version 2 | uvarint clock | uvarint count | (uvarint key length | key | uvarint value length | value | uvarint deadline)* |
// uvarint session count | (uvarint client | uvarint seq | uvarint last index | uvarint result length | result)*
// ponytail: holds the read lock while encoding, so writes wait for the whole snapshot; copy-on-write if that stalls the store
func (s *Store) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b := binary.AppendUvarint([]byte{snapshotVersion}, s.clock)
	b = binary.AppendUvarint(b, uint64(len(s.data)))
	for _, k := range slices.Sorted(maps.Keys(s.data)) {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, uint64(len(s.data[k])))
		b = append(b, s.data[k]...)
		b = binary.AppendUvarint(b, s.deadlines[k])
	}
	b = binary.AppendUvarint(b, uint64(len(s.sessions)))
	for _, c := range slices.Sorted(maps.Keys(s.sessions)) {
		sess := s.sessions[c]
		b = binary.AppendUvarint(b, c)
		b = binary.AppendUvarint(b, sess.seq)
		b = binary.AppendUvarint(b, sess.index)
		b = binary.AppendUvarint(b, uint64(len(sess.result)))
		b = append(b, sess.result...)
	}
	return b
}

// Restore replaces the store's contents with a Snapshot, leaving them unchanged if b is malformed.
func (s *Store) Restore(b []byte) error {
	if len(b) == 0 || b[0] != snapshotVersion {
		return errors.New("snapshot: unknown version")
	}
	b = b[1:]
	uvarint := func(what string) (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, fmt.Errorf("snapshot: bad %s", what)
		}
		b = b[n:]
		return v, nil
	}
	field := func() ([]byte, error) {
		size, err := uvarint("length")
		if err != nil {
			return nil, err
		}
		if size > uint64(len(b)) {
			return nil, errors.New("snapshot: bad length")
		}
		f := b[:size]
		b = b[size:]
		return f, nil
	}
	clock, err := uvarint("clock")
	if err != nil {
		return err
	}
	count, err := uvarint("key count")
	if err != nil {
		return err
	}
	data := make(map[string][]byte, min(count, uint64(len(b))))
	deadlines := map[string]uint64{}
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
		d, err := uvarint("deadline")
		if err != nil {
			return err
		}
		if d != 0 {
			deadlines[string(k)] = d
		}
	}
	count, err = uvarint("session count")
	if err != nil {
		return err
	}
	sessions := make(map[uint64]session, min(count, uint64(len(b))))
	for range count {
		var c uint64
		var sess session
		for _, f := range []struct {
			v    *uint64
			what string
		}{{&c, "client"}, {&sess.seq, "seq"}, {&sess.index, "index"}} {
			if *f.v, err = uvarint(f.what); err != nil {
				return err
			}
		}
		r, err := field()
		if err != nil {
			return err
		}
		sess.result = bytes.Clone(r)
		sessions[c] = sess
	}
	if len(b) != 0 {
		return fmt.Errorf("snapshot: %d trailing bytes", len(b))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.deadlines, s.clock, s.sessions = data, deadlines, clock, sessions
	return nil
}
