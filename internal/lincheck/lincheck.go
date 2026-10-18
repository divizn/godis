// Package lincheck checks a history of key-value operations for linearizability, each key as an independent register.
package lincheck

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

type Kind uint8

const (
	Get Kind = iota + 1
	Set
	Del
	Cas
	Incr
)

func (k Kind) String() string {
	switch k {
	case Get:
		return "get"
	case Set:
		return "set"
	case Del:
		return "del"
	case Cas:
		return "cas"
	case Incr:
		return "incr"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// Pending is the Return of an operation whose outcome is unknown: it may take effect any time after its call, or never.
const Pending = math.MaxInt64

// Op is one operation. Call and Return are logical times from one counter shared by every client,
// taken before the request is sent and after its reply arrives.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	// Value is the value written by a Set or a Cas, read by a Get that found the key, or returned by an Incr.
	Value string
	// Found is whether a Get found the key, or whether a Cas swapped.
	Found bool
	// Expected is the value a Cas swaps from, if ExpectFound; without it the Cas swaps only an absent key.
	Expected    string
	ExpectFound bool
	// Delta is what an Incr adds to the base-10 integer at Key, a missing key counting as 0.
	Delta  int64
	Call   int64
	Return int64
}

func (o Op) String() string {
	ret := fmt.Sprint(o.Return)
	if o.Return == Pending {
		ret = "pending"
	}
	s := fmt.Sprintf("[%d, %s] client %d %s %s", o.Call, ret, o.Client, o.Kind, o.Key)
	switch {
	case o.Kind == Set:
		s += " " + o.Value
	case o.Kind == Get && o.Found:
		s += " -> " + o.Value
	case o.Kind == Get:
		s += " -> not found"
	case o.Kind == Cas:
		from := "absent"
		if o.ExpectFound {
			from = o.Expected
		}
		s += fmt.Sprintf(" %s to %s -> swapped %v", from, o.Value, o.Found)
	case o.Kind == Incr:
		s += fmt.Sprintf(" %+d -> %s", o.Delta, o.Value)
	}
	return s
}

type Result struct {
	Linearizable bool
	// Unknown means the search budget ran out on Key; Linearizable is then false.
	Unknown bool
	// Key is the first key that failed or ran out, and Ops its operations.
	Key string
	Ops []Op
}

// Check reports whether history is linearizable; budget caps the search steps per key.
func Check(history []Op, budget int) Result {
	byKey := map[string][]Op{}
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		s := newSearch(byKey[key], budget)
		ok := s.run(register{})
		if s.exhausted {
			return Result{Unknown: true, Key: key, Ops: byKey[key]}
		}
		if !ok {
			return Result{Key: key, Ops: byKey[key]}
		}
	}
	return Result{Linearizable: true}
}

type register struct {
	value string
	found bool
}

// step applies op to r, reporting false if r does not explain op's result; a pending op's result is unknown
func step(r register, op Op) (register, bool) {
	pending := op.Return == Pending
	switch op.Kind {
	case Set:
		return register{op.Value, true}, true
	case Del:
		return register{}, true
	case Cas:
		swap := r.found == op.ExpectFound && (!r.found || r.value == op.Expected)
		if !pending && swap != op.Found {
			return r, false
		}
		if swap {
			return register{op.Value, true}, true
		}
		return r, true
	case Incr:
		var n int64
		if r.found {
			var err error
			if n, err = strconv.ParseInt(r.value, 10, 64); err != nil {
				return r, pending
			}
		}
		if op.Delta > 0 && n > math.MaxInt64-op.Delta || op.Delta < 0 && n < math.MinInt64-op.Delta {
			return r, pending
		}
		next := strconv.FormatInt(n+op.Delta, 10)
		if !pending && next != op.Value {
			return r, false
		}
		return register{next, true}, true
	}
	return r, r.found == op.Found && r.value == op.Value
}

// search is Wing and Gong's depth-first search for a linearization with Lowe's memoization of explored
// (linearized set, register) pairs
type search struct {
	ops       []Op
	done      []uint64
	left      int
	seen      map[string]struct{}
	steps     int
	budget    int
	exhausted bool
}

func newSearch(ops []Op, budget int) *search {
	read := map[string]bool{}
	prune := true
	for _, op := range ops {
		if op.Kind == Get && op.Found {
			read[op.Value] = true
		}
		// a cas or incr can observe a value without a get, or depend on it being overwritten
		if op.Kind == Cas || op.Kind == Incr {
			prune = false
		}
	}
	// a pending write of a value nobody read never helps explain a history of sets, dels and gets
	ops = slices.DeleteFunc(slices.Clone(ops), func(op Op) bool {
		return prune && op.Kind == Set && op.Return == Pending && !read[op.Value]
	})
	s := &search{ops: ops, done: make([]uint64, (len(ops)+63)/64), seen: map[string]struct{}{}, budget: budget}
	for _, op := range ops {
		if op.Return != Pending {
			s.left++
		}
	}
	return s
}

func (s *search) run(r register) bool {
	if s.left == 0 {
		return true
	}
	if s.steps++; s.steps > s.budget {
		s.exhausted = true
		return false
	}
	key := s.key(r)
	if _, ok := s.seen[key]; ok {
		return false
	}
	s.seen[key] = struct{}{}

	first := int64(Pending)
	for i, op := range s.ops {
		if !s.isDone(i) {
			first = min(first, op.Return)
		}
	}
	for i, op := range s.ops {
		// op can go next only if no op left out returned before op was called
		if s.isDone(i) || op.Call > first {
			continue
		}
		next, ok := step(r, op)
		if !ok {
			continue
		}
		s.mark(i, op, true)
		if s.run(next) {
			return true
		}
		s.mark(i, op, false)
	}
	return false
}

func (s *search) isDone(i int) bool { return s.done[i/64]&(1<<(i%64)) != 0 }

func (s *search) mark(i int, op Op, done bool) {
	s.done[i/64] ^= 1 << (i % 64)
	if op.Return == Pending {
		return
	}
	if done {
		s.left--
	} else {
		s.left++
	}
}

func (s *search) key(r register) string {
	var b strings.Builder
	for _, w := range s.done {
		b.Write(binary.LittleEndian.AppendUint64(nil, w))
	}
	if r.found {
		b.WriteByte(1)
		b.WriteString(r.value)
	} else {
		b.WriteByte(0)
	}
	return b.String()
}
