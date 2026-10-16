package lincheck

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func set(client int, key, value string, call, ret int64) Op {
	return Op{Client: client, Kind: Set, Key: key, Value: value, Call: call, Return: ret}
}

func del(client int, key string, call, ret int64) Op {
	return Op{Client: client, Kind: Del, Key: key, Call: call, Return: ret}
}

func get(client int, key, value string, call, ret int64) Op {
	return Op{Client: client, Kind: Get, Key: key, Value: value, Found: true, Call: call, Return: ret}
}

func miss(client int, key string, call, ret int64) Op {
	return Op{Client: client, Kind: Get, Key: key, Call: call, Return: ret}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		history []Op
		want    bool
	}{
		{"empty", nil, true},
		{"sequential", []Op{set(0, "x", "1", 1, 2), get(1, "x", "1", 3, 4)}, true},
		{"read before any write", []Op{miss(0, "x", 1, 2), set(1, "x", "1", 3, 4)}, true},
		{"stale read after a completed write", []Op{set(0, "x", "1", 1, 2), set(0, "x", "2", 3, 4), get(1, "x", "1", 5, 6)}, false},
		{"lost write", []Op{set(0, "x", "1", 1, 2), miss(1, "x", 3, 4)}, false},
		{"read of a value never written", []Op{set(0, "x", "1", 1, 2), get(1, "x", "9", 3, 4)}, false},
		{"read sees a write that starts after it returns", []Op{get(0, "x", "1", 1, 2), set(1, "x", "1", 3, 4)}, false},
		{"overlapping writes read as the first", []Op{set(0, "x", "1", 1, 4), set(1, "x", "2", 2, 5), get(2, "x", "1", 6, 7)}, true},
		{"overlapping writes read as the second", []Op{set(0, "x", "1", 1, 4), set(1, "x", "2", 2, 5), get(2, "x", "2", 6, 7)}, true},
		{"read concurrent with a write sees the old value", []Op{set(0, "x", "1", 1, 2), set(0, "x", "2", 3, 6), get(1, "x", "1", 4, 5)}, true},
		{"read concurrent with a write sees the new value", []Op{set(0, "x", "1", 1, 2), set(0, "x", "2", 3, 6), get(1, "x", "2", 4, 5)}, true},
		{"two readers see concurrent writes in opposite orders", []Op{
			set(0, "x", "1", 1, 20), set(1, "x", "2", 2, 21),
			get(2, "x", "1", 3, 4), get(2, "x", "2", 7, 8),
			get(3, "x", "2", 5, 6), get(3, "x", "1", 9, 10),
		}, false},
		{"value returns after being overwritten", []Op{set(0, "x", "1", 1, 2), get(1, "x", "1", 3, 4), set(0, "x", "2", 5, 6), get(1, "x", "2", 7, 8), get(1, "x", "1", 9, 10)}, false},
		{"pending write seen", []Op{set(0, "x", "1", 1, Pending), get(1, "x", "1", 2, 3)}, true},
		{"pending write never seen", []Op{set(0, "x", "1", 1, Pending), miss(1, "x", 2, 3), miss(1, "x", 4, 5)}, true},
		{"pending write seen then unseen", []Op{set(0, "x", "1", 1, Pending), get(1, "x", "1", 2, 3), miss(1, "x", 4, 5)}, false},
		{"pending write seen late", []Op{set(0, "x", "1", 1, Pending), set(1, "x", "2", 2, 3), get(1, "x", "2", 4, 5), get(1, "x", "1", 6, 7)}, true},
		{"pending write seen before its call", []Op{get(1, "x", "1", 1, 2), set(0, "x", "1", 3, Pending)}, false},
		{"pending delete seen", []Op{set(0, "x", "1", 1, 2), del(0, "x", 3, Pending), miss(1, "x", 4, 5)}, true},
		{"del then get", []Op{set(0, "x", "1", 1, 2), del(0, "x", 3, 4), miss(1, "x", 5, 6)}, true},
		{"get after del sees the deleted value", []Op{set(0, "x", "1", 1, 2), del(0, "x", 3, 4), get(1, "x", "1", 5, 6)}, false},
		{"set after del", []Op{del(0, "x", 1, 2), set(0, "x", "1", 3, 4), get(1, "x", "1", 5, 6)}, true},
		{"empty value is not absent", []Op{set(0, "x", "", 1, 2), miss(1, "x", 3, 4)}, false},
		{"absent is not an empty value", []Op{get(1, "x", "", 1, 2)}, false},
		{"keys are separate", []Op{set(0, "x", "1", 1, 2), miss(1, "y", 3, 4)}, true},
		{"value read from the wrong key", []Op{set(0, "x", "1", 1, 2), get(1, "y", "1", 3, 4)}, false},
		{"failed get is dropped", []Op{set(0, "x", "1", 1, 2), get(1, "x", "9", 3, Pending)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Check(tt.history, 1<<20)
			if got.Unknown {
				t.Fatalf("budget ran out on key %q", got.Key)
			}
			if got.Linearizable != tt.want {
				t.Fatalf("got linearizable %v, want %v (key %q, ops %v)", got.Linearizable, tt.want, got.Key, got.Ops)
			}
		})
	}
}

func TestCheckReportsTheFailingKey(t *testing.T) {
	h := []Op{set(0, "a", "1", 1, 2), get(1, "a", "1", 3, 4), set(0, "b", "1", 1, 2), miss(1, "b", 3, 4)}
	got := Check(h, 1<<20)
	if got.Linearizable || got.Key != "b" || len(got.Ops) != 2 {
		t.Fatalf("got %+v, want key b with its 2 ops", got)
	}
}

func TestCheckBudget(t *testing.T) {
	var h []Op
	for i := range 30 {
		h = append(h, set(i, "x", fmt.Sprint(i), int64(i+1), int64(1000+i)))
	}
	h = append(h, get(99, "x", "never", 50, 60))
	got := Check(h, 100)
	if got.Linearizable || !got.Unknown || got.Key != "x" {
		t.Fatalf("got %+v, want unknown on key x", got)
	}
}

func TestCheckAgreesWithBruteForce(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var yes, no int
	for i := range 20000 {
		h := randomHistory(rng)
		got := Check(h, 1<<20)
		want := bruteForce(h)
		if got.Unknown || got.Linearizable != want {
			t.Fatalf("history %d: Check says %+v, brute force says %v:\n%s", i, got, want, format(h))
		}
		if want {
			yes++
		} else {
			no++
		}
	}
	t.Logf("%d linearizable, %d not", yes, no)
	if yes < 2000 || no < 2000 {
		t.Fatalf("%d linearizable and %d not: the generator does not exercise both verdicts", yes, no)
	}
}

func randomHistory(rng *rand.Rand) []Op {
	n := 1 + rng.IntN(7)
	times := rng.Perm(2 * n)
	h := make([]Op, n)
	for i := range h {
		a, b := int64(times[2*i]+1), int64(times[2*i+1]+1)
		op := Op{Client: i, Key: []string{"x", "y"}[rng.IntN(2)], Call: min(a, b), Return: max(a, b)}
		switch r := rng.IntN(20); {
		case r < 8:
			op.Kind = Get
			op.Found = rng.IntN(4) > 0
			if op.Found {
				op.Value = fmt.Sprint(rng.IntN(3))
			}
			if rng.IntN(10) == 0 {
				op.Return = Pending
			}
		case r < 17:
			op.Kind = Set
			op.Value = fmt.Sprint(rng.IntN(3))
		default:
			op.Kind = Del
		}
		if op.Kind != Get && rng.IntN(4) == 0 {
			op.Return = Pending
		}
		h[i] = op
	}
	return h
}

// bruteForce tries every subset of pending writes and every order of the whole history, keys together
func bruteForce(h []Op) bool {
	var ops []Op
	var pending []int
	for _, op := range h {
		if op.Kind == Get && op.Return == Pending {
			continue
		}
		if op.Return == Pending {
			pending = append(pending, len(ops))
		}
		ops = append(ops, op)
	}
	for mask := range 1 << len(pending) {
		used := make([]bool, len(ops))
		for i, idx := range pending {
			if mask&(1<<i) == 0 {
				used[idx] = true
			}
		}
		if permute(ops, used, map[string]string{}) {
			return true
		}
	}
	return false
}

func permute(ops []Op, used []bool, state map[string]string) bool {
	if !slices.Contains(used, false) {
		return true
	}
	for i, op := range ops {
		if used[i] {
			continue
		}
		blocked := false
		for j, p := range ops {
			if !used[j] && p.Return < op.Call {
				blocked = true
			}
		}
		if blocked {
			continue
		}
		v, found := state[op.Key]
		switch op.Kind {
		case Get:
			if found != op.Found || found && v != op.Value {
				continue
			}
		case Set:
			state[op.Key] = op.Value
		case Del:
			delete(state, op.Key)
		}
		used[i] = true
		ok := permute(ops, used, state)
		used[i] = false
		if found {
			state[op.Key] = v
		} else {
			delete(state, op.Key)
		}
		if ok {
			return true
		}
	}
	return false
}

func format(h []Op) string {
	var s string
	for _, op := range h {
		s += op.String() + "\n"
	}
	return s
}
