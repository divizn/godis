package storage

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

// testStore applies commands at increasing log indexes
type testStore struct {
	*Store
	t     *testing.T
	index uint64
}

func newTestStore(t *testing.T) *testStore {
	return &testStore{Store: New(), t: t}
}

// apply applies cmd at the next index, failing the test on a malformed command
func (s *testStore) apply(cmd []byte) []byte {
	s.t.Helper()
	s.index++
	r, err := s.Apply(s.index, cmd)
	if err != nil {
		s.t.Fatalf("apply %q: %v", cmd, err)
	}
	return r
}

func wantOK(t *testing.T, r []byte, body string) {
	t.Helper()
	got, err := ParseResult(r)
	if err != nil || string(got) != body {
		t.Fatalf("result %q, %v; want OK %q", got, err, body)
	}
}

func wantFailed(t *testing.T, r []byte, msg string) {
	t.Helper()
	if _, err := ParseResult(r); err == nil || !strings.Contains(err.Error(), msg) {
		t.Fatalf("result error %v, want one containing %q", err, msg)
	}
}

func wantValue(t *testing.T, s *testStore, key, value string, found bool) {
	t.Helper()
	if v, ok := s.Get(key, 0); ok != found || string(v) != value {
		t.Fatalf("get %q: got %q, %v; want %q, %v", key, v, ok, value, found)
	}
}

func TestApplyEncodedCommands(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 0, 0, "a", []byte("1")))
	s.apply(EncodeSet(1, 2, 0, 0, "b", []byte("2")))
	s.apply(EncodeSet(1, 3, 0, 0, "a", []byte("3")))
	s.apply(EncodeSet(1, 4, 0, 0, "empty", nil))
	s.apply(EncodeSet(1, 5, 0, 0, "bin\x00\x01", []byte("\x00\xff\x02")))
	wantOK(t, s.apply(EncodeDelete(1, 6, 0, "b")), "")

	want := map[string]string{"a": "3", "empty": "", "bin\x00\x01": "\x00\xff\x02"}
	for k, v := range want {
		wantValue(t, s, k, v, true)
	}
	wantValue(t, s, "b", "", false)
}

func TestMalformedCommandChangesNothing(t *testing.T) {
	cmds := map[string][]byte{
		"empty":                {},
		"unknown op":           {9, 1, 1, 0, 1, 'k'},
		"no session":           {opSet},
		"no seq":               {opSet, 1},
		"no now":               {opSet, 1, 1},
		"seq zero":             {opSet, 1, 0, 0, 1, 'k'},
		"truncated key":        {opSet, 1, 1, 0, 5, 'k'},
		"bad varint":           {opSet, 1, 1, 0, 0xff},
		"set without deadline": {opSet, 1, 1, 0, 1, 'k'},
		"delete with data":     append(EncodeDelete(1, 1, 0, "k"), 'x'),
		"cas without flag":     {opCas, 1, 1, 0, 1, 'k', 0},
		"cas bad flag":         {opCas, 1, 1, 0, 1, 'k', 0, 2, 0},
		"cas truncated expect": {opCas, 1, 1, 0, 1, 'k', 0, 1, 5, 'v'},
		"incr without delta":   {opIncr, 1, 1, 0, 1, 'k'},
		"incr trailing bytes":  append(EncodeIncr(1, 1, 0, "k", 1), 0),
		"expire no deadline":   {opExpire, 1, 1, 0, 1, 'k'},
		"expire trailing":      append(EncodeExpire(1, 1, 0, "k", 5), 0),
		"sweep no now":         {opSweep},
		"sweep trailing":       append(EncodeSweep(5), 0),
	}
	for name, cmd := range cmds {
		t.Run(name, func(t *testing.T) {
			s := newTestStore(t)
			s.apply(EncodeSet(7, 1, 0, 0, "k", []byte("v")))
			before := s.Snapshot()
			if _, err := s.Apply(2, cmd); err == nil {
				t.Fatal("want error")
			}
			if !bytes.Equal(s.Snapshot(), before) {
				t.Fatal("malformed command changed the store or its sessions")
			}
		})
	}
}

func TestDuplicateWritesApplyOnce(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 0, 0, "k", []byte("a")))
	wantOK(t, s.apply(EncodeIncr(1, 2, 0, "n", 1)), "1")

	wantOK(t, s.apply(EncodeIncr(1, 2, 0, "n", 1)), "1")
	wantValue(t, s, "n", "1", true)
	s.apply(EncodeSet(1, 1, 0, 0, "k", []byte("b")))
	wantValue(t, s, "k", "a", true)
	if n := s.Duplicates(); n != 2 {
		t.Fatalf("counted %d duplicates, want 2", n)
	}

	wantFailed(t, s.apply(EncodeSet(2, 3, 0, 0, "k", []byte("c"))), "session expired")
	wantValue(t, s, "k", "a", true)

	s.apply(EncodeSet(3, 1, 0, 0, "k", []byte("d")))
	wantValue(t, s, "k", "d", true)
	wantOK(t, s.apply(EncodeIncr(1, 3, 0, "n", 1)), "2")

	wantFailed(t, s.apply(EncodeIncr(1, 4, 0, "k", 1)), "not an integer")
	s.apply(EncodeSet(3, 2, 0, 0, "k", []byte("5")))
	wantFailed(t, s.apply(EncodeIncr(1, 4, 0, "k", 1)), "not an integer")
	wantValue(t, s, "k", "5", true)
}

func TestIncr(t *testing.T) {
	tests := []struct {
		name    string
		initial *string
		delta   int64
		want    string
		fail    string
	}{
		{"missing counts as zero", nil, 5, "5", ""},
		{"adds to a value", ptr("40"), 2, "42", ""},
		{"negative delta", ptr("3"), -10, "-7", ""},
		{"from a negative value", ptr("-1"), 1, "0", ""},
		{"not an integer", ptr("abc"), 1, "", "not an integer"},
		{"empty value", ptr(""), 1, "", "not an integer"},
		{"float", ptr("1.5"), 1, "", "not an integer"},
		{"overflow", ptr("9223372036854775807"), 1, "", "overflow"},
		{"negative overflow", ptr("-9223372036854775808"), -1, "", "overflow"},
		{"large delta overflow", ptr("2"), math.MaxInt64, "", "overflow"},
		{"reaches the maximum", ptr("9223372036854775806"), 1, "9223372036854775807", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			if tt.initial != nil {
				s.apply(EncodeSet(1, 1, 0, 0, "k", []byte(*tt.initial)))
			}
			r := s.apply(EncodeIncr(2, 1, 0, "k", tt.delta))
			if tt.fail != "" {
				wantFailed(t, r, tt.fail)
				if tt.initial == nil {
					wantValue(t, s, "k", "", false)
				} else {
					wantValue(t, s, "k", *tt.initial, true)
				}
				return
			}
			wantOK(t, r, tt.want)
			wantValue(t, s, "k", tt.want, true)
		})
	}
}

func ptr(s string) *string { return &s }

func TestCas(t *testing.T) {
	tests := []struct {
		name        string
		initial     *string
		expected    string
		expectFound bool
		swapped     bool
	}{
		{"expect absent on absent", nil, "", false, true},
		{"expect absent on present", ptr("v"), "", false, false},
		{"expect absent on an empty value", ptr(""), "", false, false},
		{"expect value matches", ptr("v"), "v", true, true},
		{"expect value mismatches", ptr("v"), "w", true, false},
		{"expect value on absent", nil, "v", true, false},
		{"expect empty on absent", nil, "", true, false},
		{"expect empty on an empty value", ptr(""), "", true, true},
		{"expect a prefix", ptr("vv"), "v", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestStore(t)
			if tt.initial != nil {
				s.apply(EncodeSet(1, 1, 0, 0, "k", []byte(*tt.initial)))
			}
			r := s.apply(EncodeCas(2, 1, 0, 0, "k", []byte(tt.expected), tt.expectFound, []byte("new")))
			if !tt.swapped {
				wantOK(t, r, "\x00")
				if tt.initial == nil {
					wantValue(t, s, "k", "", false)
				} else {
					wantValue(t, s, "k", *tt.initial, true)
				}
				return
			}
			wantOK(t, r, "\x01")
			wantValue(t, s, "k", "new", true)
		})
	}
}

func TestSessionEvictionIsDeterministic(t *testing.T) {
	stores := []*testStore{newTestStore(t), newTestStore(t)}
	// client ids run against index order, so evicting by id instead of index shows
	clients := []uint64{}
	for i := range 21 {
		clients = append(clients, uint64(1000-i*7))
	}
	for _, s := range stores {
		s.maxSessions = 20
		for _, c := range clients {
			s.apply(EncodeIncr(c, 1, 0, "n", 1))
		}
	}
	if !bytes.Equal(stores[0].Snapshot(), stores[1].Snapshot()) {
		t.Fatal("two stores fed the same commands evicted different sessions")
	}
	s := stores[0]
	if n := len(s.sessions); n != 19 {
		t.Fatalf("%d sessions after passing a cap of 20, want 19: evict a tenth at once", n)
	}
	wantValue(t, s, "n", "21", true)
	for _, c := range clients[:2] {
		wantFailed(t, s.apply(EncodeIncr(c, 2, 0, "n", 1)), "session expired")
	}
	wantOK(t, s.apply(EncodeIncr(clients[10], 1, 0, "n", 1)), "11")
	wantValue(t, s, "n", "21", true)
}

func TestSessionsSurviveSnapshot(t *testing.T) {
	src := newTestStore(t)
	src.apply(EncodeIncr(5, 1, 0, "n", 1))
	src.apply(EncodeIncr(9, 1, 0, "n", 1))
	src.apply(EncodeIncr(5, 2, 0, "n", 1))

	dst := newTestStore(t)
	if err := dst.Restore(src.Snapshot()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !bytes.Equal(dst.Snapshot(), src.Snapshot()) {
		t.Fatal("restored store snapshots to different bytes")
	}
	dst.index = src.index
	wantOK(t, dst.apply(EncodeIncr(5, 2, 0, "n", 1)), "3")
	wantOK(t, dst.apply(EncodeIncr(9, 1, 0, "n", 1)), "2")
	wantValue(t, dst, "n", "3", true)
	wantOK(t, dst.apply(EncodeIncr(9, 2, 0, "n", 1)), "4")
}

func TestRestoreRejectsUnknownVersion(t *testing.T) {
	good := New().Snapshot()
	for _, v := range []byte{0, 1, 3} {
		b := append([]byte{v}, good[1:]...)
		if err := New().Restore(b); err == nil {
			t.Fatalf("restore of a version %d snapshot: want error", v)
		}
	}
}
