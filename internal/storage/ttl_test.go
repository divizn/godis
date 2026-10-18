package storage

import (
	"bytes"
	"testing"
)

func wantTTL(t *testing.T, s *testStore, key string, now, remaining uint64, found bool) {
	t.Helper()
	if r, ok := s.TTL(key, now); r != remaining || ok != found {
		t.Fatalf("ttl %q at %d: got %d, %v; want %d, %v", key, now, r, ok, remaining, found)
	}
}

func TestKeyExpiresExactlyAtItsDeadline(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 0, 1000, "k", []byte("v")))
	if v, ok := s.Get("k", 999); !ok || string(v) != "v" {
		t.Fatalf("get one ms before the deadline: %q, %v", v, ok)
	}
	wantTTL(t, s, "k", 999, 1, true)
	if _, ok := s.Get("k", 1000); ok {
		t.Fatal("get at the deadline: want missing")
	}
	wantTTL(t, s, "k", 1000, 0, false)
	wantOK(t, s.apply(EncodeCas(1, 2, 1000, 0, "k", nil, false, []byte("w"))), "\x01")
}

func TestTimeNeverGoesBackwards(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 100, 1500, "k", []byte("v")))
	s.apply(EncodeSet(1, 2, 2000, 0, "other", []byte("x")))
	if c := s.Clock(); c != 2000 {
		t.Fatalf("clock %d, want 2000", c)
	}
	if _, ok := s.Get("k", 10); ok {
		t.Fatal("read stamped before the clock saw a key that expired at the clock")
	}
	wantOK(t, s.apply(EncodeCas(1, 3, 50, 0, "k", nil, false, []byte("w"))), "\x01")
	if c := s.Clock(); c != 2000 {
		t.Fatalf("clock moved back to %d", c)
	}
	s.apply(EncodeSet(1, 4, 2500, 0, "later", nil))
	if c := s.Clock(); c != 2500 {
		t.Fatalf("clock %d, want 2500", c)
	}
}

func TestDeadlineSemantics(t *testing.T) {
	s := newTestStore(t)

	s.apply(EncodeSet(1, 1, 0, 1000, "set", []byte("a")))
	s.apply(EncodeSet(1, 2, 10, 0, "set", []byte("b")))
	wantTTL(t, s, "set", 10, 0, true)

	s.apply(EncodeSet(1, 3, 10, 1000, "n", []byte("1")))
	wantOK(t, s.apply(EncodeIncr(1, 4, 20, "n", 1)), "2")
	wantTTL(t, s, "n", 20, 980, true)

	s.apply(EncodeSet(1, 5, 10, 1000, "cas", []byte("a")))
	wantOK(t, s.apply(EncodeCas(1, 6, 20, 0, "cas", []byte("zzz"), true, []byte("x"))), "\x00")
	wantTTL(t, s, "cas", 20, 980, true)
	wantOK(t, s.apply(EncodeCas(1, 7, 20, 0, "cas", []byte("a"), true, []byte("b"))), "\x01")
	wantTTL(t, s, "cas", 20, 0, true)

	s.apply(EncodeSet(1, 8, 10, 1000, "exp", []byte("5")))
	wantOK(t, s.apply(EncodeIncr(1, 9, 1000, "exp", 1)), "1")
	wantTTL(t, s, "exp", 1000, 0, true)
	s.apply(EncodeSet(1, 10, 10, 1000, "expcas", []byte("v")))
	wantOK(t, s.apply(EncodeCas(1, 11, 1000, 0, "expcas", []byte("v"), true, []byte("w"))), "\x00")
	wantValue(t, s, "expcas", "", false)

	s.apply(EncodeSet(1, 12, 10, 1000, "del", []byte("1")))
	s.apply(EncodeDelete(1, 13, 20, "del"))
	wantOK(t, s.apply(EncodeIncr(1, 14, 30, "del", 1)), "1")
	wantTTL(t, s, "del", 30, 0, true)
}

func TestExpire(t *testing.T) {
	s := newTestStore(t)
	wantOK(t, s.apply(EncodeExpire(1, 1, 5, "missing", 1000)), "\x00")
	if s.Len() != 0 {
		t.Fatal("expire created a key")
	}

	s.apply(EncodeSet(1, 2, 10, 100, "gone", []byte("v")))
	wantOK(t, s.apply(EncodeExpire(1, 3, 100, "gone", 5000)), "\x00")
	wantValue(t, s, "gone", "", false)

	s.apply(EncodeSet(1, 4, 100, 0, "live", []byte("v")))
	wantOK(t, s.apply(EncodeExpire(1, 5, 100, "live", 600)), "\x01")
	wantTTL(t, s, "live", 100, 500, true)
	wantOK(t, s.apply(EncodeExpire(1, 6, 200, "live", 300)), "\x01")
	wantTTL(t, s, "live", 200, 100, true)
	wantValue(t, s, "live", "v", true)

	wantOK(t, s.apply(EncodeExpire(1, 7, 250, "live", 250)), "\x01")
	wantValue(t, s, "live", "", false)
	if s.Len() != 1 {
		t.Fatalf("expire with ttl <= 0 left %d keys, want only the unswept expired one", s.Len())
	}
	wantOK(t, s.apply(EncodeExpire(1, 8, 260, "live", 250)), "\x00")
}

func TestSweepDeletesExactlyTheKeysDue(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 0, 100, "a", []byte("1")))
	s.apply(EncodeSet(1, 2, 0, 200, "b", []byte("2")))
	s.apply(EncodeSet(1, 3, 0, 0, "c", []byte("3")))
	s.apply(EncodeSet(1, 4, 0, 300, "d", []byte("4")))
	if s.Len() != 4 {
		t.Fatalf("%d keys before the sweep", s.Len())
	}

	wantOK(t, s.apply(EncodeSweep(200)), "\x02")
	if s.Len() != 2 || s.Expired() != 2 {
		t.Fatalf("after sweep: %d keys, %d expired; want 2, 2", s.Len(), s.Expired())
	}
	wantValue(t, s, "c", "3", true)
	wantTTL(t, s, "d", 200, 100, true)

	wantOK(t, s.apply(EncodeSweep(50)), "\x00")
	if s.Len() != 2 || s.Clock() != 200 {
		t.Fatalf("sweep stamped before the clock: %d keys, clock %d", s.Len(), s.Clock())
	}
	if len(s.sessions) != 1 {
		t.Fatalf("sweeps left %d sessions, want only the client's", len(s.sessions))
	}
}

func TestSnapshotKeepsDeadlinesAndClock(t *testing.T) {
	src := newTestStore(t)
	src.apply(EncodeSet(1, 1, 10, 1000, "a", []byte("1")))
	src.apply(EncodeSet(1, 2, 20, 50, "stale", []byte("2")))
	src.apply(EncodeSet(1, 3, 700, 0, "plain", []byte("3")))

	dst := newTestStore(t)
	dst.apply(EncodeSet(9, 1, 5000, 9000, "old", nil))
	if err := dst.Restore(src.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if dst.Clock() != 700 {
		t.Fatalf("restored clock %d, want 700", dst.Clock())
	}
	wantTTL(t, dst, "a", 700, 300, true)
	wantTTL(t, dst, "plain", 700, 0, true)
	if dst.Len() != 3 {
		t.Fatalf("restore kept %d keys, want the 3 in the snapshot, expired one included", dst.Len())
	}
	if !bytes.Equal(dst.Snapshot(), src.Snapshot()) {
		t.Fatal("restored store snapshots to different bytes")
	}
}

func TestSameCommandsGiveSameSnapshot(t *testing.T) {
	cmds := [][]byte{
		EncodeSet(1, 1, 10, 100, "a", []byte("1")),
		EncodeSet(2, 1, 20, 500, "b", []byte("2")),
		EncodeSet(3, 1, 30, 400, "c", []byte("3")),
		EncodeExpire(1, 2, 40, "a", 900),
		EncodeSweep(450),
		EncodeSet(1, 3, 460, 700, "d", nil),
	}
	var snaps [][]byte
	for range 20 {
		s := newTestStore(t)
		for _, c := range cmds {
			s.apply(c)
		}
		snaps = append(snaps, s.Snapshot())
	}
	for _, b := range snaps[1:] {
		if !bytes.Equal(b, snaps[0]) {
			t.Fatal("stores fed the same commands snapshot to different bytes")
		}
	}
}

func TestAnyDueTracksTheEarliestDeadline(t *testing.T) {
	s := newTestStore(t)
	if s.AnyDue(1 << 40) {
		t.Fatal("empty store has nothing due")
	}
	s.apply(EncodeSet(1, 1, 0, 0, "plain", nil))
	s.apply(EncodeSet(1, 2, 0, 500, "a", nil))
	s.apply(EncodeSet(1, 3, 0, 900, "b", nil))
	for _, tc := range []struct {
		now  uint64
		want bool
	}{{499, false}, {500, true}, {2000, true}} {
		if got := s.AnyDue(tc.now); got != tc.want {
			t.Fatalf("AnyDue(%d) = %v, want %v", tc.now, got, tc.want)
		}
	}
	s.apply(EncodeSweep(600))
	if s.AnyDue(600) || !s.AnyDue(900) {
		t.Fatal("after a sweep at 600 only b, due at 900, should be left")
	}
	s.apply(EncodeSet(1, 4, 0, 650, "d", nil))
	if s.AnyDue(100) {
		t.Fatal("d is not due at the clock, 600")
	}
	s.apply(EncodeSet(1, 5, 700, 0, "c", nil))
	if !s.AnyDue(100) {
		t.Fatal("AnyDue ignored the clock, which is at 700 with d due")
	}
}

func TestDuplicateSetTTLKeepsTheFirstDeadline(t *testing.T) {
	s := newTestStore(t)
	s.apply(EncodeSet(1, 1, 0, 1000, "k", []byte("v")))
	s.apply(EncodeSet(1, 1, 500, 1500, "k", []byte("v")))
	wantTTL(t, s, "k", 500, 500, true)
	if s.Duplicates() != 1 {
		t.Fatalf("%d duplicates, want 1", s.Duplicates())
	}
}
