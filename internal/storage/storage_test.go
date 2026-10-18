package storage

import (
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New()

	if _, ok := s.Get("a", 0); ok {
		t.Fatal("get on empty store: want miss")
	}

	s.Set("a", []byte("1"))
	if v, ok := s.Get("a", 0); !ok || string(v) != "1" {
		t.Fatalf("get after set: got %q, %v", v, ok)
	}

	s.Set("a", []byte("2"))
	if v, _ := s.Get("a", 0); string(v) != "2" {
		t.Fatalf("get after overwrite: got %q", v)
	}

	s.Delete("a")
	if _, ok := s.Get("a", 0); ok {
		t.Fatal("get after delete: want miss")
	}
}

func TestSetCopiesValue(t *testing.T) {
	s := New()
	buf := []byte("abc")
	s.Set("k", buf)
	buf[0] = 'x'

	if v, _ := s.Get("k", 0); string(v) != "abc" {
		t.Fatalf("stored value changed with caller buffer: got %q", v)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 1000 {
				k := strconv.Itoa(j % 50)
				s.Set(k, []byte(strconv.Itoa(i)))
				s.Get(k, 0)
				if j%10 == 0 {
					s.Delete(k)
				}
			}
		})
	}
	wg.Wait()
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	for name, kv := range map[string]map[string]string{
		"empty store": {},
		"values":      {"a": "1", "bb": "22", "empty value": "", "": "empty key"},
	} {
		t.Run(name, func(t *testing.T) {
			src := New()
			for k, v := range kv {
				src.Set(k, []byte(v))
			}
			dst := New()
			dst.Set("stale", []byte("x"))
			if err := dst.Restore(src.Snapshot()); err != nil {
				t.Fatalf("restore: %v", err)
			}
			if _, ok := dst.Get("stale", 0); ok {
				t.Fatal("restore kept a key the snapshot does not have")
			}
			for k, v := range kv {
				if got, ok := dst.Get(k, 0); !ok || string(got) != v {
					t.Fatalf("get %q after restore: got %q, %v; want %q, true", k, got, ok, v)
				}
			}
			if !bytes.Equal(dst.Snapshot(), src.Snapshot()) {
				t.Fatal("restored store snapshots to different bytes")
			}
		})
	}
}

func TestRestoreRejectsBadSnapshot(t *testing.T) {
	src := New()
	for i, k := range []string{"key", "other"} {
		if _, err := src.Apply(uint64(i+1), EncodeSet(uint64(i+1), 1, 0, uint64(i)*50, k, []byte("v"))); err != nil {
			t.Fatal(err)
		}
	}
	good := src.Snapshot()
	bad := map[string][]byte{
		"empty":         {},
		"garbage":       {0xff, 0xff, 0xff},
		"trailing byte": append(slices.Clone(good), 0),
	}
	for i := 1; i < len(good); i++ {
		bad[fmt.Sprint("truncated to ", i)] = good[:i]
	}
	for name, b := range bad {
		t.Run(name, func(t *testing.T) {
			s := New()
			s.Set("keep", []byte("me"))
			if err := s.Restore(b); err == nil {
				t.Fatal("restore of a bad snapshot: want error")
			}
			if v, ok := s.Get("keep", 0); !ok || string(v) != "me" {
				t.Fatalf("failed restore changed the store: keep = %q, %v", v, ok)
			}
			if _, ok := s.Get("key", 0); ok {
				t.Fatal("failed restore applied part of the snapshot")
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", []byte("value"))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key", 0)
		}
	})
}

func BenchmarkSet(b *testing.B) {
	s := New()
	v := []byte("value")
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Set("key", v)
		}
	})
}
