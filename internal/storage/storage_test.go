package storage

import (
	"strconv"
	"sync"
	"testing"
)

func TestSetGetDelete(t *testing.T) {
	s := New()

	if _, ok := s.Get("a"); ok {
		t.Fatal("get on empty store: want miss")
	}

	s.Set("a", []byte("1"))
	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("get after set: got %q, %v", v, ok)
	}

	s.Set("a", []byte("2"))
	if v, _ := s.Get("a"); string(v) != "2" {
		t.Fatalf("get after overwrite: got %q", v)
	}

	s.Delete("a")
	if _, ok := s.Get("a"); ok {
		t.Fatal("get after delete: want miss")
	}
}

func TestSetCopiesValue(t *testing.T) {
	s := New()
	buf := []byte("abc")
	s.Set("k", buf)
	buf[0] = 'x'

	if v, _ := s.Get("k"); string(v) != "abc" {
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
				s.Get(k)
				if j%10 == 0 {
					s.Delete(k)
				}
			}
		})
	}
	wg.Wait()
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", []byte("value"))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key")
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
