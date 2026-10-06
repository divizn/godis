package storage

import (
	"io"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/divizn/godis/internal/wal"
)

func mustClose(t *testing.T, c io.Closer) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func openDB(t *testing.T, path string) *DB {
	t.Helper()
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustSet(t *testing.T, db *DB, k, v string) {
	t.Helper()
	if err := db.Set(k, []byte(v)); err != nil {
		t.Fatalf("set %q: %v", k, err)
	}
}

func TestReopenRestoresState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	db := openDB(t, path)
	mustSet(t, db, "a", "1")
	mustSet(t, db, "b", "2")
	mustSet(t, db, "a", "3")
	mustSet(t, db, "empty", "")
	mustSet(t, db, "bin\x00\x01", "\x00\xff\x02")
	if err := db.Delete("b"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	mustClose(t, db)

	db = openDB(t, path)
	defer mustClose(t, db)
	want := map[string]string{"a": "3", "empty": "", "bin\x00\x01": "\x00\xff\x02"}
	for k, v := range want {
		if got, ok := db.Get(k); !ok || string(got) != v {
			t.Errorf("get %q after reopen: got %q, %v; want %q", k, got, ok, v)
		}
	}
	if _, ok := db.Get("b"); ok {
		t.Error("deleted key b came back after reopen")
	}
}

func TestFailedAppendLeavesMemoryUnchanged(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "wal"))
	mustSet(t, db, "k", "old")
	mustClose(t, db)

	if err := db.Set("k", []byte("new")); err == nil {
		t.Fatal("set on closed log: want error")
	}
	if err := db.Delete("k"); err == nil {
		t.Fatal("delete on closed log: want error")
	}
	if v, ok := db.Get("k"); !ok || string(v) != "old" {
		t.Fatalf("memory changed despite failed append: got %q, %v", v, ok)
	}
}

func TestCorruptRecordFailsOpen(t *testing.T) {
	records := map[string][]byte{
		"unknown op":    {9, 1, 'k'},
		"truncated key": {opSet, 5, 'k'},
		"bad varint":    {opSet, 0xff},
	}
	for name, rec := range records {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			l, err := wal.Open(path, func([]byte) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Append(rec); err != nil {
				t.Fatal(err)
			}
			mustClose(t, l)

			if db, err := Open(path); err == nil {
				mustClose(t, db)
				t.Fatal("open with corrupt record: want error")
			}
		})
	}
}

func TestConcurrentWritesReplayToLiveState(t *testing.T) {
	keys := []string{"x", "y", "z"}
	for round := range 10 {
		path := filepath.Join(t.TempDir(), "wal")
		db := openDB(t, path)
		var wg sync.WaitGroup
		for g := range 4 {
			wg.Go(func() {
				for i := range 50 {
					k := keys[i%len(keys)]
					var err error
					if i%7 == 0 {
						err = db.Delete(k)
					} else {
						err = db.Set(k, []byte(strconv.Itoa(g*1000+i)))
					}
					if err != nil {
						t.Error(err)
					}
				}
			})
		}
		wg.Wait()
		live := map[string]string{}
		for _, k := range keys {
			if v, ok := db.Get(k); ok {
				live[k] = string(v)
			}
		}
		mustClose(t, db)

		db = openDB(t, path)
		for _, k := range keys {
			v, ok := db.Get(k)
			lv, lok := live[k]
			if ok != lok || string(v) != lv {
				t.Errorf("round %d, key %q: replayed %q, %v; live was %q, %v", round, k, v, ok, lv, lok)
			}
		}
		mustClose(t, db)
	}
}
