package wal

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func open(t *testing.T, path string) (*Log, []string) {
	t.Helper()
	var got []string
	l, err := Open(path, func(rec []byte) error {
		got = append(got, string(rec))
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l, got
}

func appendAll(t *testing.T, l *Log, recs ...string) {
	t.Helper()
	for _, r := range recs {
		if err := l.Append([]byte(r)); err != nil {
			t.Fatalf("append %q: %v", r, err)
		}
	}
}

func appendRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func TestReplayInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")

	l, got := open(t, path)
	if len(got) != 0 {
		t.Fatalf("new log replayed %v", got)
	}
	appendAll(t, l, "a", "bb", "ccc")
	l.Close()

	l, got = open(t, path)
	defer l.Close()
	if want := []string{"a", "bb", "ccc"}; !slices.Equal(got, want) {
		t.Fatalf("replay: got %v, want %v", got, want)
	}
}

func TestTornTailIsDropped(t *testing.T) {
	tails := map[string][]byte{
		"partial header":  {5, 0, 0},
		"partial payload": {5, 0, 0, 0, 0, 0, 0, 0, 'x'},
		"zero fill":       make([]byte, 16),
	}
	for name, tail := range tails {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			l, _ := open(t, path)
			appendAll(t, l, "a")
			l.Close()
			appendRaw(t, path, tail)

			l, got := open(t, path)
			if want := []string{"a"}; !slices.Equal(got, want) {
				t.Fatalf("replay: got %v, want %v", got, want)
			}
			appendAll(t, l, "b")
			l.Close()

			l, got = open(t, path)
			defer l.Close()
			if want := []string{"a", "b"}; !slices.Equal(got, want) {
				t.Fatalf("replay after append: got %v, want %v", got, want)
			}
		})
	}
}

func TestChecksumMismatchFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	l, _ := open(t, path)
	appendAll(t, l, "abc", "def")
	l.Close()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[headerSize] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, func([]byte) error { return nil }); err == nil {
		t.Fatal("open on corrupt log: want error")
	}
}

func TestAppendRejectsEmptyRecord(t *testing.T) {
	l, _ := open(t, filepath.Join(t.TempDir(), "wal"))
	defer l.Close()
	if err := l.Append(nil); err == nil {
		t.Fatal("append empty record: want error")
	}
}
