package vfs

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"testing"
)

func write(t *testing.T, m *StrictMem, name string, flag int, b []byte, sync bool) {
	t.Helper()
	f, err := m.OpenFile(name, flag, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func newDir(t *testing.T, seed uint64) *StrictMem {
	m := NewStrictMem(seed)
	if err := m.MkdirAll("d", 0o755); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCrashKeepsSyncedAndTearsUnsyncedAppends(t *testing.T) {
	const flag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	lengths := map[int]bool{}
	for seed := range uint64(60) {
		m := newDir(t, seed)
		write(t, m, "d/f", flag, []byte("synced"), true)
		if err := m.SyncDir("d"); err != nil {
			t.Fatal(err)
		}
		write(t, m, "d/f", flag, []byte("-unsynced-tail"), false)
		m.Crash()
		got, err := m.ReadFile("d/f")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix([]byte("synced-unsynced-tail"), got) || !bytes.HasPrefix(got, []byte("synced")) {
			t.Fatalf("seed %d: after crash file is %q, want \"synced\" plus a prefix of the unsynced tail", seed, got)
		}
		lengths[len(got)] = true
	}
	if len(lengths) < 5 {
		t.Fatalf("torn tail lengths across seeds: %v, want the kept prefix to vary", lengths)
	}
}

func TestCrashRevertsUnsyncedOverwrite(t *testing.T) {
	m := newDir(t, 1)
	write(t, m, "d/state", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, []byte("old"), true)
	if err := m.SyncDir("d"); err != nil {
		t.Fatal(err)
	}
	write(t, m, "d/state", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, []byte("new-and-longer"), false)
	m.Crash()
	if got, _ := m.ReadFile("d/state"); string(got) != "old" {
		t.Fatalf("after crash file is %q, want the synced \"old\"", got)
	}
}

func TestCreateNeedsSyncDir(t *testing.T) {
	for _, syncDir := range []bool{false, true} {
		m := newDir(t, 1)
		write(t, m, "d/f", os.O_WRONLY|os.O_CREATE, []byte("x"), true)
		if syncDir {
			if err := m.SyncDir("d"); err != nil {
				t.Fatal(err)
			}
		}
		m.Crash()
		got, err := m.ReadFile("d/f")
		if syncDir && string(got) != "x" {
			t.Fatalf("with SyncDir: file is %q, %v, want it kept", got, err)
		}
		if !syncDir && !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("without SyncDir: file is %q, %v, want it gone", got, err)
		}
	}
}

func TestRenameNeedsSyncDir(t *testing.T) {
	for _, syncDir := range []bool{false, true} {
		m := newDir(t, 1)
		write(t, m, "d/f", os.O_WRONLY|os.O_CREATE, []byte("old"), true)
		if err := m.SyncDir("d"); err != nil {
			t.Fatal(err)
		}
		write(t, m, "d/f.tmp", os.O_WRONLY|os.O_CREATE, []byte("new"), true)
		if err := m.Rename("d/f.tmp", "d/f"); err != nil {
			t.Fatal(err)
		}
		if syncDir {
			if err := m.SyncDir("d"); err != nil {
				t.Fatal(err)
			}
		}
		m.Crash()
		got, err := m.ReadFile("d/f")
		want := "old"
		if syncDir {
			want = "new"
		}
		if err != nil || string(got) != want {
			t.Fatalf("syncDir=%v: d/f is %q, %v, want %q", syncDir, got, err, want)
		}
	}
}

func TestFrozenFailsEverythingUntilCrash(t *testing.T) {
	m := newDir(t, 1)
	f, err := m.OpenFile("d/f", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	m.Freeze()
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrFrozen) {
		t.Errorf("write: %v", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrFrozen) {
		t.Errorf("sync: %v", err)
	}
	if err := m.Rename("d/f", "d/g"); !errors.Is(err, ErrFrozen) {
		t.Errorf("rename: %v", err)
	}
	if _, err := m.OpenFile("d/h", os.O_CREATE, 0o644); !errors.Is(err, ErrFrozen) {
		t.Errorf("open: %v", err)
	}
	if err := m.SyncDir("d"); !errors.Is(err, ErrFrozen) {
		t.Errorf("syncdir: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("close: %v", err)
	}

	m.Crash()
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrFrozen) {
		t.Errorf("write on a file opened before the crash: %v", err)
	}
	if _, err := m.OpenFile("d/h", os.O_CREATE, 0o644); err != nil {
		t.Errorf("open after crash: %v", err)
	}
}
