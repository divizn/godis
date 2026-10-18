package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// ErrFrozen is returned by every operation after Freeze, and by files left open across a Crash.
var ErrFrozen = errors.New("vfs: filesystem frozen")

type inode struct {
	data    []byte
	durable []byte
}

// StrictMem is an in-memory FS that keeps only what a real crash would: file contents synced with Sync,
// and directory entries synced with SyncDir. Directories themselves are always durable.
// ponytail: bytes overwritten in place are never partially kept, only appends tear; model sector writes if overwrites appear
type StrictMem struct {
	mu     sync.Mutex
	rng    *rand.Rand
	frozen bool
	armed  bool
	budget int
	epoch  int
	dirs   map[string]bool
	cur    map[string]*inode
	dur    map[string]*inode
}

func NewStrictMem(seed uint64) *StrictMem {
	return &StrictMem{
		rng:  rand.New(rand.NewPCG(seed, seed)),
		dirs: map[string]bool{".": true, "/": true},
		cur:  map[string]*inode{},
		dur:  map[string]*inode{},
	}
}

// Freeze makes every later operation except Close fail until Crash.
func (m *StrictMem) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
}

// FreezeAfter lets n more operations that change the filesystem succeed, then freezes it as Freeze does.
func (m *StrictMem) FreezeAfter(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.armed, m.budget = true, n
}

// Crash reverts to what is durable, each file keeping a random prefix of the bytes appended since its last Sync,
// then thaws the filesystem and invalidates every file open before it.
func (m *StrictMem) Crash() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	m.frozen, m.armed = false, false
	m.cur = maps.Clone(m.dur)
	for _, ino := range m.cur {
		if bytes.HasPrefix(ino.data, ino.durable) {
			ino.data = ino.data[:len(ino.durable)+m.rng.IntN(len(ino.data)-len(ino.durable)+1)]
		} else {
			ino.data = slices.Clone(ino.durable)
		}
		ino.durable = slices.Clone(ino.data)
	}
}

func (m *StrictMem) check(op, name string) error {
	if m.armed && !m.frozen {
		if m.budget == 0 {
			m.frozen = true
		}
		m.budget--
	}
	return m.checkRead(op, name)
}

func (m *StrictMem) checkRead(op, name string) error {
	if m.frozen {
		return &fs.PathError{Op: op, Path: name, Err: ErrFrozen}
	}
	return nil
}

func (m *StrictMem) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = filepath.Clean(name)
	if err := m.check("open", name); err != nil {
		return nil, err
	}
	ino := m.cur[name]
	if ino == nil {
		if flag&os.O_CREATE == 0 || !m.dirs[filepath.Dir(name)] {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
		}
		ino = &inode{}
		m.cur[name] = ino
	}
	if flag&os.O_TRUNC != 0 {
		ino.data = nil
	}
	return &memFile{m: m, ino: ino, epoch: m.epoch, name: name, appendOnly: flag&os.O_APPEND != 0, writeOnly: flag&os.O_WRONLY != 0}, nil
}

func (m *StrictMem) ReadFile(name string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = filepath.Clean(name)
	if err := m.checkRead("read", name); err != nil {
		return nil, err
	}
	ino := m.cur[name]
	if ino == nil {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	return slices.Clone(ino.data), nil
}

func (m *StrictMem) Rename(oldpath, newpath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldpath, newpath = filepath.Clean(oldpath), filepath.Clean(newpath)
	if err := m.check("rename", oldpath); err != nil {
		return err
	}
	ino := m.cur[oldpath]
	if ino == nil || !m.dirs[filepath.Dir(newpath)] {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}
	delete(m.cur, oldpath)
	m.cur[newpath] = ino
	return nil
}

func (m *StrictMem) Truncate(name string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = filepath.Clean(name)
	if err := m.check("truncate", name); err != nil {
		return err
	}
	ino := m.cur[name]
	if ino == nil {
		return &fs.PathError{Op: "truncate", Path: name, Err: fs.ErrNotExist}
	}
	if size < 0 || size > int64(len(ino.data)) {
		return &fs.PathError{Op: "truncate", Path: name, Err: fs.ErrInvalid}
	}
	ino.data = ino.data[:size]
	return nil
}

func (m *StrictMem) MkdirAll(path string, perm fs.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	path = filepath.Clean(path)
	if err := m.check("mkdir", path); err != nil {
		return err
	}
	for d := path; !m.dirs[d]; d = filepath.Dir(d) {
		m.dirs[d] = true
	}
	return nil
}

func (m *StrictMem) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = filepath.Clean(dir)
	if err := m.check("sync", dir); err != nil {
		return err
	}
	if !m.dirs[dir] {
		return &fs.PathError{Op: "sync", Path: dir, Err: fs.ErrNotExist}
	}
	for name := range m.dur {
		if filepath.Dir(name) == dir {
			delete(m.dur, name)
		}
	}
	for name, ino := range m.cur {
		if filepath.Dir(name) == dir {
			m.dur[name] = ino
		}
	}
	return nil
}

type memFile struct {
	m          *StrictMem
	ino        *inode
	epoch      int
	name       string
	pos        int
	appendOnly bool
	writeOnly  bool
}

func (f *memFile) check(op string) error {
	if f.epoch != f.m.epoch {
		return &fs.PathError{Op: op, Path: f.name, Err: ErrFrozen}
	}
	if op == "read" {
		return f.m.checkRead(op, f.name)
	}
	return f.m.check(op, f.name)
}

func (f *memFile) Read(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("read"); err != nil {
		return 0, err
	}
	if f.writeOnly {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	if f.pos >= len(f.ino.data) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[f.pos:])
	f.pos += n
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("write"); err != nil {
		return 0, err
	}
	if f.appendOnly {
		f.pos = len(f.ino.data)
	}
	end := f.pos + len(p)
	if end > len(f.ino.data) {
		f.ino.data = append(f.ino.data, make([]byte, end-len(f.ino.data))...)
	}
	copy(f.ino.data[f.pos:], p)
	f.pos = end
	return len(p), nil
}

func (f *memFile) Sync() error {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()
	if err := f.check("sync"); err != nil {
		return err
	}
	f.ino.durable = slices.Clone(f.ino.data)
	return nil
}

func (f *memFile) Close() error { return nil }
