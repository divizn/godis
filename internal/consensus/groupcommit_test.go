package consensus

import (
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/divizn/godis/internal/vfs"
)

// slowSyncFS is the real filesystem with every file Sync delayed and counted
type slowSyncFS struct {
	vfs.FS
	delay time.Duration
	syncs atomic.Int64
	// syncing, if set, gets a value as each Sync starts, when nobody is waiting on it nothing is sent
	syncing chan struct{}
}

func (f *slowSyncFS) OpenFile(name string, flag int, perm fs.FileMode) (vfs.File, error) {
	file, err := f.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return slowFile{File: file, fs: f}, nil
}

type slowFile struct {
	vfs.File
	fs *slowSyncFS
}

func (f slowFile) Sync() error {
	select {
	case f.fs.syncing <- struct{}{}:
	default:
	}
	time.Sleep(f.fs.delay)
	f.fs.syncs.Add(1)
	return f.File.Sync()
}

// TestConcurrentProposalsShareFsyncs runs in real time: synctest does not advance its clock while a goroutine waits on a mutex
func TestConcurrentProposalsShareFsyncs(t *testing.T) {
	fsys := &slowSyncFS{FS: vfs.OS, delay: 5 * time.Millisecond}
	c := newStoppedCluster(t, 1, 1)
	c.fs = fsys
	c.start("n1")
	defer c.stopAll()
	c.waitLeader()

	const proposals = 32
	before := fsys.syncs.Load()
	var wg sync.WaitGroup
	for i := range proposals {
		wg.Go(func() {
			if err := c.propose("n1", fmt.Sprint("cmd", i), 5*time.Second); err != nil {
				t.Errorf("propose %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	if syncs := fsys.syncs.Load() - before; syncs > proposals/4 {
		t.Fatalf("%d concurrent proposals took %d fsyncs, want at most %d: proposals waiting on one fsync should share the next", proposals, syncs, proposals/4)
	}
}

// TestLargeQueuedProposalsStayUnderTheRecordLimit: two 40 MiB proposals queued behind a 2s fsync would make one 80 MiB batch, over the WAL's record limit
func TestLargeQueuedProposalsStayUnderTheRecordLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 80 MiB")
	}
	fsys := &slowSyncFS{FS: vfs.OS, delay: 2 * time.Second}
	c := newStoppedCluster(t, 1, 1)
	c.fs = fsys
	c.start("n1")
	defer c.stopAll()
	c.waitLeader()

	big := []string{strings.Repeat("a", 40<<20), strings.Repeat("b", 40<<20)}
	fsys.syncing = make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := c.propose("n1", "small", 30*time.Second); err != nil {
			t.Errorf("propose small: %v", err)
		}
	})
	<-fsys.syncing
	for i, cmd := range big {
		wg.Go(func() {
			if err := c.propose("n1", cmd, 30*time.Second); err != nil {
				t.Errorf("propose %d: %v", i, err)
			}
		})
	}
	wg.Wait()
}
