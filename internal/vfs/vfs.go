// Package vfs is the filesystem seam under the write-ahead log and the Raft state files.
package vfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// FS is the part of the filesystem godis uses.
type FS interface {
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	ReadFile(name string) ([]byte, error)
	Rename(oldpath, newpath string) error
	Truncate(name string, size int64) error
	MkdirAll(path string, perm fs.FileMode) error
	// SyncDir makes the directory entries created, renamed and removed in dir durable.
	SyncDir(dir string) error
}

type File interface {
	io.Reader
	io.Writer
	Sync() error
	Close() error
}

// OS is the real filesystem.
var OS FS = osFS{}

type osFS struct{}

func (osFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (osFS) ReadFile(name string) ([]byte, error)         { return os.ReadFile(name) }
func (osFS) Rename(oldpath, newpath string) error         { return os.Rename(oldpath, newpath) }
func (osFS) Truncate(name string, size int64) error       { return os.Truncate(name, size) }
func (osFS) MkdirAll(path string, perm fs.FileMode) error { return os.MkdirAll(path, perm) }

func (osFS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
