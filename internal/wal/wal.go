// Package wal implements the append-only write-ahead log.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/divizn/godis/internal/vfs"
)

// record layout: uint32 payload length | uint32 crc32c of payload | payload, little endian
const (
	headerSize    = 8
	MaxRecordSize = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Log struct {
	f vfs.File
}

// Open replays every intact record in path through replay, drops a torn tail
// left by a crash, and opens the log for appending.
func Open(fsys vfs.FS, path string, replay func(record []byte) error) (*Log, error) {
	valid, err := replayFile(fsys, path, replay)
	if err != nil {
		return nil, err
	}
	if err := fsys.Truncate(path, valid); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("truncate wal torn tail: %w", err)
	}

	f, err := fsys.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		return nil, errors.Join(fmt.Errorf("sync wal dir: %w", err), f.Close())
	}
	return &Log{f: f}, nil
}

// Append writes record and fsyncs before returning.
func (l *Log) Append(record []byte) error {
	buf, err := frame(nil, record)
	if err != nil {
		return err
	}
	if _, err := l.f.Write(buf); err != nil {
		return fmt.Errorf("write wal record: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	return nil
}

// Rewrite atomically replaces the log at path with records and opens it for appending.
// Close any Log open on path first.
func Rewrite(fsys vfs.FS, path string, records [][]byte) (*Log, error) {
	var buf []byte
	for _, rec := range records {
		var err error
		if buf, err = frame(buf, rec); err != nil {
			return nil, err
		}
	}
	tmp := path + ".tmp"
	if err := writeSync(fsys, tmp, buf); err != nil {
		return nil, fmt.Errorf("write rewritten wal: %w", err)
	}
	if err := fsys.Rename(tmp, path); err != nil {
		return nil, fmt.Errorf("rename rewritten wal: %w", err)
	}
	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("sync wal dir: %w", err)
	}
	f, err := fsys.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("open rewritten wal: %w", err)
	}
	return &Log{f: f}, nil
}

func frame(buf, record []byte) ([]byte, error) {
	if len(record) == 0 {
		return nil, errors.New("wal record is empty")
	}
	if len(record) > MaxRecordSize {
		return nil, fmt.Errorf("wal record of %d bytes exceeds limit of %d", len(record), MaxRecordSize)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(record)))
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(record, crcTable))
	return append(buf, record...), nil
}

func writeSync(fsys vfs.FS, path string, b []byte) error {
	f, err := fsys.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

func (l *Log) Close() error {
	return l.f.Close()
}

func replayFile(fsys vfs.FS, path string, replay func([]byte) error) (int64, error) {
	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open wal for replay: %w", err)
	}
	off, err := replayRecords(bufio.NewReader(f), replay)
	return off, errors.Join(err, f.Close())
}

// ponytail: a checksum mismatch anywhere, even in the last record, fails Open; add truncate-and-continue if torn tails show up as bad checksums in practice
func replayRecords(r *bufio.Reader, replay func([]byte) error) (int64, error) {
	var off int64
	var hdr [headerSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, nil
			}
			return 0, fmt.Errorf("read wal header at offset %d: %w", off, err)
		}

		n := binary.LittleEndian.Uint32(hdr[0:4])
		if n == 0 {
			return off, nil
		}
		if n > MaxRecordSize {
			return 0, fmt.Errorf("wal record at offset %d: length %d exceeds limit", off, n)
		}

		rec := make([]byte, n)
		if _, err := io.ReadFull(r, rec); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, nil
			}
			return 0, fmt.Errorf("read wal record at offset %d: %w", off, err)
		}
		if crc32.Checksum(rec, crcTable) != binary.LittleEndian.Uint32(hdr[4:8]) {
			return 0, fmt.Errorf("wal record at offset %d: checksum mismatch", off)
		}
		if err := replay(rec); err != nil {
			return 0, fmt.Errorf("replay wal record at offset %d: %w", off, err)
		}
		off += headerSize + int64(n)
	}
}
