package consensus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"github.com/divizn/godis/internal/vfs"
	"github.com/divizn/godis/internal/wal"
)

// state file: uint64 term little endian | votedFor
func readState(fsys vfs.FS, dir string) (term uint64, votedFor string, err error) {
	b, err := fsys.ReadFile(filepath.Join(dir, "state"))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("read raft state: %w", err)
	}
	if len(b) < 8 {
		return 0, "", fmt.Errorf("raft state file is %d bytes, want at least 8", len(b))
	}
	return binary.LittleEndian.Uint64(b), string(b[8:]), nil
}

func writeState(fsys vfs.FS, dir string, term uint64, votedFor string) error {
	b := binary.LittleEndian.AppendUint64(nil, term)
	b = append(b, votedFor...)
	tmp := filepath.Join(dir, "state.tmp")
	if err := writeFileSync(fsys, tmp, b); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := fsys.Rename(tmp, filepath.Join(dir, "state")); err != nil {
		return fmt.Errorf("rename raft state: %w", err)
	}
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("sync raft state dir: %w", err)
	}
	return nil
}

func writeFileSync(fsys vfs.FS, path string, b []byte) error {
	f, err := fsys.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// snapshot file: uvarint index | uvarint term | data | uint32 crc32c of everything before it, little endian
func writeSnapshot(fsys vfs.FS, path string, index, term uint64, data []byte) error {
	b := binary.AppendUvarint(nil, index)
	b = binary.AppendUvarint(b, term)
	b = append(b, data...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
	return writeFileSync(fsys, path, b)
}

// readSnapshot returns index 0 when dir holds no snapshot
func readSnapshot(fsys vfs.FS, dir string) (index, term uint64, data []byte, err error) {
	b, err := fsys.ReadFile(filepath.Join(dir, "snapshot"))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, fmt.Errorf("read raft snapshot: %w", err)
	}
	if len(b) < 4 || crc32.Checksum(b[:len(b)-4], crcTable) != binary.LittleEndian.Uint32(b[len(b)-4:]) {
		return 0, 0, nil, errors.New("raft snapshot: checksum mismatch")
	}
	b = b[:len(b)-4]
	index, n := binary.Uvarint(b)
	if n <= 0 || index == 0 {
		return 0, 0, nil, errors.New("raft snapshot: bad index")
	}
	b = b[n:]
	term, n = binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, nil, errors.New("raft snapshot: bad term")
	}
	return index, term, b[n:], nil
}

// renameSnapshot durably makes the snapshot written to tmp dir's snapshot
func renameSnapshot(fsys vfs.FS, dir, tmp string) error {
	if err := fsys.Rename(tmp, filepath.Join(dir, "snapshot")); err != nil {
		return fmt.Errorf("rename raft snapshot: %w", err)
	}
	if err := fsys.SyncDir(dir); err != nil {
		return fmt.Errorf("sync raft snapshot dir: %w", err)
	}
	return nil
}

// logRecords encodes entries from index start as batches that each fit in one wal record
func logRecords(start uint64, entries []Entry) [][]byte {
	var recs [][]byte
	for len(entries) > 0 {
		i, size := 0, binary.MaxVarintLen64
		for i < len(entries) {
			s := 2*binary.MaxVarintLen64 + len(entries[i].Cmd)
			if i > 0 && size+s > wal.MaxRecordSize {
				break
			}
			size += s
			i++
		}
		recs = append(recs, encodeBatch(start, entries[:i]))
		start += uint64(i)
		entries = entries[i:]
	}
	return recs
}

// log record: uvarint start index | (uvarint term | uvarint cmd length | cmd)*
func encodeBatch(start uint64, entries []Entry) []byte {
	b := binary.AppendUvarint(nil, start)
	for _, e := range entries {
		b = binary.AppendUvarint(b, e.Term)
		b = binary.AppendUvarint(b, uint64(len(e.Cmd)))
		b = append(b, e.Cmd...)
	}
	return b
}

func decodeBatch(b []byte) (uint64, []Entry, error) {
	start, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, errors.New("bad start index")
	}
	b = b[n:]
	var entries []Entry
	for len(b) > 0 {
		term, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, nil, errors.New("bad entry term")
		}
		b = b[n:]
		size, n := binary.Uvarint(b)
		if n <= 0 || size > uint64(len(b)-n) {
			return 0, nil, errors.New("bad entry length")
		}
		b = b[n:]
		var cmd []byte
		if size > 0 {
			cmd = slices.Clone(b[:size])
		}
		entries = append(entries, Entry{Term: term, Cmd: cmd})
		b = b[size:]
	}
	return start, entries, nil
}
