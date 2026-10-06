package consensus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// state file: uint64 term little endian | votedFor
func readState(dir string) (term uint64, votedFor string, err error) {
	b, err := os.ReadFile(filepath.Join(dir, "state"))
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

func writeState(dir string, term uint64, votedFor string) error {
	b := binary.LittleEndian.AppendUint64(nil, term)
	b = append(b, votedFor...)
	tmp := filepath.Join(dir, "state.tmp")
	if err := writeFileSync(tmp, b); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, "state")); err != nil {
		return fmt.Errorf("rename raft state: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync raft state dir: %w", err)
	}
	return nil
}

func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
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

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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
