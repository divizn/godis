package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

func TestOversizeFrameRejectedWithoutAllocating(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFFF)
	r := io.MultiReader(bytes.NewReader(hdr[:]), bytes.NewReader(make([]byte, 64)))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ReadFrame(r)
	runtime.ReadMemStats(&after)

	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("allocated %d bytes rejecting an oversize frame", n)
	}
}

func TestTruncatedFrame(t *testing.T) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxFrameSize)
	r := io.MultiReader(bytes.NewReader(hdr[:]), strings.NewReader("only a few bytes"))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ReadFrame(r)
	runtime.ReadMemStats(&after)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
	}
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("allocated %d bytes for a frame that sent 16", n)
	}

	if _, err := ReadFrame(bytes.NewReader(hdr[:2])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated header: got %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestWriteFrameRefusesOversize(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, make([]byte, MaxFrameSize+1)); err == nil {
		t.Fatal("want error writing an oversize frame")
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes of an oversize frame", buf.Len())
	}
}

func TestRequestRoundTrip(t *testing.T) {
	for _, r := range []Request{
		{Op: OpPing},
		{Op: OpGet, Key: "k"},
		{Op: OpSet, Client: 1 << 63, Seq: 7, Key: "k", Value: []byte("v\x00")},
		{Op: OpDel, Client: 3, Seq: 1, Key: ""},
		{Op: OpCas, Client: 3, Seq: 2, Key: "k", Expected: []byte("old"), ExpectFound: true, Value: []byte("new")},
		{Op: OpCas, Client: 3, Seq: 3, Key: "k", Value: []byte("new")},
		{Op: OpIncr, Client: 3, Seq: 4, Key: "n", Delta: -1 << 62},
		{Op: OpSet, Client: 3, Seq: 5, Key: "k", Value: []byte("v"), TTL: 1500},
		{Op: OpSet, Client: 3, Seq: 6, Key: "k", TTL: MaxTTL},
		{Op: OpExpire, Client: 3, Seq: 7, Key: "k", TTL: 90_000},
		{Op: OpExpire, Client: 3, Seq: 8, Key: "k", TTL: -5},
		{Op: OpTTL, Key: "k"},
		{Op: OpAddNode, Key: "node-4", RaftAddr: "10.0.0.4:7000", ClientAddr: "10.0.0.4:8080"},
		{Op: OpRemoveNode, Key: "node-4"},
		{Op: OpNodes},
	} {
		got, err := ParseRequest(r.Append(nil))
		if err != nil {
			t.Fatalf("parse %+v: %v", r, err)
		}
		if got.Op != r.Op || got.Client != r.Client || got.Seq != r.Seq || got.Key != r.Key || !bytes.Equal(got.Value, r.Value) ||
			!bytes.Equal(got.Expected, r.Expected) || got.ExpectFound != r.ExpectFound || got.Delta != r.Delta || got.TTL != r.TTL ||
			got.RaftAddr != r.RaftAddr || got.ClientAddr != r.ClientAddr {
			t.Fatalf("round trip of %+v gave %+v", r, got)
		}
	}
}

func TestMalformedTTLFieldsRejected(t *testing.T) {
	uv := func(v uint64) []byte { return binary.AppendUvarint(nil, v) }
	bad := map[string][]byte{
		"set without ttl":         {OpSet, 1, 1, 1, 'k', 1, 'v'},
		"set truncated ttl":       {OpSet, 1, 1, 1, 'k', 1, 'v', 0x80},
		"set ttl too large":       append([]byte{OpSet, 1, 1, 1, 'k', 1, 'v'}, uv(MaxTTL+1)...),
		"set ttl past int64":      append([]byte{OpSet, 1, 1, 1, 'k', 1, 'v'}, uv(1<<63)...),
		"set trailing bytes":      {OpSet, 1, 1, 1, 'k', 1, 'v', 0, 0},
		"expire without ttl":      {OpExpire, 1, 1, 1, 'k'},
		"expire truncated ttl":    {OpExpire, 1, 1, 1, 'k', 0x80},
		"expire ttl too large":    append([]byte{OpExpire, 1, 1, 1, 'k'}, binary.AppendVarint(nil, MaxTTL+1)...),
		"expire ttl too negative": append([]byte{OpExpire, 1, 1, 1, 'k'}, binary.AppendVarint(nil, -MaxTTL-1)...),
		"expire trailing bytes":   {OpExpire, 1, 1, 1, 'k', 2, 0},
		"expire without session":  {OpExpire, 1, 'k', 2},
		"ttl without key":         {OpTTL},
		"ttl trailing bytes":      {OpTTL, 1, 'k', 0},
		"ttl with session fields": {OpTTL, 1, 1, 1, 'k'},
	}
	for name, p := range bad {
		if _, err := ParseRequest(p); err == nil {
			t.Errorf("%s: parsed, want error", name)
		}
	}
}

func TestRequestAppendMatchesTheWireLayout(t *testing.T) {
	got := Request{Op: OpSet, Client: 1, Seq: 2, Key: "k", Value: []byte("v"), TTL: 300}.Append(nil)
	want := []byte{OpSet, 1, 2, 1, 'k', 1, 'v', 0xac, 0x02}
	if !bytes.Equal(got, want) {
		t.Fatalf("set with ttl encodes as %v, want %v", got, want)
	}
	got = Request{Op: OpExpire, Client: 1, Seq: 2, Key: "k", TTL: -3}.Append(nil)
	want = []byte{OpExpire, 1, 2, 1, 'k', 5}
	if !bytes.Equal(got, want) {
		t.Fatalf("expire encodes as %v, want %v", got, want)
	}
}

func TestMalformedMembershipRequestsRejected(t *testing.T) {
	bad := map[string][]byte{
		"addnode without addresses":     {OpAddNode, 1, 'n'},
		"addnode without client addr":   {OpAddNode, 1, 'n', 1, 'r'},
		"addnode address past the end":  {OpAddNode, 1, 'n', 1, 'r', 5, 'c'},
		"addnode trailing bytes":        {OpAddNode, 1, 'n', 1, 'r', 1, 'c', 0},
		"removenode without id":         {OpRemoveNode},
		"removenode trailing bytes":     {OpRemoveNode, 1, 'n', 0},
		"removenode with session":       {OpRemoveNode, 1, 1, 1, 'n'},
		"nodes with a key":              {OpNodes, 1, 'n'},
		"op after the last membership":  {OpNodes + 1},
		"addnode with a session prefix": {OpAddNode, 1, 1, 1, 'n', 1, 'r', 1, 'c'},
	}
	for name, p := range bad {
		if _, err := ParseRequest(p); err == nil {
			t.Errorf("%s: parsed, want error", name)
		}
	}
}

func TestNodesBodyRoundTrip(t *testing.T) {
	for _, nodes := range [][]Node{
		nil,
		{{ID: "a", RaftAddr: "h:1", ClientAddr: "h:2"}},
		{{ID: "a", RaftAddr: "h:1", ClientAddr: "h:2"}, {ID: "b", RaftAddr: "", ClientAddr: "x"}},
	} {
		got, err := ParseNodes(AppendNodes(nil, nodes))
		if err != nil {
			t.Fatalf("parse %+v: %v", nodes, err)
		}
		if len(got) != len(nodes) {
			t.Fatalf("round trip of %+v gave %+v", nodes, got)
		}
		for i := range nodes {
			if got[i] != nodes[i] {
				t.Fatalf("round trip of %+v gave %+v", nodes, got)
			}
		}
	}
	for name, p := range map[string][]byte{
		"empty":              {},
		"count past the end": {5},
		"truncated field":    {1, 3, 'a'},
		"missing addresses":  {1, 1, 'a'},
		"trailing bytes":     append(AppendNodes(nil, []Node{{ID: "a"}}), 0),
	} {
		if _, err := ParseNodes(p); err == nil {
			t.Errorf("%s: parsed, want error", name)
		}
	}
}
