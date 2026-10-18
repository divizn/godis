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
	} {
		got, err := ParseRequest(r.Append(nil))
		if err != nil {
			t.Fatalf("parse %+v: %v", r, err)
		}
		if got.Op != r.Op || got.Client != r.Client || got.Seq != r.Seq || got.Key != r.Key || !bytes.Equal(got.Value, r.Value) ||
			!bytes.Equal(got.Expected, r.Expected) || got.ExpectFound != r.ExpectFound || got.Delta != r.Delta {
			t.Fatalf("round trip of %+v gave %+v", r, got)
		}
	}
}
