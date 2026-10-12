package storage

import "testing"

func mustApply(t *testing.T, s *Store, cmd []byte) {
	t.Helper()
	if err := s.Apply(cmd); err != nil {
		t.Fatalf("apply %q: %v", cmd, err)
	}
}

func TestApplyEncodedCommands(t *testing.T) {
	s := New()
	mustApply(t, s, EncodeSet("a", []byte("1")))
	mustApply(t, s, EncodeSet("b", []byte("2")))
	mustApply(t, s, EncodeSet("a", []byte("3")))
	mustApply(t, s, EncodeSet("empty", nil))
	mustApply(t, s, EncodeSet("bin\x00\x01", []byte("\x00\xff\x02")))
	mustApply(t, s, EncodeDelete("b"))

	want := map[string]string{"a": "3", "empty": "", "bin\x00\x01": "\x00\xff\x02"}
	for k, v := range want {
		if got, ok := s.Get(k); !ok || string(got) != v {
			t.Errorf("get %q: got %q, %v; want %q", k, got, ok, v)
		}
	}
	if _, ok := s.Get("b"); ok {
		t.Error("deleted key b still present")
	}
}

func TestMalformedCommandChangesNothing(t *testing.T) {
	cmds := map[string][]byte{
		"empty":            {},
		"unknown op":       {9, 1, 'k'},
		"truncated key":    {opSet, 5, 'k'},
		"bad varint":       {opSet, 0xff},
		"delete with data": append(EncodeDelete("k"), 'x'),
	}
	for name, cmd := range cmds {
		t.Run(name, func(t *testing.T) {
			s := New()
			mustApply(t, s, EncodeSet("k", []byte("v")))
			if err := s.Apply(cmd); err == nil {
				t.Fatal("want error")
			}
			if v, ok := s.Get("k"); !ok || string(v) != "v" {
				t.Fatalf("store changed by malformed command: got %q, %v", v, ok)
			}
		})
	}
}
