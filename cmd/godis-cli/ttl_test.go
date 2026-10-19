package main

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestTTLCommands(t *testing.T) {
	c, _, _ := soloClient(t, false)
	do := func(line string) string {
		t.Helper()
		args, err := splitArgs(line)
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		out, err := run(c, args)
		if err != nil {
			return "(error) " + err.Error()
		}
		return out
	}
	want := func(line, want string) {
		t.Helper()
		if got := do(line); got != want {
			t.Fatalf("%s: got %q, want %q", line, got, want)
		}
	}
	wantMillis := func(line string, min, max int) {
		t.Helper()
		got := do(line)
		n, err := strconv.Atoi(strings.TrimPrefix(got, "(integer) "))
		if err != nil || n < min || n > max {
			t.Fatalf("%s: got %q, want an integer in [%d, %d]", line, got, min, max)
		}
	}

	want("SET k v PX 60000", "OK")
	want("GET k", `"v"`)
	wantMillis("PTTL k", 50000, 60000)
	want("set k w", "OK")
	want("PTTL k", "(integer) -1")
	want("PTTL missing", "(integer) -2")

	want("PEXPIRE k 90000", "(integer) 1")
	wantMillis("PTTL k", 80000, 90000)
	want("pexpire missing 1000", "(integer) 0")
	want("PEXPIRE k 0", "(integer) 1")
	want("GET k", "(nil)")
	want("PEXPIRE k 1000", "(integer) 0")

	for _, line := range []string{
		"SET k v PX 0",
		"SET k v PX -5",
		"SET k v PX abc",
		"SET k v PX",
		"SET k v EX 5",
		"SET k v PX 1 2",
		"PEXPIRE k abc",
		"PEXPIRE k",
		"PEXPIRE k " + fmt.Sprint(int64(1)<<62),
		"SET k v PX " + fmt.Sprint(int64(1)<<62),
		"PTTL",
		"PTTL a b",
	} {
		if got := do(line); !strings.HasPrefix(got, "(error)") {
			t.Fatalf("%s: got %q, want an error", line, got)
		}
	}
	want("GET k", "(nil)")
}
