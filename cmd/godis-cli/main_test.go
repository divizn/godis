package main

import (
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/divizn/godis/internal/network"
	"github.com/divizn/godis/internal/node"
)

func TestSplitArgs(t *testing.T) {
	cases := map[string][]string{
		"GET k":                      {"GET", "k"},
		"  set   k\tv  ":             {"set", "k", "v"},
		`SET k "hello world"`:        {"SET", "k", "hello world"},
		`SET "a key" "line\n\x00\""`: {"SET", "a key", "line\n\x00\""},
		`SET k ""`:                   {"SET", "k", ""},
		"":                           nil,
	}
	for line, want := range cases {
		got, err := splitArgs(line)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("splitArgs(%q) = %q, %v; want %q", line, got, err, want)
		}
	}
	for _, line := range []string{`SET k "unterminated`, `SET k "a"b`, `SET k "\q"`} {
		if got, err := splitArgs(line); err == nil {
			t.Errorf("splitArgs(%q) = %q; want error", line, got)
		}
	}
}

func TestScriptedSession(t *testing.T) {
	var lns [2]net.Listener
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
	}
	n, err := node.Start(node.Config{
		ID:             "solo",
		Peers:          []node.Peer{{ID: "solo", RaftAddr: lns[1].Addr().String(), ClientAddr: lns[0].Addr().String()}},
		Dir:            t.TempDir(),
		ClientListener: lns[0],
		RaftListener:   lns[1],
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Stop(); err != nil {
			t.Error(err)
		}
	}()

	c, err := network.Dial(lns[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	script := strings.Join([]string{
		"ping",
		`SET greeting "hello world"`,
		"get greeting",
		"",
		"GET missing",
		`set empty ""`,
		"GET empty",
		"Del greeting",
		"GET greeting",
		"GET",
		"FLY away",
		`SET k "oops`,
		"PING",
	}, "\n")
	var out strings.Builder
	if err := repl(c, strings.NewReader(script), &out, ""); err != nil {
		t.Fatalf("repl: %v", err)
	}
	want := strings.Join([]string{
		"PONG",
		"OK",
		`"hello world"`,
		"(nil)",
		"OK",
		`""`,
		"OK",
		"(nil)",
		"(error) wrong number of arguments for 'get'",
		`(error) unknown command "FLY"`,
		"(error) invalid quoted argument",
		"PONG",
	}, "\n") + "\n"
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}
