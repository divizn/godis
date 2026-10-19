package main

import (
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/divizn/godis/client"
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

// startNode runs a node for the test with fresh listeners, returning its raft and client addresses; peers is nil for a joining node
func startNode(t *testing.T, id string, peers func(raft, client string) []node.Peer, ops bool) (raft, clientAddr string) {
	t.Helper()
	var lns [2]net.Listener
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
	}
	var members []node.Peer
	if peers != nil {
		members = peers(lns[1].Addr().String(), lns[0].Addr().String())
	}
	n, err := node.Start(node.Config{
		ID:              id,
		Peers:           members,
		Dir:             t.TempDir(),
		ClientListener:  lns[0],
		RaftListener:    lns[1],
		ElectionTimeout: 100 * time.Millisecond,
		MembershipOps:   ops,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Stop(); err != nil {
			t.Error(err)
		}
	})
	return lns[1].Addr().String(), lns[0].Addr().String()
}

func dialClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// soloClient runs a one-node cluster for the test and returns a client of it
func soloClient(t *testing.T, ops bool) (c *client.Client, raft, clientAddr string) {
	t.Helper()
	raft, clientAddr = startNode(t, "solo", func(raft, client string) []node.Peer {
		return []node.Peer{{ID: "solo", RaftAddr: raft, ClientAddr: client}}
	}, ops)
	return dialClient(t, clientAddr), raft, clientAddr
}

func TestScriptedSession(t *testing.T) {
	c, _, _ := soloClient(t, false)

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
		"CAS c - 1",
		"CAS c - 2",
		"CAS c 1 2",
		"GET c",
		"INCR n",
		"INCR n -5",
		"INCR n x",
		"INCR c",
		"incr greeting 1 2",
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
		"(integer) 1",
		"(integer) 0",
		"(integer) 1",
		`"2"`,
		"(integer) 1",
		"(integer) -4",
		`(error) delta "x" is not an integer`,
		"(integer) 3",
		"(error) wrong number of arguments for 'incr'",
		"PONG",
	}, "\n") + "\n"
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestClusterCommands(t *testing.T) {
	c, raft, clientAddr := soloClient(t, true)
	raft2, client2 := startNode(t, "joiner", nil, true)

	script := strings.Join([]string{
		"CLUSTER NODES",
		"cluster remove nobody",
		"CLUSTER REMOVE solo",
		"CLUSTER ADD joiner",
		"CLUSTER FLY",
		"CLUSTER",
		fmt.Sprint("CLUSTER ADD joiner ", raft2, " ", client2),
		"cluster nodes",
		"CLUSTER REMOVE solo",
		"CLUSTER NODES",
	}, "\n")
	var out strings.Builder
	if err := repl(c, strings.NewReader(script), &out, ""); err != nil {
		t.Fatalf("repl: %v", err)
	}
	want := strings.Join([]string{
		fmt.Sprint("solo ", raft, " ", clientAddr),
		`(error) remove node nobody: consensus: not a member: "nobody"`,
		"(error) remove node solo: consensus: cannot remove the last member",
		"(error) syntax error, expected CLUSTER ADD id raft-addr client-addr, CLUSTER REMOVE id or CLUSTER NODES",
		"(error) syntax error, expected CLUSTER ADD id raft-addr client-addr, CLUSTER REMOVE id or CLUSTER NODES",
		"(error) wrong number of arguments for 'cluster'",
		"OK",
		fmt.Sprint("solo ", raft, " ", clientAddr, "\njoiner ", raft2, " ", client2),
		"OK",
		fmt.Sprint("joiner ", raft2, " ", client2),
	}, "\n") + "\n"
	if out.String() != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

func TestClusterCommandsAreRefusedByDefault(t *testing.T) {
	c, _, _ := soloClient(t, false)
	var out strings.Builder
	if err := repl(c, strings.NewReader("CLUSTER NODES\n"), &out, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "--membership-ops") {
		t.Fatalf("output %q, want the refusal to name --membership-ops", out.String())
	}
}
