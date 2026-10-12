package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/divizn/godis/internal/node"
)

func TestParseCluster(t *testing.T) {
	got, err := parseCluster("node-1=godis-1:7000/localhost:8081, node-2=10.0.0.2:7000/10.0.0.2:8080")
	if err != nil {
		t.Fatal(err)
	}
	want := []node.Peer{
		{ID: "node-1", RaftAddr: "godis-1:7000", ClientAddr: "localhost:8081"},
		{ID: "node-2", RaftAddr: "10.0.0.2:7000", ClientAddr: "10.0.0.2:8080"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	for _, bad := range []string{
		"node-1=godis-1:7000",
		"godis-1:7000/localhost:8081",
		"=godis-1:7000/localhost:8081",
		"node-1=/localhost:8081",
		"node-1=godis-1:7000/",
		"node-1=a:1/b:2,",
	} {
		if got, err := parseCluster(bad); err == nil {
			t.Errorf("parseCluster(%q) = %+v, want error", bad, got)
		}
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func buildServer(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "server")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func TestServesMetrics(t *testing.T) {
	bin := buildServer(t)
	port, raftPort, metricsPort := freePort(t), freePort(t), freePort(t)
	cmd := exec.Command(bin, "--id=a", fmt.Sprint("--port=", port), fmt.Sprint("--raft-port=", raftPort),
		fmt.Sprint("--metrics-port=", metricsPort), "--data-dir="+t.TempDir(), "--election-timeout=50ms")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
	})

	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", metricsPort)
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(body, "\ngodis_raft_leader 1\n") {
		if time.Now().After(deadline) {
			t.Fatalf("%s never reported this node as leader; last body:\n%s", url, body)
		}
		time.Sleep(20 * time.Millisecond)
		resp, err := http.Get(url)
		if err != nil {
			continue
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", url, err)
		}
		body = string(b)
	}
	for _, want := range []string{"\ngo_goroutines ", "\nprocess_cpu_seconds_total ", "\ngodis_raft_leader_changes_total 1\n"} {
		if !strings.Contains(body, want) {
			t.Errorf("%s lacks %q", url, strings.TrimSpace(want))
		}
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("server exited with %v after SIGTERM, want a clean exit", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server still running 5s after SIGTERM")
	}
}

func TestServerExitsWhenRaftHalts(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	bin := buildServer(t)
	data := filepath.Join(t.TempDir(), "data")
	t.Cleanup(func() {
		if err := os.Chmod(data, 0o755); err != nil {
			t.Error(err)
		}
	})

	port, raftPort := freePort(t), freePort(t)
	// the unreachable peer keeps the node in elections, each one persisting a new term
	cmd := exec.Command(bin, "--id=a", fmt.Sprint("--port=", port), fmt.Sprint("--raft-port=", raftPort), "--data-dir="+data,
		"--election-timeout=20ms", fmt.Sprintf("--cluster=a=127.0.0.1:%d/127.0.0.1:%d,b=127.0.0.1:1/127.0.0.1:1", raftPort, port))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(data, "state")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server never persisted a term")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.Chmod(data, 0o500); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-exited:
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("server exited with %v, want a non-zero exit status", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server still running 5s after raft could no longer persist its term")
	}
}
