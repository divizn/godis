package main

import (
	"slices"
	"testing"

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
