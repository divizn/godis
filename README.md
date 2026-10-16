# godis

A distributed key-value store written from scratch in Go: a Raft-replicated log, an in-memory map as the state machine, and a small binary TCP protocol. Storage, the write-ahead log, consensus and networking use only the standard library; the one dependency is the Prometheus client for metrics. It runs as a 3-node cluster with linearizable reads and writes. No snapshots, membership changes or client dedupe yet (see Limitations).

## Layout

- `internal/storage`: the in-memory `Store` (map behind an `RWMutex`) and the command codec the Raft log carries
- `internal/wal`: append-only log, one fsync per record (`uint32 len | crc32c | payload`), torn-tail truncation on replay
- `internal/consensus`: Raft (elections, replication, `Propose`, `ReadIndex`), persisted through the WAL
- `internal/network`: the client protocol, its server and the Go client
- `internal/node`: wires Raft, the store, the client server, the `net/rpc` peer transport and the metrics together
- `cmd/server`: one node; `cmd/godis-cli`: REPL and one-shot client

## How a write flows

```
client → any node ──REDIRECT──→ leader
leader: append to Raft log, fsync
leader ──AppendEntries──→ followers: append, fsync, ack
majority acked → committed → applied to the Store → OK to client
```

Followers never serve reads or writes; they answer REDIRECT with the leader's client address and the client follows it. Reads go to the leader too, after a ReadIndex round: the leader confirms with a majority that it still leads, then waits until it has applied everything committed before the read.

## Guarantees

- Linearizable writes and reads, from the leader only.
- A write is acked only once it is fsynced on a majority and applied on the leader.
- Leader loss: the remaining majority elects a new leader within one to two election timeouts (1s by default) and the client retries through the redirect. A write that timed out may still have committed, and a retry applies it twice (no dedupe).
- A leader that hasn't heard from a majority for an election timeout steps down, and its pending writes and reads fail with a redirect instead of timing out.
- A node that can no longer persist its log or term halts and the process exits non-zero, rather than serve from a state it can't make durable.

## Running it

Single node (a cluster of one):

```sh
go run ./cmd/server --port=8080 --id=node-1 --data-dir=data
```

Three nodes plus Prometheus:

```sh
docker compose up -d --build   # clients on 8081-8083, metrics on 2112-2114, Prometheus UI on 9090
docker compose down -v         # also deletes the data volumes
```

Server flags: `--id`, `--port` (8080), `--raft-port` (7000), `--metrics-port` (2112), `--data-dir`, `--election-timeout` (1s, heartbeats a tenth of it), `--cluster=id=raft-addr/client-addr,...` listing every member including itself (empty means a cluster of one).

Client:

```sh
go run ./cmd/godis-cli --addr=localhost:8081             # REPL: PING, GET k, SET k v, DEL k
go run ./cmd/godis-cli --addr=localhost:8081 SET k v     # one-shot
```

Metrics are at `http://<node>:2112/metrics`:

- `godis_requests_total{op,result}`: get, set, del by ok, not_found, redirect, error
- `godis_request_duration_seconds{op}`: histogram, Raft round trip included
- `godis_raft_term`, `godis_raft_commit_index`, `godis_raft_leader` (1 on the leader)
- `godis_raft_leader_changes_total`: times this node learned of a new leader

Plus the Go runtime and process collectors. Leadership transitions are also logged (became leader, stepped down, following leader).

## Wire protocol

Every message is a frame: `uint32 length | payload`, big endian, payloads up to 16 MiB. One request in flight per connection.

- Request: `op` byte (1 PING, 2 GET, 3 SET, 4 DEL), then for GET, SET and DEL the key as `uvarint length | bytes`, then for SET the value the same way.
- Response: `status` byte (0 OK, 1 NOT_FOUND, 2 ERROR, 3 REDIRECT), then the body: the value for a GET, the error message for ERROR, the leader's client address for REDIRECT (empty when no leader is known; the client waits 100ms and retries).

Peers talk to each other over `net/rpc` with gob on the raft port.

## Benchmarks

`go test -run='^$' -bench=. -benchtime=3s ./internal/node/`: a 3-node cluster over loopback in one process, real fsync, 1s election timeout, 128-byte values, client on the leader. Set is sequential with one client, SetParallel runs one client per goroutine (8), Get is a ReadIndex read.

| benchmark | ns/op | ops/s |
|---|---|---|
| Set | 2,909,303 | 344 |
| SetParallel | 1,235,083 | 810 |
| Get | 199,924 | 5,002 |

Intel Core i7-8650U (4 cores, 8 threads), SATA SSD (ext4), Linux 6.8, Go 1.26.0, 2026-10-12, with other work running on the machine (load average around 5). All three nodes share one disk, so every write costs three fsyncs on the same device; a real cluster spreads them. Writes are bound by fsync: each Propose holds the Raft mutex across its own fsync and there is no group commit. Reads cost one heartbeat round each. None of this has been optimized yet.

## Tests

```sh
go test -race ./...
go test -race -count=20 ./internal/consensus/   # flake check for the Raft tests
```

Raft is tested deterministically with `testing/synctest` on an in-memory network with partitions, drops and delays; `internal/node` runs real loopback clusters.

## Limitations

- No snapshots or log compaction: the log, memory and restart replay grow with all history.
- No client dedupe: a write that timed out may still commit, and a retry applies it again.
- One heartbeat round per read; concurrent reads are not batched.
- A follower holds the Raft mutex across the fsync of a big append, so a heartbeat arriving meanwhile waits for it.
- A corrupt log is not repaired: wipe that node's data dir by hand and let it catch up.
- No membership changes: the cluster is fixed at startup.
- No pre-vote, so a node rejoining after a partition bumps the term and forces an election.
