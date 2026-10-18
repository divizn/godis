# godis

A distributed key-value store written from scratch in Go: a Raft-replicated log, an in-memory map as the state machine, and a small binary TCP protocol. Storage, the write-ahead log, consensus and networking use only the standard library; the one dependency is the Prometheus client for metrics. It runs as a 3-node cluster with linearizable reads and writes, applies each write exactly once through client sessions even when the client retries it, and compacts its log with snapshots that it ships to followers that fall behind. No membership changes yet (see Limitations).

Data dirs written before key expiry (TTL) can't be read: the log and snapshot formats changed again, as they did for client sessions before it. Wipe them.

## Layout

- `internal/storage`: the in-memory `Store` (map behind an `RWMutex`), its client session table, key deadlines, and the command codec the Raft log carries
- `internal/wal`: append-only log, one fsync per record (`uint32 len | crc32c | payload`), torn-tail truncation on replay
- `internal/consensus`: Raft (elections, replication, `Propose`, `ReadIndex`, snapshots with log compaction and `InstallSnapshot`), persisted through the WAL and a checksummed snapshot file
- `client`: the Go client, importable as `github.com/divizn/godis/client`
- `internal/protocol`: the client protocol's framing and request encoding
- `internal/network`: the client protocol server
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
- Exactly-once writes through the Go client. Each client picks a random 64-bit id and numbers its writes; every write carries `(client, seq)` through the log, and the store keeps each client's last seq and result, so a write that reaches the log twice takes effect once and the duplicate gets the cached result. The client retries a write with the same seq whenever the outcome is unknown: a lost leadership, a server timeout, a dropped connection, a failed dial. It gives up after 10 seconds, or sooner when the call's context ends, with `ErrOutcomeUnknown`, and the next write uses the next seq, so the abandoned one can still commit at most once. Until one of its writes has applied, a client whose write fails starts a new session instead, because the cluster may hold no session for it yet. A write that returns success took effect exactly once.
- Expiring keys. Every write in the log carries `now`, the proposing leader's clock in Unix ms. The state machine keeps a clock that is the highest `now` it has applied and runs each command at the later of its own `now` and that clock, so time in the state machine never goes backwards and every replica, and every replay, agrees on which keys are alive at each log position. `SET k v PX ms` (`SetTTL`) sets a deadline `now + ms`; a plain `SET` clears it, as does a swapping `CAS`; `INCR` keeps it; `DEL` drops it. A key is expired at time `t` once its deadline is at or before `t`, and an expired key behaves as missing for every op: `CAS` sees it as absent, `INCR` starts from 0 with no deadline, `PEXPIRE` returns 0. `PEXPIRE k ms` sets a new deadline on a live key (a `ms` of 0 or less deletes it). Reads on the leader, after the ReadIndex round, evaluate at the later of the wall clock and the state machine clock, so a read never returns an expired key. Memory is reclaimed by the leader: once a second it checks for expired keys and, if there are any, proposes a sweep stamped with its clock; applying it deletes every key due at that time on every replica. A retried `SET ... PX` that reaches the log twice keeps the deadline of the first.
- Known limit of expiry: there is no clock synchronization. After a leader change, keys may live past their deadline by up to the clock skew between the old and new leader (the new leader's clock is behind), and a key can expire early by the same skew if the new leader's clock is ahead. Deadlines are only as accurate as the leaders' clocks.
- Leader loss: the remaining majority elects a new leader within one to two election timeouts (1s by default) and the client retries through the redirect, or redials the other nodes if the leader is gone.
- A leader that hasn't heard from a majority for an election timeout steps down instead of timing out its pending requests. Pending reads get a redirect. Pending writes get "outcome unknown" (a write already in the log may still commit), and the client resends them with the same seq.
- Snapshots: every node snapshots the store after each `--snapshot-entries` applied entries and drops the log before it. The snapshot is fsynced and renamed into place before the log is rewritten, so a crash between the two restarts cleanly, and a follower acks an InstallSnapshot only once both are durable. A restart restores the snapshot and replays only the log after it.
- A node that can no longer persist its log or term halts and the process exits non-zero, rather than serve from a state it can't make durable.

## Go client

```go
c, err := client.Dial("localhost:8081", "localhost:8082", "localhost:8083")
if err != nil {
	return err
}
defer c.Close()
if err := c.Set(ctx, "k", []byte("v")); err != nil {
	return err
}
v, ok, err := c.Get(ctx, "k")
n, err := c.Incr(ctx, "hits", 1)
swapped, err := c.CAS(ctx, "k", []byte("v"), true, []byte("w"))
err = c.SetTTL(ctx, "session", []byte("v"), 30*time.Second) // ttl must be positive
found, err := c.Expire(ctx, "k", time.Minute)                // false if k is missing or expired; ttl <= 0 deletes
left, ok, err := c.TTL(ctx, "k")                             // left 0 means no deadline; ok false if missing or expired
```

A `Client` is safe for concurrent use. It keeps a pool of sessions, each with one connection and one request in flight, so concurrent calls run in parallel and each session's writes still reach the log in order.

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

Server flags: `--id`, `--port` (8080), `--raft-port` (7000), `--metrics-port` (2112), `--data-dir`, `--election-timeout` (1s, heartbeats a tenth of it), `--snapshot-entries` (10000, applied entries between snapshots), `--cluster=id=raft-addr/client-addr,...` listing every member including itself (empty means a cluster of one).

Client:

```sh
go run ./cmd/godis-cli --addr=localhost:8081,localhost:8082,localhost:8083   # REPL
go run ./cmd/godis-cli --addr=localhost:8081 SET k v                         # one-shot
```

Commands: `PING`, `GET k`, `SET k v`, `DEL k`, `CAS k expected new` (sets `k` to `new` only if it holds exactly `expected`; an `expected` of `-` means "only if `k` is absent"; prints 1 if it swapped, 0 if not), `INCR k [delta]` (adds `delta`, default 1, to the base-10 int64 at `k`, a missing key counting as 0, and prints the new value; fails without writing on a value that is not an integer or on overflow), `SET k v PX ms` (sets `k` with a deadline `ms` milliseconds from now), `PEXPIRE k ms` (prints 1 if `k` was live, 0 if not; a `ms` of 0 or less deletes it), `PTTL k` (prints -2 if `k` is missing or expired, -1 if it has no deadline, else the milliseconds left).

Metrics are at `http://<node>:2112/metrics`:

- `godis_requests_total{op,result}`: get, set, del, cas, incr, expire, ttl by ok, not_found, redirect, unknown, error
- `godis_duplicate_writes_total`: retried writes this node's state machine dropped as already applied, since the process started
- `godis_expired_keys_total`: keys this node's state machine deleted in sweeps since the process started
- `godis_request_duration_seconds{op}`: histogram, Raft round trip included
- `godis_raft_term`, `godis_raft_commit_index`, `godis_raft_leader` (1 on the leader)
- `godis_raft_snapshot_index`: last log index this node's snapshot covers, 0 if none
- `godis_raft_leader_changes_total`: times this node learned of a new leader

Plus the Go runtime and process collectors. Leadership transitions are also logged (became leader, stepped down, following leader).

## Wire protocol

Every message is a frame: `uint32 length | payload`, big endian, payloads up to 16 MiB. One request in flight per connection.

- Request: `op` byte (1 PING, 2 GET, 3 SET, 4 DEL, 5 CAS, 6 INCR, 7 EXPIRE, 8 TTL). Writes (SET, DEL, CAS, INCR, EXPIRE) then carry the session, `uvarint client | uvarint seq` (seq starts at 1). Every op but PING then has the key as `uvarint length | bytes`. SET adds the value the same way, then a `uvarint` ttl in ms (0 for none; at most about 100 years); EXPIRE adds the ttl in ms as a signed varint (0 or less deletes); CAS adds a byte (1 if the key must hold the expected value, 0 if it must be absent), the expected value and the new value, both length-prefixed; INCR adds the delta as a signed varint.
- Response: `status` byte (0 OK, 1 NOT_FOUND, 2 ERROR, 3 REDIRECT, 4 UNKNOWN), then the body. OK: the value for a GET, one byte 1 or 0 for whether a CAS swapped, the new value in decimal for an INCR, one byte 1 or 0 for whether an EXPIRE found a live key, for a TTL the `uvarint` milliseconds left (0 for no deadline; NOT_FOUND if the key is missing or expired), empty otherwise. ERROR: the message, for a definite failure (a malformed request, INCR of a non-integer, an expired session): the write did not apply. REDIRECT: the leader's client address, empty when no leader is known (the client waits 100ms and retries); the request never ran. UNKNOWN: the message; the write may or may not apply, and the client resends it with the same seq.

Peers talk to each other over `net/rpc` with gob on the raft port.

## Benchmarks

`go test -run='^$' -bench=. -benchtime=3s ./internal/node/`: a 3-node cluster over loopback in one process, real fsync, 1s election timeout, 128-byte values, client on the leader. Set is sequential with one client, SetParallel runs one client per goroutine (8), Get is a ReadIndex read.

| benchmark | ns/op | ops/s |
|---|---|---|
| Set | 2,909,303 | 344 |
| SetParallel | 1,235,083 | 810 |
| Get | 199,924 | 5,002 |

Intel Core i7-8650U (4 cores, 8 threads), SATA SSD (ext4), Linux 6.8, Go 1.26.0, 2026-10-12, with other work running on the machine (load average around 5). All three nodes share one disk, so every write costs three fsyncs on the same device; a real cluster spreads them. Writes are bound by fsync: each Propose holds the Raft mutex across its own fsync and there is no group commit. Reads cost one heartbeat round each. None of this has been optimized yet.

### Snapshots

One node, 200,000 SETs of 128-byte values over 1,000 keys, data dir on tmpfs, 2026-10-18 on the same laptop. Without compaction the WAL is 30.2 MB and `Start` (replay) takes about 140ms; with the default `--snapshot-entries=10000` the WAL is 1.5 MB, the snapshot 138 KB, and `Start` (restore plus the log after the snapshot) takes about 10ms. A wiped follower in a 3-node cluster catches up by InstallSnapshot in about 64ms over loopback; on tmpfs that is no faster than replaying the whole log by appends, since tmpfs makes the follower's per-batch fsync nearly free.

## Tests

```sh
go test -race ./...
go test -race -count=20 ./internal/consensus/   # flake check for the Raft tests
```

Raft is tested deterministically with `testing/synctest` on an in-memory network with partitions, drops and delays; `internal/node` runs real loopback clusters.

### Linearizability

TTL is not covered by the linearizability checker: it models a register with no notion of time, so the fuzz and the checker never use deadlines, `EXPIRE` or sweeps. Expiry is tested with the store's deterministic unit tests (stamps given by hand) and with real loopback clusters in `internal/node`: expiry on read, the sweep dropping the key from every node, and a deadline surviving a leader kill. The command format still goes through the fuzz, since every write now carries `now`.

`internal/lincheck` is a hand-written linearizability checker: Wing and Gong's search with Lowe's memoization, each key checked as its own register, writes with an unknown outcome allowed to take effect at any point after their call or never. Every test run cross-checks its verdicts against a brute-force search on 20,000 random small histories.

`TestLinearizable` in `internal/consensus` runs 3 or 5 Raft nodes, each feeding a real `storage.Store`, on the in-memory network: 4 clients over 3 keys for 30 to 60 fake seconds, while a seeded nemesis partitions the cluster, deafens the leader (it sends but hears nothing), stops and restarts single nodes or the whole cluster, drops and delays messages, and holds back appends that carry entries. Each run snapshots every 1 to 20 entries (chosen by the seed), so snapshots, log rewrites and InstallSnapshot to stopped or partitioned followers happen throughout. Clients write through sessions: 4 clients mix SET, DEL and CAS over 3 keys and INCR over 2 counter keys, and retry a write whose outcome is unknown with the same `(client, seq)` on other nodes for up to 10 seconds, so an op stays pending only if its client gave up. Each run's history goes through the checker; replaying the applied log through a fresh store must execute each `(client, seq)` once and rebuild exactly the store every node holds (the log itself holds a retried write once per attempt that reached it); and the usual invariants hold (one leader per term, every node applied a prefix of the same log).

```sh
go test -run TestLinearizable ./internal/consensus/ -lin.runs=500   # 3 runs by default
go test -run TestLinearizable ./internal/consensus/ -lin.seed=N     # replay a failing run's fault schedule
```

On 2026-10-17, 2000 runs passed with no failures, about 265,000 operations and 56,000 leader terms in 75 minutes. The fuzz catches the bugs it exists for: with reads served from the commit index instead of a read index confirmed in the leader's term, 13 of 15 runs fail; with a step-down answered by a redirect (the client resends a logged write), 13 of 15 runs apply a write twice; with followers acking before they persist, every run fails.

On 2026-10-18, with snapshots on, 500 runs passed: about 67,000 operations, 19,500 snapshots and 5,000 InstallSnapshots in 20 minutes. The fuzz exercises snapshots but did not by itself catch snapshot bugs reintroduced on purpose (it had no crash fault; see below); unit tests in `internal/consensus/snapshot_test.go` catch each of them (one, a leader that never advances past the snapshot it sent, only by hanging until the test timeout).

On 2026-10-19, with client sessions, CAS and INCR, 500 runs passed in 20 minutes: about 64,000 operations, 41,000 writes, 8,000 retries of writes whose outcome was unknown, 3,600 duplicates dropped by the state machine, 15,000 leader terms, 24,000 snapshots and 6,000 InstallSnapshots. 0.6% of writes ended pending (their client gave up after 10 seconds), down from about 10% before clients retried. With the duplicate check removed from the store, every one of 20 runs at the CI default of 3 seeds fails (60 of 60 seeds: the replay check finds a write that took effect twice, and the checker also rejects 50 of the histories). Comparing seq with `<` instead of `<=` fails 20 of 20 (58 of 60 seeds). Caching an empty result instead of the real one fails 18 of 20 (41 of 60 seeds), caught by the checker only: it shows only when a retried CAS or INCR gets the cached answer.

Each node runs on `vfs.StrictMem` (`internal/vfs`), an in-memory filesystem that keeps only what a real crash keeps: file contents that were fsynced, and directory entries (creates, renames) whose directory was fsynced. The nemesis's crash action freezes a node's filesystem so nothing it does afterwards persists, stops it, reverts the filesystem (unsynced appends keep a random prefix, a torn tail; a rename without a directory fsync is undone) and restarts it later, alone or with the whole cluster. Half of the single-node crashes freeze the filesystem a few writes in the future, so the crash lands inside a snapshot or log rewrite. A node failing to restart after a crash fails the run, as does any persistence error on a node that was not crashed. `TestCrashAtEveryWrite` crashes a lone node after each of 100 possible write counts. On 2026-10-19, 500 runs with crashes passed in 5 minutes: about 59,000 operations, 38,000 writes, 9,000 crashes, 12,000 retries, 14,000 leader terms, 25,000 snapshots and 5,400 InstallSnapshots; 1.2% of writes ended pending. At the CI default of 3 seeds, 20 repetitions each (60 runs), reintroduced bugs fail: follower acks AppendEntries before persisting 20 of 20 repetitions (54 runs); WAL append without fsync 20 of 20 (52 runs); follower acks InstallSnapshot before the snapshot and rewrite are durable 15 of 20 (41 runs); log rewrite without the directory sync 16 of 20 (26 runs); term and vote file without fsync 20 of 20 (60 runs), without the directory sync 16 of 20 (27 runs). Not caught by the fuzz: rewriting the log before the snapshot rename is durable (caught only by `TestCrashAtEveryWrite`), a snapshot rename without its own directory sync (harmless: the log rewrite syncs the same directory before anything is acked), and replay dropping a batch that straddles the snapshot index (loses only unapplied tail entries from one log; unit test only).

A seed reproduces the fault schedule, not every goroutine interleaving (synctest fixes time, not scheduling), so a replay usually but not always fails the same way. `TestLinearizableAcrossRestarts` in `internal/node` checks the real stack the same way (TCP client with its sessions and retries, net/rpc, fsync, all five ops) for 15 seconds while nodes, the leader included, are stopped and restarted; it is skipped under `-short`. It rarely produces a retried write, so it does not catch dedupe being switched off; `TestWriteRetriedAcrossALeaderKillAppliesOnce` does, by killing a leader that has logged an INCR it cannot commit.

## Limitations

- Expiry trusts the leaders' clocks (see Guarantees); there is no lease or clock sync. Sweeps scan every key with a deadline once a second, a min-heap would be the fix if keys with TTLs get numerous.
- A snapshot is one in-memory blob sent in a single RPC, with no chunking, so the whole store must fit in memory twice and in one gob message.
- Taking a snapshot encodes the store under its read lock, which blocks writes to the store (and so applying) for that long.
- The session table is capped at 100,000 clients. Past the cap it evicts the tenth with the oldest last write, by log index, so every replica evicts the same ones. A write with seq above 1 from a client with no session fails with "session expired", which covers an evicted client's retries except one case: a seq 1 write retried after its client was evicted applies twice. Sessions never expire by time.
- Exactly-once holds only for writes sent through the Go client, one at a time per client; a client that crashes and restarts is a new session.
- One heartbeat round per read; concurrent reads are not batched.
- A follower holds the Raft mutex across the fsync of a big append or a received snapshot, so a heartbeat arriving meanwhile waits for it.
- A corrupt log is not repaired: wipe that node's data dir by hand and let it catch up.
- No membership changes: the cluster is fixed at startup.
- No pre-vote, so a node rejoining after a partition bumps the term and forces an election.
