# GT Store

GT Store is a small distributed key/value store with a centralized manager and multiple storage nodes. All binaries and scripts live inside this `gtstore/` directory.

## 1. Build

```bash
cd gtstore
make
```
The Makefile builds `bin/manager`, `bin/storage`, `bin/client`, and `bin/test_app`. Run `make clean` to remove binaries before packaging.

## 2. Start the service

Use the helper scripts in the project root:
```bash
./start_service --nodes <N> --rep <K>
```
- Launches the manager on port 5000.
- Starts `N` storage processes with labels `node1`, `node2`, …
- Sets `GTSTORE_REPL=K` so the manager and clients agree on the replication factor.

To stop everything:
```bash
./stop_service
```
All logs are under `gtstore/logs/` and get cleared by `run.sh` before each scenario.

## 3. Run scenarios

`run.sh` ties everything together:
```bash
./run.sh single        # smoke test
./run.sh test1         # spec Test 1 trace
./run.sh test2         # spec Test 2 trace
./run.sh test3         # single failure
./run.sh test4         # multiple failures
./run.sh throughput    # performance ops/sec
./run.sh load          # load-balance histogram
```
Each run produces console output plus log files and CSVs for the report.

## 4. How the system works (short)
- **Manager (`bin/manager`)** keeps the membership list, hashes each storage node into a consistent-hash ring, and serves routing tables to clients. Storage nodes send heartbeats every two seconds; the manager removes nodes that miss three heartbeats.
- **Storage nodes (`bin/storage`)** hold key/value pairs in memory, handle `CLIENT_PUT` and `CLIENT_GET`, and log a full snapshot of their map after every write.
- **Client library (`GTStoreClient`)** hashes keys, selects the first `k` successors on the ring, and fan-out writes to every replica so reads always fetch the latest value. Failed connections trigger a table refresh.
- **Driver (`bin/test_app`)** exercises the API for Tests 1–4 and gathers throughput/load-balance metrics.

Keys are limited to 20 bytes, values to 1 KB, and all communication uses length-prefixed TCP messages defined in `net_common.*`.
