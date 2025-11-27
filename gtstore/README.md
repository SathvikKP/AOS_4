# GT Store

GT Store is a distributed key/value store built around a centralized manager, multiple replicated storage nodes, and a reusable C++ client. This README describes everything you need to build the binaries, launch the service, drive functional/perf tests, and collect the resulting logs.

## Build the binaries

```bash
cd gtstore
make            # builds bin/manager, bin/storage, bin/client, bin/test_app
make clean      # optional: remove artifacts
```

`make client` alone is enough if you only need the CLI binary, but the default target is fast and always up to date.

## Key components

- `bin/manager` – central controller that tracks storage membership, generates the consistent-hash ring (128 virtual nodes per member), and pushes routing tables to clients.
- `bin/storage` – storage server that owns multiple hash ranges, persists data in-memory, replicates PUTs to peers, and answers GETs.
- `bin/client` – simple CLI (`--put/--get`) that links against `GTStoreClient` and mirrors the format required in the project spec.
- `bin/test_app` – scripted driver used by `run.sh` to replay spec traces, run fault-injection tests, and gather performance metrics.
- `run.sh` – orchestration script that starts/stops clusters with `start_service`/`stop_service`, launches clients, and (for the full suite) archives every log bundle.

## Starting and stopping the service manually

From the project root (one level up from this README):

```bash
./start_service --nodes <N> --rep <K>
```

- Launches the manager (listening on `<DEFAULT_MANAGER_HOST>:<DEFAULT_MANAGER_PORT>` from `gtstore.hpp`).
- Spawns `N` storage processes labeled `node1`, `node2`, …
- Sets `GTSTORE_REPL=K` so every process agrees on the replication factor.
- Streams logs to `gtstore/logs` (manager + storages) and prints the PIDs + log location.

To shut everything down cleanly:

```bash
./stop_service
```

Both scripts are idempotent; running `stop_service` when nothing is up is safe.

## Using the CLI client directly

Run commands from `gtstore/` (or supply absolute paths):

```bash
./bin/client --put key1 --val value1                  # prints "OK, serverX"
./bin/client --get key1                               # prints "key1, value1, serverX"
./bin/client --put key2 --val value2 --manager-host 127.0.0.1 --manager-port 6606
```

Flags:
- `--put <key> --val <value>` performs a replicated PUT.
- `--get <key>` fetches the latest value from any replica.
- `--manager-host/--manager-port` override the default manager endpoint (defaults come from `DEFAULT_MANAGER_HOST/PORT`).
- `-h/--help` prints usage.

Every invocation performs `init -> (get|put) -> finalize`, so the CLI is safe to use repeatedly while a cluster is running. The `run.sh cli_demo` scenario replays the spec’s exact CLI trace end-to-end.

## Driving predefined scenarios with run.sh

`run.sh` handles compilation, cluster lifecycle, and logging for all supported tests. Pass an explicit scenario name (there is no default):

| Scenario      | Description |
|---------------|-------------|
| `original`    | Legacy manual demo (manager + 2 storages + 3 clients launched directly).
| `single`      | Smoke test with 2 nodes, RF=2, and three `single_set_get` clients.
| `cli_demo`    | Runs the CLI spec trace (`./bin/client --put/--get ...`).
| `test1`/`test2` | Official spec traces for single-node and multi-node GET/PUT.
| `test3`       | Single-node failure handling (kill one storage, verify reads succeed).
| `test4`       | Multi-node failure handling (kill two storages, verify reads succeed).
| `throughput`  | 200k mixed ops on 7 nodes at RF=1/3/5; results land in `logs/perf_throughput.csv`.
| `load`        | 100k inserts on 7 nodes at RF=1; histogram saved to `logs/perf_loadbalance.csv`.
| `suite`       | **New**: runs Tests 1–4 plus `throughput` and `load` sequentially, archiving each run’s logs before starting the next.

Examples:

```bash
./run.sh --help       # print the scenario list
./run.sh test1        # single-node GET/PUT trace
./run.sh test3        # kill/recovery scenario
./run.sh throughput   # emits CSV with ops/sec per replication factor
./run.sh suite        # executes tests 1-4 + throughput + load, archiving logs each time
```

The suite command copies the `gtstore/logs` directory into `gtstore/log_archives/suite_<timestamp>/` after every test (`.../<timestamp>_<scenario>`), so you can inspect each test’s artifacts even after the next test runs.

## Logs, artifacts, and environment knobs

- Live logs: `gtstore/logs/manager.log`, `gtstore/logs/storage_<n>.log`, plus any CSVs created by performance tests.
- Archived logs: `gtstore/log_archives/` (populated automatically by `run.sh suite`). Feel free to copy additional runs there manually.
- Performance overrides: set `GTSTORE_TP_OPS` to change the throughput test length (default 200k ops) or `GTSTORE_LB_INSERTS` to change the load-balance insert count (default 100k) before invoking `run.sh`.

## Troubleshooting tips

- If a previous service crashed, run `./stop_service` to clean up stray processes before starting new tests.
- `make clean && make -j` resolves most stale binary issues.
- All commands emit rich logs; inspect the relevant file under `gtstore/logs` (or the archived copy) whenever a test fails or hangs.
