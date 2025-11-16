#!/bin/bash
#/bin/bash

make clean
make

# Launch the GTStore Manager
./bin/manager &
sleep 5

# Launch couple GTStore Storage Nodes
./bin/storage &
sleep 5
./bin/storage &
sleep 5

# Launch the client testing app
# Usage: ./test_app <test> <client_id>
./bin/test_app single_set_get 1 &
./bin/test_app single_set_get 2 &
./bin/test_app single_set_get 3

# Ensure legacy background processes do not leak into automated scenarios
pkill -f "bin/test_app single_set_get" >/dev/null 2>&1 || true
pkill -f bin/storage >/dev/null 2>&1 || true
pkill -f bin/manager >/dev/null 2>&1 || true

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
START_SCRIPT="$ROOT_DIR/start_service"
STOP_SCRIPT="$ROOT_DIR/stop_service"
PID_FILE="$SCRIPT_DIR/service_pids.txt"
SCENARIO="${1:-single}"
TP_OPS=${GTSTORE_TP_OPS:-200000}
LB_INSERTS=${GTSTORE_LB_INSERTS:-100000}
THROUGHPUT_FILE="$SCRIPT_DIR/logs/perf_throughput.csv"
LOAD_FILE="$SCRIPT_DIR/logs/perf_loadbalance.csv"

mkdir -p "$SCRIPT_DIR/logs"

start_cluster() {
    local nodes="$1"
    local rep="$2"
    "$START_SCRIPT" --nodes "$nodes" --rep "$rep"
}

cleanup() {
    "$STOP_SCRIPT" >/dev/null 2>&1 || true
}

trap cleanup EXIT

kill_storage() {
    local index="$1"
    if [[ ! -f "$PID_FILE" ]]; then
        echo "No pid file found for killing storage"
        return 1
    fi
    local line=$((index + 2))
    local pid
    pid=$(sed -n "${line}p" "$PID_FILE")
    if [[ -z "$pid" ]]; then
        echo "Storage index $index not found in pid file"
        return 1
    fi
    if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
        echo "Killed storage index $index (pid $pid)"
    fi
}

run_single_clients() {
    ./bin/test_app single_set_get 1 &
    ./bin/test_app single_set_get 2 &
    ./bin/test_app single_set_get 3 &
    wait
}

run_test3() {
    start_cluster 3 2
    sleep 3
    ./bin/test_app failure_load 301
    sleep 2
    kill_storage 0
    sleep 7
    ./bin/test_app failure_verify 302
}

run_test4() {
    start_cluster 7 3
    sleep 3
    ./bin/test_app multi_failure_load 401
    sleep 2
    kill_storage 0
    kill_storage 1
    sleep 7
    ./bin/test_app multi_failure_verify 402
}

run_throughput_suite() {
    echo "replicas,ops,seconds,ops_per_sec" > "$THROUGHPUT_FILE"
    for rep in 1 3 5; do
        start_cluster 7 "$rep"
        sleep 3
        GTSTORE_PERF_FILE="$THROUGHPUT_FILE" ./bin/test_app throughput 500 "$TP_OPS"
        cleanup
        sleep 2
    done
    echo "Throughput suite completed. CSV: $THROUGHPUT_FILE"
}

run_load_suite() {
    echo "node_id,count" > "$LOAD_FILE"
    start_cluster 7 1
    sleep 3
    GTSTORE_PERF_FILE="$LOAD_FILE" ./bin/test_app load_balance 600 "$LB_INSERTS"
    cleanup
    echo "Load-balance suite completed. CSV: $LOAD_FILE"
}

case "$SCENARIO" in
    single)
        start_cluster 2 2
        sleep 3
        run_single_clients
        ;;
    test1)
        start_cluster 1 1
        sleep 3
        ./bin/test_app basic_trace 101
        ;;
    test2)
        start_cluster 5 3
        sleep 3
        ./bin/test_app basic_trace 202
        ;;
    test3)
        run_test3
        ;;
    test4)
        run_test4
        ;;
    throughput)
        run_throughput_suite
        exit 0
        ;;
    load)
        run_load_suite
        exit 0
        ;;
    *)
        echo "Usage: $0 [single|test1|test2|test3|test4|throughput|load]"
        exit 1
        ;;
esac

echo "Scenario $SCENARIO completed. Logs: $SCRIPT_DIR/logs"

