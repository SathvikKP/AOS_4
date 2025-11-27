#!/bin/bash
#/bin/bash

set -e

usage() {
        cat <<EOF
Usage: $0 <scenario>

Scenarios:
    original     Run the original manual demo flow (manager + 2 storages + clients)
    single       Smoke test with 2 nodes, RF=2, and three single_set_get clients
    cli_demo     Run spec-style CLI commands using ./bin/client
    test1        Official Test 1 trace (1 node, RF=1)
    test2        Official Test 2 trace (5 nodes, RF=3)
    test3        Failure test with single node kill (3 nodes, RF=2)
    test4        Failure test with two node kills (7 nodes, RF=3)
    throughput   Performance test (200k ops, RF 1/3/5)
    load         Load-balance histogram test (100k inserts)
    suite        Run tests 1-4, throughput, and load in sequence with per-test log archives

Options:
    -h, --help   Show this message and exit
EOF
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        usage
        exit 0
fi

SCENARIO="$1"

run_original_demo() {
    make clean
    make

    ./bin/manager &
    sleep 5

    ./bin/storage &
    sleep 5
    ./bin/storage &
    sleep 5

    ./bin/test_app single_set_get 1 &
    ./bin/test_app single_set_get 2 &
    ./bin/test_app single_set_get 3

    pkill -f "bin/test_app single_set_get" >/dev/null 2>&1 || true
    pkill -f bin/storage >/dev/null 2>&1 || true
    pkill -f bin/manager >/dev/null 2>&1 || true
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
START_SCRIPT="$ROOT_DIR/start_service"
STOP_SCRIPT="$ROOT_DIR/stop_service"
PID_FILE="$SCRIPT_DIR/service_pids.txt"
TP_OPS=${GTSTORE_TP_OPS:-200000}
LB_INSERTS=${GTSTORE_LB_INSERTS:-100000}
THROUGHPUT_FILE="$SCRIPT_DIR/logs/perf_throughput.csv"
LOAD_FILE="$SCRIPT_DIR/logs/perf_loadbalance.csv"
LOG_ARCHIVE_DIR="$SCRIPT_DIR/log_archives"

mkdir -p "$SCRIPT_DIR/logs"
rm -f "$SCRIPT_DIR"/logs/* >/dev/null 2>&1 || true

start_cluster() {
        "$START_SCRIPT" --nodes "$1" --rep "$2"
}

cleanup() {
        "$STOP_SCRIPT" >/dev/null 2>&1 || true
}

trap cleanup EXIT

kill_storage() {
        if [[ ! -f "$PID_FILE" ]]; then
                echo "No pid file found for killing storage"
                return 1
        fi
        local index="$1"
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

    archive_logs() {
        local scenario="$1"
        local target_root="${2:-$LOG_ARCHIVE_DIR}"
        if [[ ! -d "$SCRIPT_DIR/logs" ]]; then
            echo "No logs directory to archive for $scenario"
            return 0
        fi
        mkdir -p "$target_root"
        local stamp
        stamp=$(date +%Y%m%d_%H%M%S)
        local dest="$target_root/${stamp}_${scenario}"
        mkdir -p "$dest"
        cp -r "$SCRIPT_DIR/logs/." "$dest/" >/dev/null 2>&1 || true
        echo "Archived logs to $dest"
    }

    run_throughput() {
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

    run_load_balance() {
        echo "node_id,count" > "$LOAD_FILE"
        start_cluster 7 1
        sleep 3
        GTSTORE_PERF_FILE="$LOAD_FILE" ./bin/test_app load_balance 600 "$LB_INSERTS"
        cleanup
        echo "Load-balance suite completed. CSV: $LOAD_FILE"
    }

    run_named_scenario() {
        local scenario="$1"
        case "$scenario" in
            original)
                run_original_demo
                ;;
            cli_demo)
                start_cluster 3 2
                sleep 3
                ./bin/client --put key1 --val value1
                ./bin/client --get key1
                ./bin/client --put key1 --val value2
                ./bin/client --get key1
                ./bin/client --put key2 --val value3
                ./bin/client --get key2
                ./bin/client --put key3 --val value4
                ./bin/client --get key3
                ;;
            single)
                start_cluster 2 2
                sleep 3
                ./bin/test_app single_set_get 1 &
                ./bin/test_app single_set_get 2 &
                ./bin/test_app single_set_get 3 &
                wait
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
                start_cluster 3 2
                sleep 3
                ./bin/test_app failure_load 301
                sleep 2
                kill_storage 0
                sleep 7
                ./bin/test_app failure_verify 302
                ;;
            test4)
                start_cluster 7 3
                sleep 3
                ./bin/test_app multi_failure_load 401
                sleep 2
                kill_storage 0
                kill_storage 1
                sleep 7
                ./bin/test_app multi_failure_verify 402
                ;;
            throughput)
                run_throughput
                ;;
            load)
                run_load_balance
                ;;
            *)
                return 1
                ;;
        esac
        return 0
    }

    run_suite() {
        local suite_root="$LOG_ARCHIVE_DIR/suite_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$suite_root"
        local scenarios=(test1 test2 test3 test4 throughput load)
        for scenario in "${scenarios[@]}"; do
            echo "==== Running $scenario ===="
            if ! run_named_scenario "$scenario"; then
                echo "Failed to run scenario $scenario"
                return 1
            fi
            cleanup
            archive_logs "$scenario" "$suite_root"
            sleep 2
        done
        echo "Full suite completed. Log archives stored under $suite_root"
    }

if [[ "$SCENARIO" == "suite" ]]; then
        if ! run_suite; then
                echo "Suite run failed"
                exit 1
        fi
        exit 0
fi

if ! run_named_scenario "$SCENARIO"; then
        echo "Unknown scenario: $SCENARIO"
        usage
        exit 1
fi

echo "Scenario $SCENARIO completed. Logs: $SCRIPT_DIR/logs"

