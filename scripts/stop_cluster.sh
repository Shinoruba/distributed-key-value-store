#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PID_FILE="$ROOT_DIR/data/cluster.pids"

echo "Stopping DistributedKVStore Cluster..."

if [ -f "$PID_FILE" ]; then
    while IFS= read -r pid; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null && echo "  [Stopped] Process PID $pid" || true
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

pkill -f "kvstore-server" 2>/dev/null || true

echo "Cluster stopped successfully."