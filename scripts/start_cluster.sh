#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
BIN_PATH="$ROOT_DIR/build/DistributedKVStore/kvstore-server"

if [ ! -f "$BIN_PATH" ]; then
    BIN_PATH="$ROOT_DIR/build/DistributedKVStore/Release/kvstore-server"
fi

if [ ! -f "$BIN_PATH" ]; then
    echo "Error: kvstore-server binary not found. Please build the project first."
    exit 1
fi

DATA_DIR="$ROOT_DIR/data"
mkdir -p "$DATA_DIR"

PEERS_1="node_2=127.0.0.1:7081,node_3=127.0.0.1:7082"
PEERS_2="node_1=127.0.0.1:7080,node_3=127.0.0.1:7082"
PEERS_3="node_1=127.0.0.1:7080,node_2=127.0.0.1:7081"

echo "Starting DistributedKVStore 3-Node Cluster..."

"$BIN_PATH" --id node_1 --port 6380 --raft-port 7080 --peers "$PEERS_1" --wal "$DATA_DIR/node1.wal" > "$DATA_DIR/node1.log" 2>&1 &
PID1=$!
echo "  [Started] Node 1 (Client: 6380, Raft: 7080, PID: $PID1)"

"$BIN_PATH" --id node_2 --port 6381 --raft-port 7081 --peers "$PEERS_2" --wal "$DATA_DIR/node2.wal" > "$DATA_DIR/node2.log" 2>&1 &
PID2=$!
echo "  [Started] Node 2 (Client: 6381, Raft: 7081, PID: $PID2)"

"$BIN_PATH" --id node_3 --port 6382 --raft-port 7082 --peers "$PEERS_3" --wal "$DATA_DIR/node3.wal" > "$DATA_DIR/node3.log" 2>&1 &
PID3=$!
echo "  [Started] Node 3 (Client: 6382, Raft: 7082, PID: $PID3)"

echo -e "$PID1\n$PID2\n$PID3" > "$DATA_DIR/cluster.pids"

echo ""
echo "Cluster successfully started!"
echo "Connect via CLI: ./build/DistributedKVStore/kvstore-cli --port 6380"