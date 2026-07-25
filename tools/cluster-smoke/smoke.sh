#!/bin/bash
# tools/cluster-smoke — real-binary 3-node cluster smoke test.
#
# Builds dflockd from the working tree, launches three processes on
# loopback with distinct --raft-dir / --raft-addr / --port, exercises:
#
#   1. Leader election (sit until exactly one node accepts mutations).
#   2. Follower redirect: every non-leader returns
#      error_not_leader <leaderClientAddr>.
#   3. Acquire + Release over a single persistent TCP session on the
#      leader.
#   4. Hard-crash the leader with kill -9; another node must take over.
#   5. Acquire + Release on the new leader.
#   6. Run the external cluster-soak workload through all three targets.
#
# Run from the repo root: bash tools/cluster-smoke/smoke.sh
# Set DFLOCKD_SMOKE_TLS=1 to run the same scenario with mutual TLS on
# the Raft transport (needs `openssl`).
set -uo pipefail

BIN=/tmp/dflockd-smoke
SOAK_BIN=/tmp/dflockd-cluster-soak-smoke
DIR=/tmp/dflockd-smoke-data
LOGS=/tmp/dflockd-smoke-logs

cleanup() {
  local pids
  pids=$(jobs -p 2>/dev/null)
  if [[ -n "$pids" ]]; then kill -9 $pids 2>/dev/null || true; fi
  # Belt-and-suspenders: kill any leftover smoke processes by name.
  pkill -9 -f "$BIN" 2>/dev/null || true
  rm -rf "$DIR" "$BIN" "$SOAK_BIN"
  # Logs are left in $LOGS for inspection.
}
trap cleanup EXIT

echo "==> building dflockd"
go build -o "$BIN" ./cmd/dflockd || { echo "build failed"; exit 1; }
go build -o "$SOAK_BIN" ./cmd/cluster-soak || { echo "cluster-soak build failed"; exit 1; }
echo "    binary: $BIN ($(wc -c <"$BIN") bytes)"

A_RAFT=17101 A_CLIENT=17201
B_RAFT=17102 B_CLIENT=17202
C_RAFT=17103 C_CLIENT=17203
PEERS="a=127.0.0.1:$A_RAFT@127.0.0.1:$A_CLIENT,b=127.0.0.1:$B_RAFT@127.0.0.1:$B_CLIENT,c=127.0.0.1:$C_RAFT@127.0.0.1:$C_CLIENT"

mkdir -p "$DIR/a" "$DIR/b" "$DIR/c" "$LOGS"
RAFT_SECRET_FILE="$DIR/raft-auth-token"
printf '%s\n' "0123456789abcdef0123456789abcdef" >"$RAFT_SECRET_FILE"
chmod 600 "$RAFT_SECRET_FILE"

# Optional: DFLOCKD_SMOKE_TLS=1 exercises the mutual-TLS Raft transport.
# Each node gets a distinct self-signed cert whose Common Name is its
# NodeID; the concatenated bundle trusts all three leaves.
if [[ "${DFLOCKD_SMOKE_TLS:-}" == "1" ]]; then
  echo "==> generating per-node certs for mutual TLS"
  for id in a b c; do
    openssl req -x509 -newkey rsa:2048 \
      -keyout "$DIR/$id-key.pem" -out "$DIR/$id-cert.pem" -nodes -days 1 \
      -subj "/CN=$id" \
      -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
      -addext "extendedKeyUsage=serverAuth,clientAuth" \
      >/dev/null 2>&1 || { echo "openssl cert gen failed"; exit 1; }
  done
  cat "$DIR/a-cert.pem" "$DIR/b-cert.pem" "$DIR/c-cert.pem" >"$DIR/raft-ca.pem"
fi

start_node() {
  local id=$1 raft=$2 client=$3
  local tls_args=()
  if [[ "${DFLOCKD_SMOKE_TLS:-}" == "1" ]]; then
    tls_args=(--raft-tls-cert "$DIR/$id-cert.pem" --raft-tls-key "$DIR/$id-key.pem" --raft-tls-ca "$DIR/raft-ca.pem")
  fi
  # ${arr[@]+"${arr[@]}"} expands to nothing when arr is empty — and is
  # safe under `set -u` on the bash 3.2 that ships with macOS.
  "$BIN" --raft-dir "$DIR/$id" --node-id "$id" \
    --raft-addr "127.0.0.1:$raft" --advertise-addr "127.0.0.1:$client" \
    --raft-auth-token-file "$RAFT_SECRET_FILE" \
    --port "$client" --host 127.0.0.1 --cluster-peers "$PEERS" \
    --default-lease-ttl 60 --orphan-ttl 10 \
    ${tls_args[@]+"${tls_args[@]}"} \
    >"$LOGS/$id.log" 2>&1 &
  echo $!
}

# One-shot: open conn, send three lines, read one response, close.
# (Each one-shot triggers CleanupConn on success — that's fine for the
# probe/redirect checks; for "acquire then release" we use a persistent
# session below.)
send_one() {
  local port=$1 cmd=$2 key=$3 arg=$4
  python3 - "$port" "$cmd" "$key" "$arg" <<'PYEOF'
import socket, sys
port = int(sys.argv[1]); cmd, key, arg = sys.argv[2], sys.argv[3], sys.argv[4]
s = socket.create_connection(("127.0.0.1", port), 2)
s.sendall(f"{cmd}\n{key}\n{arg}\n".encode())
buf = b""
while not buf.endswith(b"\n"):
    chunk = s.recv(4096)
    if not chunk: break
    buf += chunk
print(buf.decode().rstrip())
PYEOF
}

find_leader() {
  local probe_key="probe-$RANDOM-$$"
  for n in a b c; do
    case "$n" in a) port=$A_CLIENT;; b) port=$B_CLIENT;; c) port=$C_CLIENT;; esac
    resp=$(send_one "$port" "l" "$probe_key" "1 60" 2>/dev/null || echo "ERR")
    case "$resp" in
      "ok "*) echo "$n $port"; return 0;;
    esac
  done
  return 1
}

echo "==> launching 3 nodes"
PA=$(start_node a $A_RAFT $A_CLIENT)
PB=$(start_node b $B_RAFT $B_CLIENT)
PC=$(start_node c $C_RAFT $C_CLIENT)
echo "    pids: a=$PA b=$PB c=$PC"

echo "==> step 1: wait for a leader"
LEADER_NODE=""
for _ in $(seq 1 50); do
  if leader=$(find_leader); then
    LEADER_NODE=$(echo $leader | awk '{print $1}')
    LEADER_PORT=$(echo $leader | awk '{print $2}')
    echo "    leader: $LEADER_NODE port=$LEADER_PORT"
    break
  fi
  sleep 0.1
done
[[ -z "$LEADER_NODE" ]] && { echo "FAIL: no leader emerged"; exit 1; }

echo "==> step 2: each follower redirects mutating ops to the leader"
for n in a b c; do
  [[ "$n" == "$LEADER_NODE" ]] && continue
  case "$n" in a) port=$A_CLIENT;; b) port=$B_CLIENT;; c) port=$C_CLIENT;; esac
  resp=$(send_one "$port" "l" "redirect-probe" "1 60")
  case "$resp" in
    "error_not_leader 127.0.0.1:$LEADER_PORT") echo "    node $n -> leader 127.0.0.1:$LEADER_PORT (correct)";;
    *) echo "FAIL node $n redirect: $resp"; exit 1;;
  esac
done

echo "==> step 3: persistent session acquire + release on the leader"
python3 - "$LEADER_PORT" <<'PYEOF'
import socket, sys
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), 2)
def rpc(cmd, key, arg):
    s.sendall(f"{cmd}\n{key}\n{arg}\n".encode())
    buf = b""
    while not buf.endswith(b"\n"):
        c = s.recv(4096)
        if not c: break
        buf += c
    return buf.decode().rstrip()
r1 = rpc("l", "kSession", "1 60")
assert r1.startswith("ok "), f"acquire: {r1}"
tok = r1.split()[1]
r2 = rpc("r", "kSession", tok)
assert r2 == "ok", f"release: {r2}"
print(f"    acquire ok (token {tok[:16]}...), release ok")
PYEOF

echo "==> step 4: hard-crash leader $LEADER_NODE (SIGKILL)"
case "$LEADER_NODE" in a) kill -9 $PA;; b) kill -9 $PB;; c) kill -9 $PC;; esac

NEW_LEADER=""
for _ in $(seq 1 80); do
  if leader=$(find_leader); then
    NEW_LEADER=$(echo $leader | awk '{print $1}')
    NEW_PORT=$(echo $leader | awk '{print $2}')
    if [[ "$NEW_LEADER" != "$LEADER_NODE" ]]; then
      echo "    new leader: $NEW_LEADER port=$NEW_PORT"
      break
    fi
  fi
  sleep 0.1
done
[[ -z "$NEW_LEADER" || "$NEW_LEADER" == "$LEADER_NODE" ]] && { echo "FAIL: no new leader after crash"; exit 1; }

echo "==> step 5: post-failover write path on the new leader"
python3 - "$NEW_PORT" <<'PYEOF'
import socket, sys
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), 2)
def rpc(cmd, key, arg):
    s.sendall(f"{cmd}\n{key}\n{arg}\n".encode())
    buf = b""
    while not buf.endswith(b"\n"):
        c = s.recv(4096)
        if not c: break
        buf += c
    return buf.decode().rstrip()
r1 = rpc("l", "kPost", "1 60")
assert r1.startswith("ok "), f"acquire post-failover: {r1}"
tok = r1.split()[1]
r2 = rpc("r", "kPost", tok)
assert r2 == "ok", f"release post-failover: {r2}"
print(f"    post-failover acquire ok (token {tok[:16]}...), release ok")
PYEOF

echo "==> step 6: external soak workload routes around the crashed member"
"$SOAK_BIN" \
  --targets "a=127.0.0.1:$A_CLIENT,b=127.0.0.1:$B_CLIENT,c=127.0.0.1:$C_CLIENT" \
  --workers 2 --duration 2s --fault-interval 0 --lease-ttl 5s --redirect-budget 6 ||
  { echo "FAIL: external cluster soak"; exit 1; }

echo "==> ALL SMOKE STEPS PASSED"
