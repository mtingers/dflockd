#!/usr/bin/env bash
# Fault hook for cmd/cluster-soak on dedicated Linux/systemd hosts.
#
# Required environment:
#   DFLOCKD_SOAK_SSH_TARGETS="a=ops@host-a,b=ops@host-b,c=ops@host-c"
#   DFLOCKD_SOAK_RAFT_ADDRS="a=10.0.0.1:7000,b=10.0.0.2:7000,c=10.0.0.3:7000"
#
# Optional:
#   DFLOCKD_SOAK_SERVICE="dflockd.service"
#
# The SSH user needs permission to run `sudo -n bash`. The hook changes
# only Raft-port packet filtering and a process-local dflockd test
# offset; it never changes the host clock.
set -euo pipefail
set -f

usage() {
  echo "usage: $0 {partition|heal|restart|skew|unskew} NODE [OFFSET]" >&2
  exit 2
}

lookup_value() {
  local mapping=$1 wanted=$2 item id value
  local old_ifs=$IFS
  IFS=,
  for item in $mapping; do
    id=${item%%=*}
    value=${item#*=}
    if [[ "$id" == "$wanted" && "$value" != "$item" ]]; then
      IFS=$old_ifs
      printf '%s\n' "$value"
      return 0
    fi
  done
  IFS=$old_ifs
  return 1
}

validate_node_id() {
  [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]
}

validate_ssh_target() {
  [[ "$1" =~ ^[A-Za-z0-9._@-]+$ ]]
}

validate_service() {
  [[ "$1" =~ ^[A-Za-z0-9._@-]+$ ]]
}

validate_offset() {
  [[ "$1" =~ ^-?([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$ ]]
}

validate_raft_addr() {
  local addr=$1 ip port
  ip=${addr%:*}
  port=${addr##*:}
  [[ "$ip" =~ ^[0-9.]+$ && "$port" =~ ^[0-9]+$ ]]
}

[[ $# -ge 2 ]] || usage
action=$1
node=$2
offset=${3:-}

case "$action" in
  partition|heal|restart|unskew) [[ $# -eq 2 ]] || usage ;;
  skew) [[ $# -eq 3 ]] || usage ;;
  *) usage ;;
esac

validate_node_id "$node" || { echo "invalid node id: $node" >&2; exit 2; }
ssh_targets=${DFLOCKD_SOAK_SSH_TARGETS:?DFLOCKD_SOAK_SSH_TARGETS is required}
raft_addrs=${DFLOCKD_SOAK_RAFT_ADDRS:?DFLOCKD_SOAK_RAFT_ADDRS is required}
service=${DFLOCKD_SOAK_SERVICE:-dflockd.service}
validate_service "$service" || { echo "invalid service: $service" >&2; exit 2; }
[[ "$service" == *.service ]] || service="${service}.service"

ssh_target=$(lookup_value "$ssh_targets" "$node") ||
  { echo "missing SSH target for node $node" >&2; exit 2; }
validate_ssh_target "$ssh_target" ||
  { echo "invalid SSH target for node $node" >&2; exit 2; }

own_addr=$(lookup_value "$raft_addrs" "$node") ||
  { echo "missing Raft address for node $node" >&2; exit 2; }
validate_raft_addr "$own_addr" ||
  { echo "invalid IPv4 Raft address for node $node: $own_addr" >&2; exit 2; }
own_ip=${own_addr%:*}
own_port=${own_addr##*:}

if [[ "$action" == "skew" ]]; then
  validate_offset "$offset" || { echo "invalid clock offset: $offset" >&2; exit 2; }
fi

peer_args=()
old_ifs=$IFS
IFS=,
for item in $raft_addrs; do
  peer_id=${item%%=*}
  peer_addr=${item#*=}
  validate_node_id "$peer_id" && validate_raft_addr "$peer_addr" ||
    { echo "invalid Raft mapping: $item" >&2; exit 2; }
  if [[ "$peer_id" != "$node" ]]; then
    peer_args+=("${peer_addr%:*}" "${peer_addr##*:}")
  fi
done
IFS=$old_ifs

ssh -o BatchMode=yes -o ConnectTimeout=10 "$ssh_target" \
  sudo -n bash -s -- "$action" "$own_ip" "$own_port" "$service" "$offset" \
  "${peer_args[@]}" <<'REMOTE'
set -euo pipefail

action=$1
own_ip=$2
own_port=$3
service=$4
offset=$5
shift 5

in_chain=DFLOCKD_SOAK_IN
out_chain=DFLOCKD_SOAK_OUT

ensure_chains() {
  iptables -N "$in_chain" 2>/dev/null || true
  iptables -N "$out_chain" 2>/dev/null || true
  iptables -C INPUT -j "$in_chain" 2>/dev/null ||
    iptables -I INPUT 1 -j "$in_chain"
  iptables -C OUTPUT -j "$out_chain" 2>/dev/null ||
    iptables -I OUTPUT 1 -j "$out_chain"
}

remove_chains() {
  iptables -D INPUT -j "$in_chain" 2>/dev/null || true
  iptables -D OUTPUT -j "$out_chain" 2>/dev/null || true
  iptables -F "$in_chain" 2>/dev/null || true
  iptables -F "$out_chain" 2>/dev/null || true
  iptables -X "$in_chain" 2>/dev/null || true
  iptables -X "$out_chain" 2>/dev/null || true
}

case "$action" in
  partition)
    command -v iptables >/dev/null
    ensure_chains
    iptables -F "$in_chain"
    iptables -F "$out_chain"
    iptables -A "$in_chain" -p tcp -d "$own_ip" --dport "$own_port" -j DROP
    while [[ $# -gt 0 ]]; do
      peer_ip=$1
      peer_port=$2
      shift 2
      iptables -A "$out_chain" -p tcp -d "$peer_ip" --dport "$peer_port" -j DROP
    done
    ;;
  heal)
    command -v iptables >/dev/null
    remove_chains
    ;;
  restart)
    systemctl restart "$service"
    ;;
  skew)
    dropin="/run/systemd/system/${service}.d"
    install -d -m 0755 "$dropin"
    printf '%s\n' \
      '[Service]' \
      "Environment=DFLOCKD_UNSAFE_TEST_CLOCK_OFFSET=$offset" \
      >"$dropin/soak-clock.conf"
    systemctl daemon-reload
    systemctl restart "$service"
    ;;
  unskew)
    rm -f "/run/systemd/system/${service}.d/soak-clock.conf"
    systemctl daemon-reload
    systemctl restart "$service"
    ;;
esac
REMOTE
