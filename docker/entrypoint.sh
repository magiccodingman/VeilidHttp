#!/bin/sh
set -eu

mkdir -p \
  /data/veilid/protected-store \
  /data/veilid/table-store \
  /data/veilid/block-store \
  /data/bridge/route \
  /data/bridge/transfers \
  /data/bridge/completed \
  /data/bridge/spool
chown -R veilid:veilid /data/veilid /data/bridge

# Validate the translation-layer configuration before starting either process.
veilid-http-bridge --check

# Convenience-first default requested by the project: update only Veilid packages.
# A pinned VEILID_VERSION or VEILID_AUTO_UPDATE=false makes the image deterministic.
if /usr/local/bin/update-veilid.sh; then
  :
else
  update_status=$?
  if [ "$update_status" -ne 10 ]; then
    echo "Veilid update check failed with status $update_status" >&2
    exit "$update_status"
  fi
fi

terminate() {
  [ -n "${BRIDGE_PID:-}" ] && kill "$BRIDGE_PID" 2>/dev/null || true
  [ -n "${VEILID_PID:-}" ] && kill "$VEILID_PID" 2>/dev/null || true
  wait "${BRIDGE_PID:-}" 2>/dev/null || true
  wait "${VEILID_PID:-}" 2>/dev/null || true
}
trap terminate INT TERM EXIT

su -s /bin/sh veilid -c "veilid-server" &
VEILID_PID=$!

# The bridge currently exposes a validation supervisor until the remote adapter lands.
# This is deliberately explicit rather than pretending traffic is being translated.
if [ "${VHTTP_ADAPTER_MODE:-remote}" = "validation" ]; then
  veilid-http-bridge --validation-supervisor &
else
  veilid-http-bridge &
fi
BRIDGE_PID=$!

auto_update_loop() {
  case "${VEILID_AUTO_UPDATE:-true}" in
    1|true|TRUE|yes|YES|on|ON) ;;
    *) return 0 ;;
  esac
  [ "${VEILID_VERSION:-latest}" = "latest" ] || return 0
  interval="${VEILID_UPDATE_INTERVAL_SECONDS:-3600}"
  while sleep "$interval"; do
    if /usr/local/bin/update-veilid.sh; then
      :
    else
      update_status=$?
      if [ "$update_status" -eq 10 ]; then
        echo '{"event":"veilid-update-restart-required"}'
        kill "$BRIDGE_PID" "$VEILID_PID" 2>/dev/null || true
        return 0
      fi
      echo "Veilid periodic update failed with status $update_status" >&2
    fi
  done
}
auto_update_loop &
UPDATE_PID=$!

while kill -0 "$VEILID_PID" 2>/dev/null && kill -0 "$BRIDGE_PID" 2>/dev/null; do
  sleep 1
done
kill "$UPDATE_PID" 2>/dev/null || true

wait "$BRIDGE_PID" || status=$?
status=${status:-1}
exit "$status"
