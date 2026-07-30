#!/bin/sh
set -eu

mkdir -p \
  /data/veilid/protected-store \
  /data/veilid/table-store \
  /data/veilid/block-store \
  /data/bridge/route/history \
  /data/bridge/transfers \
  /data/bridge/completed \
  /data/bridge/spool
chown -R veilid:veilid /data/veilid /data/bridge

# Validate translation-layer configuration before starting either required process.
veilid-http-bridge --check

terminate() {
  [ -n "${BRIDGE_PID:-}" ] && kill "$BRIDGE_PID" 2>/dev/null || true
  [ -n "${VEILID_PID:-}" ] && kill "$VEILID_PID" 2>/dev/null || true
  wait "${BRIDGE_PID:-}" 2>/dev/null || true
  wait "${VEILID_PID:-}" 2>/dev/null || true
}
trap terminate INT TERM EXIT

su -s /bin/sh veilid -c "veilid-server" &
VEILID_PID=$!

# The official remote API is private to this container. Wait for the listener before
# starting the bridge so normal cold boots do not rely on process scheduling luck.
endpoint="${VEILID_CLIENT_ENDPOINT:-127.0.0.1:5959}"
case "$endpoint" in
  tcp://*) endpoint=${endpoint#tcp://} ;;
esac
case "$endpoint" in
  *:*)
    host=${endpoint%:*}
    port=${endpoint##*:}
    attempts=0
    until nc -z "$host" "$port" >/dev/null 2>&1; do
      attempts=$((attempts + 1))
      if ! kill -0 "$VEILID_PID" 2>/dev/null; then
        echo "veilid-server exited before its client API became ready" >&2
        wait "$VEILID_PID" || exit $?
        exit 1
      fi
      if [ "$attempts" -ge "${VEILID_CLIENT_STARTUP_ATTEMPTS:-240}" ]; then
        echo "timed out waiting for Veilid client API at $endpoint" >&2
        exit 1
      fi
      sleep 0.5
    done
    ;;
  unix://*)
    socket=${endpoint#unix://}
    attempts=0
    until [ -S "$socket" ]; do
      attempts=$((attempts + 1))
      if ! kill -0 "$VEILID_PID" 2>/dev/null; then
        echo "veilid-server exited before its client API socket became ready" >&2
        wait "$VEILID_PID" || exit $?
        exit 1
      fi
      if [ "$attempts" -ge "${VEILID_CLIENT_STARTUP_ATTEMPTS:-240}" ]; then
        echo "timed out waiting for Veilid client API socket $socket" >&2
        exit 1
      fi
      sleep 0.5
    done
    ;;
esac

# Development can explicitly request the configuration-only supervisor. Production
# always starts the real official veilid-server remote adapter.
if [ "${VHTTP_ADAPTER_MODE:-remote}" = "validation" ]; then
  veilid-http-bridge --validation-supervisor &
else
  veilid-http-bridge &
fi
BRIDGE_PID=$!

# The two processes form one deployable unit. If either exits, terminate the other
# and let Docker's restart policy recreate the complete immutable container.
while kill -0 "$VEILID_PID" 2>/dev/null && kill -0 "$BRIDGE_PID" 2>/dev/null; do
  sleep 1
done

status=1
if ! kill -0 "$BRIDGE_PID" 2>/dev/null; then
  wait "$BRIDGE_PID" || status=$?
elif ! kill -0 "$VEILID_PID" 2>/dev/null; then
  wait "$VEILID_PID" || status=$?
fi
exit "$status"
