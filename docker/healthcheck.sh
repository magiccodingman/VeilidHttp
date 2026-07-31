#!/bin/sh
set -eu
pgrep -x veilid-server >/dev/null
pgrep -f "/usr/local/bin/veilid-http-bridge" >/dev/null
veilid-http-bridge --check >/dev/null

if [ "${VHTTP_ADAPTER_MODE:-remote}" != "validation" ]; then
  test -s "${VHTTP_DATA_DIR:-/data/bridge}/route/current.blob"
  test -s "${VHTTP_DATA_DIR:-/data/bridge}/route/current.json"
fi
