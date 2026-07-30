#!/bin/sh
set -eu
pgrep -x veilid-server >/dev/null
pgrep -f "/usr/local/bin/veilid-http-bridge" >/dev/null
veilid-http-bridge --check >/dev/null
