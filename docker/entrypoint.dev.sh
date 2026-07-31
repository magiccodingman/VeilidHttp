#!/bin/sh
set -eu
cargo build -p veilid-http-bridge -p veilid-http-cli
install -m 0755 target/debug/veilid-http-bridge /usr/local/bin/veilid-http-bridge
install -m 0755 target/debug/veilid-http-cli /usr/local/bin/veilid-http-cli
exec /usr/local/bin/entrypoint.sh
