#!/bin/sh
set -eu

is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

if ! is_true "${VEILID_AUTO_UPDATE:-true}"; then
  echo '{"event":"veilid-update-skipped","reason":"disabled"}'
  exit 0
fi

if [ "${VEILID_VERSION:-latest}" != "latest" ]; then
  echo '{"event":"veilid-update-skipped","reason":"version-pinned"}'
  exit 0
fi

before="$(dpkg-query -W -f='${Version}' veilid-server 2>/dev/null || true)"
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --only-upgrade veilid-server veilid-cli
rm -rf /var/lib/apt/lists/*
after="$(dpkg-query -W -f='${Version}' veilid-server 2>/dev/null || true)"

if [ "$before" != "$after" ]; then
  printf '{"event":"veilid-updated","from":"%s","to":"%s"}\n' "$before" "$after"
  exit 10
fi
printf '{"event":"veilid-update-current","version":"%s"}\n' "$after"
