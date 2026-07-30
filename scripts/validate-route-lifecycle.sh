#!/bin/sh
set -eu

compose_files="-f compose.yaml"
if [ "${VHTTP_USE_DEV_COMPOSE:-false}" = "true" ]; then
  compose_files="$compose_files -f dev-compose.yaml"
fi

compose() {
  # shellcheck disable=SC2086
  docker compose $compose_files "$@"
}

wait_ready() {
  timeout_seconds="${VHTTP_ROUTE_WAIT_SECONDS:-180}"
  started="$(date +%s)"
  while :; do
    if output="$(compose exec -T veilid-http veilid-http-cli status 2>/dev/null)" \
      && printf '%s\n' "$output" | grep -q '^route_present=true$'; then
      printf '%s\n' "$output"
      return 0
    fi
    now="$(date +%s)"
    if [ $((now - started)) -ge "$timeout_seconds" ]; then
      echo "Timed out waiting for a published private route" >&2
      return 1
    fi
    sleep 2
  done
}

fingerprint() {
  compose exec -T veilid-http veilid-http-cli route show \
    | sed -n 's/^fingerprint=//p' \
    | head -n 1
}

export_blob() {
  compose exec -T veilid-http veilid-http-cli route export --format base64
}

printf '%s\n' 'Waiting for the current private route...'
wait_ready >/dev/null
before_fingerprint="$(fingerprint)"
before_blob="$(export_blob)"
[ -n "$before_fingerprint" ] || {
  echo "The bridge did not report a route fingerprint" >&2
  exit 1
}
[ -n "$before_blob" ] || {
  echo "The bridge did not export a RouteBlob" >&2
  exit 1
}
printf 'before_fingerprint=%s\n' "$before_fingerprint"
printf 'before_base64_bytes=%s\n' "$(printf %s "$before_blob" | wc -c | tr -d ' ')"

if [ "${VHTTP_RESTART_CONTAINER:-true}" = "true" ]; then
  printf '%s\n' 'Restarting the one VeilidHttp container...'
  compose restart veilid-http >/dev/null
  wait_ready >/dev/null
fi

after_fingerprint="$(fingerprint)"
after_blob="$(export_blob)"
printf 'after_fingerprint=%s\n' "$after_fingerprint"
printf 'after_base64_bytes=%s\n' "$(printf %s "$after_blob" | wc -c | tr -d ' ')"

if [ "$before_blob" = "$after_blob" ]; then
  printf '%s\n' 'route_blob_stable_across_restart=true'
else
  printf '%s\n' 'route_blob_stable_across_restart=false'
  printf '%s\n' 'The bridge published a different route after restart. Existing clients need the replacement blob.'
fi

history_count="$(compose exec -T veilid-http sh -c \
  'find /data/bridge/route/history -type f -name "*.json" 2>/dev/null | wc -l' \
  | tr -d '[:space:]')"
printf 'route_history_records=%s\n' "$history_count"

printf '%s\n' 'This diagnostic checks local persistence/publication only.'
printf '%s\n' 'Use a second client/node and real requests to validate remote reachability before and after restart/relay change.'
