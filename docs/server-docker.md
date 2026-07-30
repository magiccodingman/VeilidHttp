# Docker Server

The server is Docker-first and intentionally deploys as exactly one container.

```text
Debian 13.6
├── official pinned veilid-server
├── veilid-http-bridge
├── veilid-http-cli
└── tini + process supervisor
```

The Dockerfile targets `linux/amd64` and `linux/arm64`. It does not launch a second
application container, embedded reverse proxy, cache, authentication service, or load
balancer.

## One upstream

Set exactly one base URL:

```dotenv
VHTTP_UPSTREAM_URL=http://host.docker.internal:8080
```

The transported client supplies only an absolute path and query. It cannot select the
upstream scheme, host, or port. The bridge combines that path with the configured base
URL. To use port 80, 443, 8443, or anything custom, set it here on the server.

Point this URL at HAProxy, NGINX, Caddy, or Traefik when you need several services,
TLS/mTLS to another machine, authentication to the upstream, routing, or failover.
Those tools receive normal HTTP and do not need to understand Veilid.

## Compose layering

Production-shaped development image:

```bash
cp .env.example .env
docker compose up -d --build
```

Source-mounted development:

```bash
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

The later development file overrides or augments the base service. Both Veilid and the
bridge still run inside the same container. Linux receives the
`host.docker.internal:host-gateway` mapping; Docker Desktop provides the hostname
natively.

## Persistence

Two host directories are mounted:

```text
./data/veilid  -> /data/veilid
./data/bridge  -> /data/bridge
```

Veilid uses persistent protected, table, block, and route-spec stores. The bridge keeps:

```text
/data/bridge/
├── route/
│   ├── current.blob
│   ├── current.base64
│   ├── current.json
│   └── history/
├── completed/
├── transfers/
└── spool/
```

`completed/` contains retained atomic replies or tombstones used to avoid re-forwarding
a completed transaction ID. `transfers/` and `spool/` are reserved for diagnostics and
future crash-resume work; normal V1 transfers use bounded in-memory windows and never
materialize a complete large object there.

Never run production without persistent mounts. Recreating the container must not
silently recreate node identity, lose the published route, or erase completion records
that protect recent non-idempotent requests.

## RouteBlob lifecycle

Live startup performs:

1. Start the official `veilid-server` with its client API bound to
   `127.0.0.1:5959` inside the container.
2. Wait until that API accepts connections.
3. Connect the multiplexed remote adapter and verify its version prefix.
4. Allocate a reliable private route.
5. Atomically publish the binary RouteBlob, Base64URL representation, metadata, and
   history entry.
6. Begin accepting VHTTP AppCalls and AppMessages over that private route.
7. If Veilid reports the receiving route dead, cancel active streams, allocate a
   replacement, and publish it atomically.

Inspect/export it with:

```bash
docker compose exec veilid-http veilid-http-cli status
docker compose exec veilid-http veilid-http-cli route show
docker compose exec veilid-http veilid-http-cli route export --format base64
docker compose exec veilid-http veilid-http-cli route export --format descriptor
docker compose exec veilid-http veilid-http-cli transfers list
```

Do not share or configure the server NodeId as the destination. VeilidHttp uses the
published private RouteBlob. Streamed clients also supply a private return RouteBlob so
response data and acknowledgements do not fall back to a public node target.

A rotated route creates a new RouteBlob/fingerprint. Existing clients need the new blob
unless a future discovery/pointer layer is placed above VHTTP.

## Trusted forwarding metadata

Before contacting the one upstream, the bridge strips spoofed values and adds:

```http
X-Veilid-Route-Fingerprint: <current-server-route-fingerprint>
X-Veilid-Origin: veilid://<current-server-route-fingerprint>
```

These fields are useful for logging or external proxy policy. They are transport
metadata, not proof of user identity and not a replacement for application
authentication.

## Backups

Stop the container or use a filesystem-consistent snapshot, then back up both trees:

```bash
docker compose down
tar -C data -czf veilid-http-backup.tgz veilid bridge
```

Restore them to the same paths before starting the replacement container. A backup
containing only `current.base64` is sufficient to share the current endpoint, but not
to restore the server's Veilid identity, route state, or duplicate-suppression records.

## Veilid versions and updates

The server and remote API schema are pinned together at image build time:

```dotenv
VEILID_RELEASE_CHANNEL=stable
VEILID_VERSION=0.5.5
VEILID_EXPECTED_VERSION_PREFIX=0.5.5
```

A running container never invokes `apt upgrade`. This keeps deployments reproducible
and rollback sane. Updating Veilid means rebuilding and testing a new image, then
recreating the container against the same persistent stores.

A future release workflow may publish tested images automatically. That release process
is intentionally separate from this development PR.

## Memory and concurrency bounds

Each transaction uses a bounded send window, pending-compression budget, out-of-order
receive budget, and bounded HTTP channels. The bridge also caps active executions.
This means object size does not determine peak memory; configured concurrency and window
sizes do.

The bridge should reject a configuration whose complete retransmission window cannot fit
inside `VHTTP_MAX_PENDING_BYTES`. It does not silently solve an unsafe setting by
spooling an entire model or archive to disk.
