# Docker Server

The server is Docker-first and intentionally deploys as exactly one container.

```text
Debian 13.6
├── official veilid-server
├── veilid-http-bridge
├── veilid-http-cli
├── Veilid-only updater
└── tini + process supervisor
```

The Dockerfile is designed for `linux/amd64` and `linux/arm64`. It does not launch a
second application container, an embedded reverse proxy, or a cache.

## One upstream

Set exactly one base URL:

```dotenv
VHTTP_UPSTREAM_URL=http://host.docker.internal:8080
```

The transported client supplies only an absolute path and query. It cannot select the
upstream scheme, host, or port. The bridge combines that path with the configured
base URL. To use port 80, 443, 8443, or anything else, set it here on the server.

Point this one URL at HAProxy/NGINX/Caddy/Traefik when you need multiple services,
TLS/mTLS to another machine, authentication to the upstream, routing, or failover.

## Compose layering

Production-shaped configuration:

```bash
cp .env.example .env
docker compose up -d --build
```

Development:

```bash
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

The later development file overrides or augments the base service. Both Veilid and
the bridge still run in the same container. Linux receives the
`host.docker.internal:host-gateway` mapping; Docker Desktop provides the hostname
natively.

## Persistence

Two host directories are mounted:

```text
./data/veilid  -> /data/veilid
./data/bridge  -> /data/bridge
```

Veilid uses persistent protected, table, and block stores. The bridge stores its
RouteBlob, transfer journal, retained completion state, and bounded temporary spool
files separately.

Never run production without persistent mounts. Recreating the container must not
silently recreate the node identity or lose the published private route.

## RouteBlob lifecycle

The intended live startup sequence is:

1. Start the official `veilid-server` with its client API bound inside the container.
2. Wait until it attaches to the network.
3. Restore the existing private route from persistent state, or allocate a reliable
   private route on first startup.
4. Write the RouteBlob atomically to:

```text
/data/bridge/route/current.blob
/data/bridge/route/current.base64
/data/bridge/route/current.json
```

5. Calculate the 128-bit BLAKE3/Base32 fingerprint.
6. Begin accepting VHTTP AppCalls/AppMessages over that private route.

Export it with:

```bash
docker compose exec veilid-http veilid-http-cli route show
docker compose exec veilid-http veilid-http-cli route export --format base64
docker compose exec veilid-http veilid-http-cli route export --format descriptor
```

Do not share or configure a NodeId as the destination. VeilidHttp's transport trait
uses private RouteIds only.

The route allocation/restore portion is currently blocked on the unfinished remote
adapter and is called out honestly in the startup error rather than silently falling
back to a public NodeId.

## Backups

Stop the container or use a filesystem-consistent snapshot, then back up both trees:

```bash
docker compose down
tar -C data -czf veilid-http-backup.tgz veilid bridge
```

Restore them to the same paths before starting the replacement container. A backup
that contains only `current.base64` is enough to share the endpoint, but not enough
to restore the server's Veilid identity and route state.

Transfer spool files are transient; preserving them can help future resumability but
they are not a substitute for application-level storage.

## Veilid updates

The image defaults to:

```dotenv
VEILID_AUTO_UPDATE=true
VEILID_VERSION=latest
```

At startup and periodically, only `veilid-server` and `veilid-cli` are upgraded. When
a new package is installed, the container exits so Docker can restart both processes
against the same persistent stores.

Operators who require reproducible immutable deployments should use either:

```dotenv
VEILID_AUTO_UPDATE=false
```

or pin an exact package version:

```dotenv
VEILID_VERSION=<exact-debian-package-version>
```

The updater never upgrades Debian generally and never modifies VeilidHttp itself.
Future release automation will publish tested images, but it is intentionally not in
this initial PR.
