# VeilidHttp

VeilidHttp is a deliberately narrow HTTP translation layer carried over Veilid
private routes. An ordinary PWA, SPA, static site, SSR application, or REST API keeps
speaking normal HTTP; VeilidHttp handles the Veilid transport details between them.

```text
ordinary Chromium website
        ↕ HTTP requests and streams
locked-down Electron client
        ↕ VHTTP/1
Veilid private route
        ↕ VHTTP/1
one Docker container: veilid-server + bridge
        ↕ ordinary HTTP
one configured upstream application or reverse proxy
```

> **Current status:** development alpha. The native and remote Veilid adapters, private
> route lifecycle, binary Electron IPC, bounded request/response streaming, retries,
> selective acknowledgements, persistent duplicate suppression, one-upstream bridge,
> Docker runtime, and browser compatibility tests are implemented in this branch.
> Real-network interoperability, long-running route behavior, packaging, and release
> validation remain required before production use. See
> [Known limitations](docs/limitations.md).

## Scope

VeilidHttp owns only what is required to translate HTTP over Veilid:

- AppCall/AppMessage framing and correlation.
- Streaming Zstandard compression and fragmentation below Veilid's 32 KiB ceiling.
- Multi-frame AppMessage bundling.
- Sliding windows, selective ACKs, retries, reordering, deduplication, expiry, and
  cancellation.
- Incremental request/response streams with bounded memory and end-to-end backpressure.
- Persistent at-most-once forwarding records for duplicate transaction IDs.
- Reconstructing one ordinary HTTP request against one configured upstream.
- Returning status, headers, and a streaming body to Chromium.

It is **not** a cache, CDN, load balancer, authentication platform, service router, or
HA proxy. Point the single upstream at NGINX, HAProxy, Caddy, Traefik, or your own app
when those features are needed.

Veilid may use TCP and UDP underneath. VHTTP/1 transports HTTP semantics; it does not
expose arbitrary TCP or UDP tunnels. WebSockets, SignalR, WebTransport, and HTTP
`CONNECT`/upgrade tunnels are outside V1.

## Repository

- `crates/veilid-http-wire`: deterministic VHTTP frames and multi-frame bundles.
- `crates/veilid-http-core`: bounded batching, windows, ACKs, retries, reassembly,
  journals, Zstandard, and integrity helpers.
- `crates/veilid-http-engine`: shared request/response streaming state machines.
- `crates/veilid-http-stream`: VHTTP stream metadata and frame encoding.
- `crates/veilid-http-route`: RouteBlob encoding and 128-bit BLAKE3/Base32 identities.
- `crates/veilid-http-http`: HTTP validation, one-upstream policy, and trusted route
  headers.
- `crates/veilid-http-ipc`: authenticated framed binary sidecar IPC.
- `crates/veilid-http-transport`: environment-neutral AppCall/AppMessage traits.
- `crates/veilid-http-veilid-native`: embedded native `veilid-core` adapter.
- `crates/veilid-http-veilid-remote`: multiplexed `veilid-server` remote API adapter.
- `apps/veilid-http-electron`: generic locked-down Chromium loader.
- `apps/veilid-http-native`: trusted Electron Rust sidecar.
- `apps/veilid-http-bridge`: Docker-side translation process.
- `apps/veilid-http-cli`: route/status/export tooling.
- `samples/`: static PWA and SSR/streaming compatibility fixtures.

Both Veilid 0.5.5 adapters construct explicit `Target::RouteId` destinations for
AppCall/AppMessage traffic. A RouteId is never treated as a node target or left to an
implicit conversion.

## Development

Requirements:

- Rust 1.89+
- Node.js 22+
- pnpm 10+
- Docker Engine with Compose v2

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features

pnpm install --frozen-lockfile
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build

cp .env.example .env
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

Compose files are layered production first, development second. The development
container runs the same official `veilid-server` and live bridge adapter while mounting
workspace source and Cargo caches.

## Server route

After the bridge attaches, it allocates a reliable private route and atomically publishes
its shareable RouteBlob under `/data/bridge/route`:

```bash
docker compose exec veilid-http veilid-http-cli status
docker compose exec veilid-http veilid-http-cli route show
docker compose exec veilid-http veilid-http-cli route export --format base64
docker compose exec veilid-http veilid-http-cli route export --format descriptor
```

Share the RouteBlob—not the node ID. The NodeId is not a VeilidHttp endpoint. Streamed
transactions also allocate a private client return route so both directions remain on
private routes.

## Client launch

The Electron client accepts:

```bash
veilid-http --route-base64 '<unpadded-base64url-route-blob>'
veilid-http --route-file ./route.blob --path /admin
veilid-http ./example.veilidapp
```

A sibling `app.veilidapp` can auto-launch a fixed site without modifying the signed
executable. See [Electron client](docs/client-electron.md).

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Development](docs/development.md)
- [Protocol](docs/protocol.md)
- [Streaming and large objects](docs/streaming.md)
- [Electron client](docs/client-electron.md)
- [Docker server](docs/server-docker.md)
- [Configuration](docs/configuration.md)
- [RouteBlobs and privacy](docs/route-blobs.md)
- [Reverse proxies](docs/reverse-proxy.md)
- [Security](docs/security.md)
- [Testing](docs/testing.md)
- [Platform builds](docs/platform-builds.md)
- [Known limitations](docs/limitations.md)

## License

MPL-2.0.
