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

> **Current status:** substantial foundation implementation, not yet a production
> proxy. The protocol wire format, route identity, batching, bounded stream windows,
> selective acknowledgements, Zstandard helpers, HTTP policy, Electron security shell,
> one-container Docker layout, fixtures, tests, and documentation are implemented.
> The live native and `veilid-server` transport adapters remain the primary unfinished
> integration. See [Known limitations](docs/limitations.md).

## Scope

VeilidHttp owns only what is required to translate HTTP over Veilid:

- AppCall/AppMessage framing and correlation.
- Compression, fragmentation below Veilid's 32 KiB ceiling, and transport bundling.
- Sliding windows, selective ACKs, retries, deduplication, expiry, and cancellation.
- Incremental request/response streams with bounded memory and backpressure.
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
- `crates/veilid-http-core`: bounded batching, send/receive windows, ACKs, retries,
  reassembly, journals, Zstandard, and integrity helpers.
- `crates/veilid-http-route`: RouteBlob encoding and 128-bit BLAKE3/Base32 identities.
- `crates/veilid-http-http`: HTTP validation, hop-by-hop filtering, one-upstream URL
  policy, and trusted route headers.
- `crates/veilid-http-transport`: environment-neutral AppCall/AppMessage traits.
- `apps/veilid-http-electron`: generic locked-down Chromium loader.
- `apps/veilid-http-native`: trusted Electron Rust sidecar boundary.
- `apps/veilid-http-bridge`: Docker-side translation process.
- `apps/veilid-http-cli`: route/status/export tooling.
- `samples/`: static PWA and SSR/streaming compatibility fixtures.

## Development

Requirements:

- Rust 1.89+
- Node.js 22+
- pnpm 10+
- Docker Engine with Compose v2

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features

pnpm install
pnpm test
pnpm build

cp .env.example .env
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

Compose files are intentionally layered production first, development second. The
`dev-compose.yaml` file builds and runs both `veilid-server` and the bridge in the
same container, but uses the explicit validation adapter until the live remote
adapter is completed.

## Server route

The intended live bridge allocates a reliable private route after `veilid-server`
attaches and persists the binary RouteBlob under `/data/bridge/route`. Operators share
the RouteBlob—not the node ID.

```bash
docker compose exec veilid-http veilid-http-cli route show
docker compose exec veilid-http veilid-http-cli route export --format base64
docker compose exec veilid-http veilid-http-cli route export --format descriptor
```

The server's NodeId is not a VeilidHttp endpoint. Default routing uses a private route
for receiver privacy and Veilid's default safety routing for sender privacy.

## Client launch

The Electron client accepts:

```bash
veilid-http --route-base64 '<unpadded-base64url-route-blob>'
veilid-http --route-file ./route.blob --path /admin
veilid-http ./example.veilidapp
```

A sibling `app.veilidapp` can also auto-launch a fixed site without modifying the
signed executable. See [Electron client](docs/client-electron.md).

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
