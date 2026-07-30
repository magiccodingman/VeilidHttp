# Testing Strategy

## Unit tests already present

- Wire round trips, malformed lengths, and hard size bounds.
- Multi-frame transport bundle round trips.
- RouteBlob Base64URL and deterministic BLAKE3/Base32 identity.
- Bounded batching and out-of-order reassembly.
- Sliding send windows and selective ACK release.
- Hostile-network retry backoff.
- Zstandard round trips and decompression-bomb limits.
- Transaction duplicate/expiry state.
- Hop-by-hop HTTP filtering and one-upstream policy.
- Normal HTTP method support and explicit CONNECT rejection.
- Trusted route-header overwrite.
- Electron site-ID validation.

## HTTP compatibility fixtures

`samples/static-pwa` contains an installable manifest, service worker, Cache Storage,
JavaScript, and same-origin API fetch.

`samples/ssr-node` exposes:

- Static PWA files.
- SSR-shaped HTML responses.
- GET/HEAD/POST/PUT/PATCH/DELETE/OPTIONS echo behavior.
- Trusted route-header visibility.
- A streaming endpoint that obeys Node backpressure.

Run the dependency-free smoke test:

```bash
node scripts/smoke-http-fixtures.mjs
```

## Required live integration matrix

Once adapters are connected, CI/host tests must inject dropped, delayed, duplicated,
and reordered frames; abort requests; run concurrent streams; and stream generated
objects far larger than the configured memory window.

The large-object test must assert bounded retained bytes. It must not commit or first
materialize a giant fixture.

## Electron compatibility gate

A packaged test must prove under `veilid://`:

- Service-worker registration and fetch interception.
- Cache Storage persistence.
- IndexedDB persistence.
- WebAssembly loading.
- Streaming request/response bodies.
- Range requests.
- Upload and download dialogs.
- Cross-origin CORS between two route hosts.
- Session data clearing.

## Platform matrix

- Linux x64 and arm64: primary.
- Windows x64 and arm64: secondary source builds, later signed release.
- macOS x64 and arm64: source-build instructions only.
- Docker: linux/amd64 and linux/arm64.
