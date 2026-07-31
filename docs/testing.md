# Testing Strategy

## Rust unit and fault tests

The workspace includes tests for:

- Wire round trips, malformed lengths, and hard size bounds.
- Multi-frame Veilid transport bundles.
- MessagePack stream metadata and unknown frame handling.
- RouteBlob Base64URL and deterministic BLAKE3/Base32 identity.
- Streaming Zstandard encode/decode and final logical integrity.
- Pending plus in-flight retransmission byte accounting.
- Bounded out-of-order reassembly.
- Sliding send windows, cumulative/selective ACK release, and peer zero windows.
- Dropped, reordered, duplicated, and retried frames.
- Delayed end-frame completion.
- Large generated streams that remain inside configured windows.
- Persistent completion replay/tombstones across reopen.
- Active execution capacity without evicting live transactions.
- Hop-by-hop HTTP filtering and one-upstream policy.
- Normal HTTP method support and explicit CONNECT rejection.
- Trusted route-header overwrite.
- IPC framing, authentication data, and length limits.

The bridge integration test starts a real loopback HTTP listener, sends a streamed VHTTP
request through a mock Veilid transport, receives/ACKs the incremental response, and then
replays the same transaction ID. It asserts that the upstream received exactly one
request.

Run:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

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

The large-object smoke path generates data while streaming. It never commits or first
materializes a giant fixture.

## Electron compatibility gate

`test/electron-pwa-smoke.cjs` launches real Electron under Xvfb and proves under
`veilid://`:

- Secure-context recognition.
- Service-worker registration, activation, and fetch interception.
- Cache Storage and IndexedDB.
- Persistent Electron partitions.
- WebAssembly availability.
- A multi-chunk streamed custom-protocol response.
- Cross-origin Veilid CORS allow and deny behavior.

```bash
xvfb-run -a pnpm --filter @veilid-http/electron exec \
  electron test/electron-pwa-smoke.cjs
```

Packaged release validation must additionally cover:

- Streaming uploads and downloads through the actual Rust sidecar.
- Range/media requests.
- File/folder input and download dialogs.
- Site-data clearing.
- Sidecar crash/restart behavior.
- Windows named-pipe IPC.

## Real Veilid integration matrix

A normal development machine or dedicated integration runner must exercise:

1. Start the one-container bridge and export its RouteBlob.
2. Start the Electron/native client and import that blob.
3. Load static/PWA and SSR fixtures.
4. Upload and download generated data larger than all memory windows.
5. Abort both request and response streams.
6. Run many concurrent assets/API calls.
7. Restart the server and client independently.
8. Observe relay replacement and dead-route rotation.
9. Repeat a retained non-idempotent transaction ID and verify no second upstream hit.
10. Run long enough to measure actual route, retry, timeout, frame, and batching behavior.

This matrix is required before declaring production readiness. Mock transport tests prove
our protocol engine; they cannot prove the behavior of the public Veilid network.

## Platform matrix

- Linux x64 and arm64: primary.
- Windows x64 and arm64: secondary source builds, later signed release.
- macOS x64 and arm64: source-build instructions only.
- Docker: `linux/amd64` and `linux/arm64`, each built separately in CI.
