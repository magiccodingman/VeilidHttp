# Development

## Toolchains

- Rust 1.89.0
- Node.js 22 or later
- pnpm 10
- Docker Engine and Compose v2

## Rust workspace

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Changes to Rust crates are picked up directly through workspace path dependencies.
There is no manual DLL, SO, or WASM re-reference step. Electron development launches
the current `target/debug/veilid-http-native` sidecar; package builds include the
release binary as an Electron resource.

A future browser adapter can compile selected environment-neutral crates to WASM. It
will remain another adapter over VHTTP rather than becoming a second protocol source of
truth.

## Electron

```bash
pnpm install
cargo build -p veilid-http-native
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build
pnpm --filter @veilid-http/electron dev
```

Set `VEILID_HTTP_NATIVE_PATH` to override the sidecar location. The main process creates
a private Unix socket or Windows named pipe and supplies a one-launch authentication
secret to the child process. Loaded sites never receive this path, secret, or raw IPC
surface.

### Chromium/PWA compatibility gate

On Linux with Xvfb:

```bash
xvfb-run -a pnpm --filter @veilid-http/electron exec \
  electron test/electron-pwa-smoke.cjs
```

The harness verifies the privileged `veilid://` scheme with:

- Service-worker registration and fetch interception.
- Cache Storage and IndexedDB.
- Persistent Electron partitions.
- Cross-origin Veilid CORS allow and deny behavior.
- A genuinely streamed custom-protocol response.
- Secure-context and WebAssembly availability.

A failure here is an architectural signal. Do not bypass it by disabling Chromium web
security; investigate the localhost-origin fallback instead.

## HTTP compatibility fixtures

```bash
node scripts/smoke-http-fixtures.mjs
```

This starts the sample server on loopback, exercises common methods and an incremental
multi-megabyte stream, and exits without third-party Node dependencies.

The Rust bridge integration test also drives a streamed VHTTP transaction through a
mock Veilid transport into a real loopback HTTP server, acknowledges the response, and
verifies a duplicate transaction ID does not hit the upstream twice.

## Docker

```bash
cp .env.example .env
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

The development overlay mounts the Rust source and Cargo caches, then runs the live
remote adapter alongside the official pinned `veilid-server` in the same container.
The host upstream defaults to port 8080. Docker Desktop provides
`host.docker.internal`; Linux uses the overlay's `host-gateway` mapping.

The base Compose file is always listed first and `dev-compose.yaml` second so local
source mounts and debug settings override the production-shaped defaults.

## Lockfiles

`Cargo.lock` and `pnpm-lock.yaml` belong in the repository for reproducible application
and container builds. Regenerate them after dependency changes:

```bash
cargo generate-lockfile
pnpm install --lockfile-only
```

CI also uploads generated lockfiles as artifacts to recover them when an execution
environment cannot resolve package registries.

## Generated artifacts

Do not commit `target`, `node_modules`, Electron Forge output, mounted Veilid data,
RouteBlobs, completion databases, secrets, IDE caches, or test reports. The root
`.gitignore` covers Rust, Electron/Node, Visual Studio, Rider, future C#/.NET work, and
common platform noise.
