# Development

## Toolchains

- Rust 1.89.0
- Node.js 22 or later
- pnpm 10
- Docker Engine and Compose v2

## Rust workspace

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace --all-features
```

Changes to a Rust crate are picked up directly through workspace path dependencies.
There is no manual DLL/SO/WASM re-reference step. Electron development launches the
current `target/debug/veilid-http-native` sidecar; package builds include the release
binary as an Electron resource.

A future WASM adapter will compile selected environment-neutral crates from the same
workspace. It will not make the browser implementation the source of truth.

## Electron

```bash
pnpm install
cargo build -p veilid-http-native
pnpm --filter @veilid-http/electron test
pnpm --filter @veilid-http/electron build
pnpm --filter @veilid-http/electron dev
```

Set `VEILID_HTTP_NATIVE_PATH` to override the sidecar location.

## HTTP compatibility fixtures

```bash
node scripts/smoke-http-fixtures.mjs
```

This starts the sample server on loopback, exercises common methods and a two-MiB
incremental stream, and exits without third-party Node dependencies.

## Docker

```bash
cp .env.example .env
docker compose -f compose.yaml -f dev-compose.yaml up --build
```

The development overlay mounts the Rust source and Cargo caches, then builds the bridge
inside the same Debian/Veilid container used for the node process. Its adapter mode is
explicitly `validation` until the remote API integration lands.

The host upstream defaults to port 8080. Docker Desktop provides
`host.docker.internal`; Linux uses the overlay's `host-gateway` mapping.

## Generated artifacts

Do not commit `target`, `node_modules`, Electron Forge output, mounted Veilid data,
RouteBlobs, SQLite journals, secrets, IDE caches, or test reports. The root
`.gitignore` covers Rust, Electron/Node, Visual Studio, Rider, future C#/.NET work,
and common platform noise.
