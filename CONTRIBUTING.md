# Contributing

Keep the scope narrow: HTTP translation over Veilid private routes. Features that belong
in NGINX, HAProxy, an authentication service, a cache, or an application framework do
not belong in the core.

Before opening a PR:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
pnpm test
pnpm build
```

Protocol changes require updates to `docs/protocol.md`, unit tests, and compatibility
vectors. Never silently reinterpret an existing VHTTP/1 frame.
