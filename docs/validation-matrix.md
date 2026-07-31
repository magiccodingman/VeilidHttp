# Validation Matrix

This document tracks the evidence required before VeilidHttp can move beyond development alpha.
It separates deterministic repository checks from environment-dependent Veilid and packaging tests.

## Required in CI

- Rust workspace metadata resolves with the committed `Cargo.lock`.
- `cargo fmt --all --check` passes.
- Clippy passes for every target and feature with warnings denied.
- All Rust unit and integration tests pass.
- The Electron workspace installs from the committed `pnpm-lock.yaml`.
- Electron unit tests and strict TypeScript production builds pass.
- The real Electron custom-protocol/PWA smoke harness passes under Chromium.
- Compose and shell files validate.
- Docker images build for both `linux/amd64` and `linux/arm64`.

## Required integration evidence

- Electron request body streams through the native sidecar without complete-body buffering.
- The native client uses the server-advertised request receive window.
- Request and response streams survive loss, duplication, and reordering within configured retry limits.
- Duplicate transaction IDs cannot execute the configured HTTP upstream twice during retention, including after bridge restart.
- Route publication survives a clean restart and rotates after a dead-route notification.
- A real private-route round trip reaches an ordinary HTTP upstream and returns a streamed response to Chromium.
- Packaged Linux and Windows clients launch their bundled sidecars and preserve per-site browser state.
- An arm64 container starts and reaches healthy status on real arm64 hardware or an equivalent runtime.

## Release boundary

Passing repository CI is necessary but not sufficient for a production-readiness claim. Real Veilid route lifecycle, multi-hop behavior, packaged application behavior, and long-running resource profiles must also be recorded before a release is promoted beyond alpha.
