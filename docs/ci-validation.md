# Continuous integration validation

The draft branch is validated in four independent layers:

1. **Rust** — workspace metadata, `rustfmt`, Clippy with warnings denied, and all tests.
2. **Electron** — frozen pnpm install, TypeScript, unit fixtures, and a real Chromium smoke test.
3. **Compose** — merged production/development configuration and shell syntax.
4. **Docker** — separate `linux/amd64` and `linux/arm64` image builds after Rust succeeds.

The Electron smoke test uses a real loopback HTTP server and virtual hosts of the form
`http://<route-fingerprint>.veilid.localhost:<stable-port>/`. It verifies service-worker
control, Cache Storage, IndexedDB persistence, WebAssembly, streamed responses, and
Chromium-enforced cross-origin allow/deny behavior.

An old GitHub Actions run always rebuilds the merge snapshot captured when that run was
created. After protocol or adapter changes, use the newest run associated with the current
pull-request head rather than rerunning a stale workflow attempt.
