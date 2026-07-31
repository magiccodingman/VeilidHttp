# Current Limitations

VeilidHttp is a development-alpha implementation, not a production release.

## Implemented but awaiting full validation

- The embedded native `veilid-core` adapter and official `veilid-server` remote API
  adapter are implemented against pinned upstream versions, but still need a complete
  compiler/API correction pass and a real-network round trip on the current head.
- Electron-to-sidecar IPC and VHTTP request/response transport are incremental and
  bounded, but packaged-client behavior must be exercised on Windows and Linux.
- A Chromium smoke harness checks service workers, Cache Storage, IndexedDB, CORS,
  secure-context behavior, WebAssembly availability, custom-protocol streaming, and
  persistent sessions under `veilid://`. Results still need to pass in CI and packaged
  builds.
- Docker definitions target both `linux/amd64` and `linux/arm64`; arm64 requires a
  successful QEMU/native build and runtime smoke test.
- RouteBlob behavior still needs long-running observation across server restart, relay
  replacement, dead-route notification, and automatic route rotation.
- Real-network frame size, window, retry, batching, and timeout defaults need profiling.

The repository is currently private. GitHub-hosted workflow jobs require available
private-repository Actions allowance; without it, GitHub rejects jobs before checkout
and produces no compiler or test logs. That scheduler failure is separate from code test
results.

## Reliability boundary

VeilidHttp provides crash-safe at-most-once upstream forwarding for a retained
transaction ID:

- A transaction ID is durably claimed before the upstream is contacted.
- Concurrent duplicates wait on or reuse the original execution.
- Small atomic responses may be retained and replayed.
- Large and streamed responses leave persistent completion tombstones.
- A completed request is not silently forwarded again merely because its response was
  lost.
- A pre-execution claim left by a process crash is recovered as an indeterminate
  tombstone and is not automatically forwarded again.

The conservative recovery rule may result in zero executions if the bridge crashed
after persisting the claim but before the upstream received the request. That is the
honest at-most-once trade: the bridge refuses to risk a second POST when it cannot know
whether the first reached the backend.

This is not a claim of mathematically exactly-once execution. Losing persistent bridge
state, changing transaction IDs, or bypassing the conforming client changes that
boundary.

## Large objects and crash recovery

A transfer is streamed through bounded channels and retransmission windows; VeilidHttp
does not accumulate an entire model, archive, or media object in memory or on disk.
Only compressed frames inside the configured pending and in-flight byte budget are
retained for retransmission. When that budget or the peer receive window fills, reads
stop and backpressure propagates toward Chromium or the HTTP upstream.

Current V1 behavior does not resume an in-progress body stream after either process is
restarted. The original transaction ID remains protected by the durable claim, so an
application that deliberately retries after an indeterminate crash must make a new
application-level decision and transaction rather than receiving an automatic replay.

The configured pending-byte budget must be large enough for the configured frame/window
combination. Unsafe configurations are rejected rather than spilling an unbounded object
to disk.

## Intentionally out of scope

- WebSockets, SignalR, WebTransport, HTTP upgrades, CONNECT, or raw socket tunnels.
- Multiple upstream mappings.
- Embedded NGINX, HAProxy, cache, CDN, load balancer, or authentication service.
- A generic native-capability permission framework.
- Tauri support in V1.
- Official release and publishing workflows in this development phase.
- macOS release binaries; macOS remains source-build documentation only.
