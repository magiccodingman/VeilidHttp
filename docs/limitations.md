# Current Limitations

This initial PR is a serious protocol and deployment foundation, but it is not yet a
production end-to-end proxy.

## Primary incomplete integration

- `veilid-http-native` does not yet start and attach native `veilid-core`.
- `veilid-http-bridge` does not yet drive the official `veilid-server` remote API.
- Because of those adapters, the bridge does not yet allocate/restore its RouteBlob or
  forward a live request across Veilid.
- Electron request/response body IPC is not yet the final binary streaming channel.

These boundaries fail explicitly. The code does not claim a successful route or fall
back to a NodeId.

## Validation still required

- Packaged Electron service-worker/PWA behavior under the privileged `veilid://`
  scheme.
- RouteBlob durability across restart, relay changes, route repair, and long runtimes.
- Real-network frame size/window/timeout tuning.
- Durable crash-resume journals for very large in-progress streams.
- Docker image builds on both amd64 and arm64.

## Intentionally out of scope

- WebSockets, SignalR, WebTransport, HTTP upgrades, CONNECT, or raw socket tunnels.
- Multiple upstream mappings.
- Embedded NGINX/HAProxy/cache/load balancer.
- Native capability permission framework.
- Official release/publishing workflows.
- Tauri adapter.

The protocol core and environment adapters are separated specifically so completing
the live integration does not require redesigning HTTP semantics, framing, browser
isolation, or Docker deployment.
