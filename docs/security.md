# Security Model

## Private routes by default

VeilidHttp imports and allocates private RouteBlobs. It does not advertise or target a
public NodeId. Default Veilid routing contexts retain sender safety routing, while the
private route supplies receiver privacy. Streamed clients publish a separate private
return RouteBlob for response data and ACKs.

The server's node identity is not the application address and must never be documented
or accepted as one.

## Generic client

Arbitrary loaded sites are untrusted. Electron renderer requirements:

- `nodeIntegration: false`
- `contextIsolation: true`
- `sandbox: true`
- `webSecurity: true`
- no raw `ipcRenderer`
- no navigation to `file:` or insecure schemes
- no arbitrary window creation
- no native filesystem/process/network API

Normal user-mediated uploads and downloads remain available. A site can only receive
the files the user deliberately chooses, just like a normal browser.

The Electron main process communicates with the Rust sidecar over a random private Unix
socket or Windows named pipe. The first frame must present a one-launch random secret.
The site renderer receives neither the endpoint nor secret. IPC metadata and payload
lengths are capped before allocation.

## Origin and CORS

Each RouteBlob fingerprint is a separate `veilid://` host and persistent Chromium
partition. Same-origin requests are transported automatically. Cross-origin Veilid
calls remain subject to Chromium CORS. Ordinary public HTTPS calls stay under Chromium's
normal certificate, CSP, mixed-content, cookie, and CORS rules.

CORS protects conforming browser users; it is not server authentication and can be
forged by a custom client. Servers use real application authentication where needed.

## Request identity and retries

A transaction ID correlates VHTTP frames and protects the upstream from retry
duplication:

- A new transaction ID is durably claimed before the bridge forwards it upstream.
- Concurrent duplicates share or wait on one active execution.
- Small atomic replies may be replayed.
- Large and streamed completions leave durable tombstones.
- Completed retained IDs are not forwarded upstream again.
- An unfinished durable claim found after restart becomes an indeterminate tombstone.

That last rule is intentionally conservative. A crash may happen after the upstream
executes but before VeilidHttp records the response. The bridge therefore treats a
recovered in-flight claim as “possibly executed” and refuses to forward that same ID
automatically. This can produce zero executions when a crash happened before the
upstream received the request, but it prevents two executions under the same retained
transaction ID.

This is crash-safe at-most-once forwarding within retained persistent state—not a claim
that an arbitrary distributed system can provide perfect exactly-once execution.
Destroying bridge state, choosing a new transaction ID, or bypassing the conforming
runtime changes that guarantee.

## Resource limits

The protocol enforces bounds on:

- Frame and metadata lengths.
- IPC payloads.
- Pending plus in-flight retransmission bytes.
- Send-window frames.
- Out-of-order receive bytes.
- Decompressed logical lengths.
- Active client and server transactions.
- Completion record counts and retained response sizes.
- Overall transaction time.

The shared sender refuses a configuration whose full retransmission window cannot fit
inside its byte budget. Zstandard decoders verify final logical length and BLAKE3 digest.
A malicious site may consume resources only within the configured browser/process
limits; it cannot force one complete 70 GB object to be buffered.

## Trusted forwarding headers

The bridge removes spoofed values before adding:

```http
X-Veilid-Route-Fingerprint: <server-route-fingerprint>
X-Veilid-Origin: veilid://<server-route-fingerprint>
```

These fields help an external reverse proxy log or route traffic. They identify the
VeilidHttp receiving route, not the human user or a cryptographically authenticated
application identity.

## Upstream boundary

The transported request cannot choose an upstream authority or port. It supplies only a
validated absolute path/query. The server configuration owns the single base URL. When
the upstream is not local or requires TLS/client authentication, put NGINX, HAProxy,
Caddy, or another established gateway at that boundary instead of adding credentials or
routing policy to VeilidHttp itself.
