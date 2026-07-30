# VeilidHttp Architecture

## Purpose

VeilidHttp is a bidirectional HTTP interoperability layer over Veilid private routes.
Both ends remain ordinary:

- The client application behaves like a normal site in Chromium.
- The backend receives an ordinary HTTP request at one configured upstream URL.
- VHTTP/1 owns only the translation required to cross Veilid's message boundary.

## Components

```text
Electron main process
├── trusted navigation shell
├── sandboxed site WebContentsView
├── privileged veilid:// protocol handler
└── native Rust sidecar
    ├── VHTTP client engine
    ├── route registry
    └── Veilid native adapter

Debian 13.6 container
├── official veilid-server process
└── veilid-http-bridge
    ├── Veilid remote adapter
    ├── VHTTP server engine
    └── one HTTP upstream
```

## Scope boundary

VeilidHttp owns framing, compression, fragmentation, ordering, selective ACKs,
retry decisions, deduplication, cancellation, expiration, bounded buffering,
backpressure, and HTTP reconstruction.

It does not own routing among backend services. One upstream is configured. A user
who needs several services points that one upstream at NGINX, HAProxy, Caddy,
Traefik, or an application gateway.

## Core crates

- `veilid-http-wire`: deterministic VHTTP frame encoding and protocol metadata.
- `veilid-http-core`: batching, receive windows, reassembly, compression limits,
  transaction state, and retry-safe completion tracking.
- `veilid-http-transport`: abstract AppCall/AppMessage contract.
- `veilid-http-http`: HTTP head models, hop-by-hop filtering, upstream URL building,
  and trusted forwarding metadata.

## Transport model

Small transactions may use AppCall and return an atomic response. Large or streaming
transactions open with AppCall and use AppMessage for pipelined body frames and
selective acknowledgements. A successful AppMessage dispatch is not treated as an
application-level completion; VHTTP maintains its own receipts and transaction state.

Veilid's 32,768-byte application payload ceiling is never filled to the edge. The
initial complete-frame target is 30 KiB, leaving room for VHTTP metadata and future
extensions.

## Streaming

Streaming is end-to-end and bounded:

```text
browser ReadableStream
↕ bounded native IPC
↕ zstd stream + VHTTP frames
↕ Veilid AppMessages
↕ zstd stream + bounded HTTP body
↕ upstream application
```

No design step requires materializing a 70 GB model in memory or on disk. A bounded
window is retained for retransmission; optional spool storage is a safety valve, not
the normal path for a complete object.

## Browser origin

The preferred origin is `veilid://<route-fingerprint>/`. Electron registers the
scheme as standard, secure, Fetch-capable, CORS-enabled, stream-capable, and service-
worker-capable. Each site uses an isolated persistent Electron partition.

The compatibility fallback is `http://<fingerprint>.veilid.localhost:<stable-port>/`.
A changing port changes origin and therefore is not acceptable for persistent service
workers or IndexedDB.

## Route identity

The initial site identifier is the first 128 bits of BLAKE3 over the complete binary
RouteBlob, encoded as lowercase unpadded Base32. The same blob produces the same ID.
Route rotation may change the origin; stable application identity is intentionally a
future signed-manifest layer rather than a hidden V1 promise.

## Security

The loaded site receives normal browser powers and no Node/Electron/native powers.
The renderer is sandboxed, context isolated, and has Node integration disabled.
Standard uploads and downloads remain available because they are user-mediated browser
operations, not arbitrary filesystem access.

## Forwarded route metadata

The bridge strips any incoming spoofed route headers and adds a trusted value:

```http
X-Veilid-Route-Fingerprint: <128-bit-base32-fingerprint>
X-Veilid-Origin: veilid://<fingerprint>
```

A reverse proxy may route or log using these values. They are transport metadata, not
an authentication credential. Applications still need real authentication.

## Hostile-network defaults

- 5 minute idle timeout.
- 60 minute overall timeout.
- AbortSignal-driven cancellation from browser requests.
- 32-frame initial send window.
- Selective ACK bitmap.
- Retry only missing frames.
- 15 minute completed transaction retention.
- Bounded decompression and frame sizes.

All values are configurable, but defaults assume multi-hop latency rather than a fast
local LAN.
