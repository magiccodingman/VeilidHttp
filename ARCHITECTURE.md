# VeilidHttp Architecture

## Purpose

VeilidHttp is a bidirectional HTTP interoperability layer over Veilid private routes.
Both ends remain ordinary:

- The client application behaves like a normal site in Chromium.
- The backend receives an ordinary HTTP request at one configured upstream URL.
- VHTTP/1 owns only the translation required to cross Veilid's message boundary.

```text
ordinary PWA / SPA / static / SSR site
        ↕ HTTP semantics
locked-down Electron + native Rust client
        ↕ VHTTP/1
Veilid private server and client-return routes
        ↕ VHTTP/1
one Debian container: veilid-server + Rust bridge
        ↕ ordinary HTTP
one configured upstream application or reverse proxy
```

## Components

```text
Electron main process
├── trusted navigation shell
├── sandboxed site WebContentsView
├── validated 127.0.0.1 *.veilid.localhost origin server
└── authenticated private binary IPC
    └── native Rust sidecar
        ├── embedded veilid-core 0.5.5
        ├── rotating private return route
        ├── VHTTP client engine
        └── bounded streaming channels

Debian 13.6 container
├── official pinned veilid-server 0.5.5
└── veilid-http-bridge
    ├── multiplexed remote API adapter
    ├── published private receiving route
    ├── VHTTP server engine
    ├── persistent completion coordinator
    └── one HTTP upstream
```

## Scope boundary

VeilidHttp owns framing, compression, fragmentation, transport bundling, ordering,
selective ACKs, retry decisions, persistent duplicate suppression, cancellation,
expiration, bounded buffering, backpressure, and HTTP reconstruction.

It does not own routing among backend services. One upstream is configured. A user who
needs several services points that upstream at NGINX, HAProxy, Caddy, Traefik, or an
application gateway. VeilidHttp also does not own HTTP caching, CDN behavior, load
balancing, authentication, WebSockets, SignalR, WebTransport, CONNECT, or arbitrary
TCP/UDP tunnels.

## Core crates

- `veilid-http-wire`: deterministic VHTTP frames and multi-frame Veilid bundles.
- `veilid-http-core`: batching, windows, ACKs, retries, reassembly, compression, and
  integrity helpers.
- `veilid-http-engine`: shared bounded outgoing/incoming body state machines.
- `veilid-http-stream`: MessagePack stream metadata and frame codecs.
- `veilid-http-route`: RouteBlob Base64URL plus BLAKE3/Base32 site identities.
- `veilid-http-http`: HTTP policy, hop-by-hop filtering, atomic codec, one-upstream URL,
  and trusted forwarding metadata.
- `veilid-http-ipc`: authenticated framed binary sidecar transport.
- `veilid-http-transport`: abstract AppCall/AppMessage contract.
- `veilid-http-veilid-native`: embedded client adapter.
- `veilid-http-veilid-remote`: official server remote API adapter.

## Transport model

The generic Electron client uses the streamed opening for normal website requests:

1. AppCall `RequestOpen` to the server private route.
2. The opening carries HTTP metadata and the client's private return RouteBlob.
3. AppCall reply `RequestAccepted` advertises request receive capacity.
4. AppMessages carry request data/end and selective ACKs.
5. AppMessages to the client return route carry response open/data/end.
6. Client ACKs, cancellation, and errors travel back to the server route.

A compact atomic AppCall/AtomicResponse path remains available for bounded control/tests.
It does not guess response size or silently change modes after executing a request.

Veilid's 32,768-byte application payload ceiling is never filled to the edge. The
complete-frame target is 30 KiB. Several complete VHTTP frames can share one length-
prefixed Veilid AppMessage bundle while preserving independent transaction state.

## Streaming and memory

```text
Chromium ReadableStream
↕ Electron-main loopback HTTP stream
↕ bounded socket/pipe IPC
↕ bounded native channels
↕ independent streaming zstd context
↕ pending + retained retransmission byte budget
↕ Veilid AppMessage bundles
↕ bounded reassembly and zstd decoder
↕ bounded upstream HTTP body
```

No step materializes a complete 70 GB object. The sender byte budget covers compressed
bytes waiting outside the window plus complete encoded frames retained for retry. The
receiver emits contiguous bytes and retains only bounded compressed out-of-order data.
The full configured frame window must fit inside the pending-byte budget or construction
fails.

Backpressure propagates by delaying local consumption and ACK progress. Object size does
not determine peak memory; active transaction count, window sizes, compression buffers,
and out-of-order limits do. Both client and bridge cap active transactions.

V1 does not pretend to resume a transfer after process restart and does not hide complete
objects in disk spool. Persistent completion records protect recent upstream execution,
not unfinished body bytes.

## At-most-once forwarding

Every transaction uses a 128-bit ID. The bridge coordinates atomic and streamed paths:

- A durable pre-execution claim is written before forwarding a new transaction.
- Concurrent duplicates do not create another upstream request.
- Capacity exhaustion is retryable and does not poison a new transaction ID.
- Small atomic responses may be retained and replayed.
- Large or streamed completions leave durable tombstones.
- A recovered in-flight claim becomes an indeterminate tombstone after restart.
- Recent completed or indeterminate IDs are not forwarded again after a lost reply or
  restart.

This is an honest at-most-once boundary within retained bridge state, not a claim of
perfect distributed exactly-once side effects. When crash recovery cannot determine
whether the upstream executed a request, it deliberately refuses automatic replay.

## Browser origin

Each RouteBlob fingerprint is exposed to Chromium at:

```text
http://<fingerprint>.veilid.localhost:<stable-port>/
```

Electron main owns a listener bound only to `127.0.0.1`. It validates the Host header,
extracts exactly one 26-character Base32 fingerprint, and streams the request through
authenticated sidecar IPC. Each site also uses an isolated persistent Electron
partition.

This origin model is required because Chromium Cache Storage rejects requests whose URL
uses a custom `veilid:` scheme, even when that scheme is registered as standard, secure,
Fetch-capable, CORS-enabled, stream-capable, code-cache-capable, and service-worker-
capable. Loopback localhost origins retain trustworthy-context browser behavior without
fake certificates or disabled web security.

A real Electron smoke harness validates service workers, Cache Storage, IndexedDB,
CORS, WebAssembly, secure-context behavior, persistent sessions, and streamed responses
on the implemented localhost-origin model.

## Route identity and lifecycle

The V1 site identifier is the first 128 bits of BLAKE3 over the complete binary
RouteBlob, encoded as 26 lowercase unpadded Base32 characters. The same blob produces
the same ID.

The server allocates and atomically publishes a reliable private RouteBlob. The client
imports it and separately allocates a private return route. A dead receiving or return
route triggers rotation. Route rotation can change the server fingerprint/origin;
stable signed application identity and discovery remain future layers rather than a
hidden protocol promise.

## Security

The loaded site receives normal Chromium powers and no Node/Electron/native powers. The
renderer is sandboxed, context-isolated, and has Node integration disabled. Standard
uploads and downloads remain available because they are user-mediated browser
operations, not arbitrary filesystem access.

Electron main and the sidecar communicate through a random mode-0600 Unix socket or
Windows named pipe authenticated by a one-launch secret. IPC metadata/payload sizes are
bounded. The loopback HTTP server accepts only validated site hosts on `127.0.0.1`.
Public HTTPS remains ordinary Chromium networking. Cross-origin Veilid site requests
remain subject to Chromium CORS.

## Forwarded route metadata

The bridge strips any spoofed values and adds:

```http
X-Veilid-Route-Fingerprint: <128-bit-base32-fingerprint>
X-Veilid-Origin: veilid://<fingerprint>
```

`X-Veilid-Origin` is a canonical overlay identity for backend metadata; it is not the
literal Chromium address bar URL. A reverse proxy may log or route using these values.
They describe the receiving VeilidHttp route and are not user authentication
credentials.

## Hostile-network defaults

- 5 minute idle policy and 60 minute overall transaction timeout.
- AbortSignal-driven cancellation.
- 32-frame initial windows and 64-bit selective ACK maps.
- Retry only missing/unacknowledged frames with bounded exponential delay.
- 8 MiB pending/retransmission and 4 MiB out-of-order budgets per direction.
- 15 minute persistent completion retention.
- Bounded decompression, frame, IPC, transaction, and completion counts.

Defaults assume multi-hop latency rather than a fast LAN. Real-network profiling and
route-lifecycle observation remain required before a production release.
