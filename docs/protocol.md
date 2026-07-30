# VHTTP/1 Protocol

VHTTP/1 maps one ordinary HTTP transaction onto Veilid AppCall and AppMessage
primitives. AppCall opens or completes compact exchanges; AppMessage pipelines bulk
request/response data, acknowledgements, status, and cancellation.

## Privacy-preserving bidirectional routes

The server publishes one private RouteBlob. A client imports that blob and sends the
initial `RequestOpen` through AppCall.

An AppCall provides a one-shot reply handle, but it does not provide the server with a
reusable privacy-preserving target for later response frames or acknowledgements. Every
stream-capable `RequestOpen` therefore includes a client-allocated **return RouteBlob**
in the required extension:

```text
org.veilidhttp.return-route/v1
```

The server imports that blob and uses the resulting private route target for:

- `RequestAccepted` continuation when needed.
- Request-body acknowledgements.
- `ResponseOpen`, `ResponseData`, and `ResponseEnd`.
- Status, flow-control, cancellation, and error frames after the opening AppCall.

The return RouteBlob is transport metadata, not application identity or authorization.
It is scoped to the requesting client runtime and may be rotated. Neither side falls
back to sharing or targeting a Veilid NodeId.

Compact atomic exchanges do not require a return route because the complete response is
returned directly through the AppCall reply.

## Frame header

Every frame starts with a 40-byte fixed header:

| Field | Bytes | Notes |
|---|---:|---|
| Magic | 4 | `VHTP` |
| Version | 1 | `1` |
| Frame type | 1 | See enum |
| Flags | 2 | Big-endian bit field |
| Transaction ID | 16 | Random 128-bit value |
| Sequence | 4 | Per-direction sequence |
| Cumulative ACK | 4 | Highest contiguous sequence |
| Metadata length | 2 | MessagePack bytes |
| Reserved | 2 | Must be zero in V1 |
| Payload length | 4 | Raw payload bytes |

The conservative complete-frame target is 30 KiB; Veilid's hard AppCall/AppMessage
payload ceiling is 32,768 bytes.

## Frame types

`Negotiate`, `RequestOpen`, `RequestAccepted`, `RequestData`, `RequestEnd`,
`ResponseOpen`, `ResponseData`, `ResponseEnd`, `AtomicResponse`, `Ack`, `Cancel`,
`Error`, and `Status`.

## Metadata

MessagePack carries structured metadata. Binary body bytes are never inflated through
JSON/Base64 on the Veilid wire. Namespaces are extensible:

- `http`: method, path/query, headers, status.
- `vhttp`: compression, deadline hints, priority, flush behavior, ACK window state.
- `extensions`: namespaced optional or required values such as the return RouteBlob.

Unknown optional extensions are ignored. Features marked required must be negotiated
or rejected cleanly.

## Transaction opening

### Atomic fast path

When request metadata plus the complete compressed body fit one frame, the client sends:

```text
AppCall RequestOpen
```

The server may answer through the same AppCall with:

```text
AtomicResponse
```

The atomic response remains bounded by the Veilid application-message limit. If the
upstream response does not fit, the server returns `ResponseOpen` through the AppCall and
continues the body over the imported client return route.

### Streaming path

The opening flow is:

```text
client -> server route: AppCall RequestOpen(return RouteBlob)
server -> AppCall reply: RequestAccepted or ResponseOpen
client -> server route: AppMessage RequestData × N, RequestEnd
server -> client return route: AppMessage Ack / ResponseOpen / ResponseData × N / ResponseEnd
```

A bodyless request whose response must stream may move directly from `RequestOpen` to
`ResponseOpen`.

## Compression and batching

Each request and response direction owns an independent streaming Zstandard context.
Compression occurs before fragmentation. `none` remains available for diagnostics and
negotiation.

Small writes are coalesced until a frame target, short flush deadline, stream end,
backpressure change, or explicit immediate flush. The client and server apply the
same policy.

VHTTP also defines a length-prefixed `VHB1` transport bundle. Several independently
encoded tiny frames—potentially from different transactions—can share one Veilid
AppMessage. This gains cross-request batching without sharing a compression stream or
making one transaction's retry/cancellation block another.

## Reliability

A sender retains only a bounded in-flight frame window. The receiver reports:

- Highest contiguous sequence received.
- A 64-bit selective bitmap after that point.
- Current receive-window capacity.

ACK metadata keys are:

```text
vhttp.cumulative-ack
vhttp.selective-ack
vhttp.receive-window
```

Only missing frames are retried. Retrying a transaction reuses its 128-bit ID. The
server journal therefore knows not to forward the exact same transported POST twice
merely because an answer was lost.

This is retry-safe translation, not magical exactly-once execution across arbitrary
backend side effects.

## Streaming and backpressure

Request and response streams are independently sequenced. Receivers deliver contiguous
bytes immediately and buffer only bounded out-of-order frames. Senders retain only the
configured in-flight window for retransmission.

When Chromium stops consuming a response:

1. Electron stops requesting additional IPC stream data.
2. The native sidecar advertises a smaller VHTTP receive window.
3. The bridge stops advancing its response send window.
4. The bridge stops aggressively reading the upstream HTTP response.

The same pressure propagates in reverse for uploads. A complete large object is never a
required in-memory or on-disk intermediate representation.

## HTTP mapping

The client carries:

- Method token.
- Absolute path and query only.
- End-to-end headers.
- Streaming body.

The client does not carry the upstream authority or port. The server uses its one
configured upstream base URL. Hop-by-hop headers are stripped and regenerated as
necessary.

Normal GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, and other non-upgrade method
tokens can be forwarded. `CONNECT`, WebSocket upgrades, SignalR-specific behavior,
WebTransport, and raw TCP/UDP tunnels are outside V1.

## Timeouts and cancellation

Browsers do not define a general request-timeout header. `AbortSignal` maps directly
to `Cancel`. Optional VHTTP deadline metadata may shorten the request within server
policy. Defaults are intentionally long: five minutes idle and sixty minutes overall.
An actively progressing stream is not expired merely because it is large.

Cancellation is idempotent. A repeated `Cancel` for a completed or already-cancelled
transaction is acknowledged or ignored; it never causes the upstream request to execute
again.

## Integrity

Veilid protects its own messages. VHTTP additionally completes each logical stream
with its decompressed byte length and BLAKE3 digest, detecting bad reassembly,
truncation, or implementation defects.
