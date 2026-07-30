# VHTTP/1 Protocol

VHTTP/1 maps one ordinary HTTP transaction onto Veilid AppCall and AppMessage
primitives. AppCall opens or completes compact exchanges; AppMessage pipelines bulk
request/response data, acknowledgements, status, errors, and cancellation.

## Privacy-preserving bidirectional routes

The server publishes one private RouteBlob. A client imports that blob and sends the
initial `RequestOpen` through AppCall.

An AppCall provides a one-shot reply handle, but it does not provide the server with a
reusable privacy-preserving target for later response frames or acknowledgements. Every
stream-capable `RequestOpen` therefore includes a client-allocated private return
RouteBlob.

The server imports that blob and uses the resulting private route target for:

- Request-body acknowledgements.
- `ResponseOpen`, `ResponseData`, and `ResponseEnd`.
- Flow-control, cancellation, and error frames after the opening AppCall.

The return RouteBlob is transport metadata, not application identity or authorization.
It is scoped to the client runtime and may be rotated. Neither side falls back to
sharing or targeting a Veilid NodeId.

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

Unknown frame type numbers can be decoded as `Other` and rejected/ignored according to
negotiated required-feature rules instead of crashing an older implementation.

## Metadata

MessagePack carries structured metadata. Binary body bytes are never inflated through
JSON/Base64 on either the Veilid wire or Electron IPC.

Metadata includes:

- HTTP method, path/query, headers, status.
- Compression mode and body presence.
- Private client return RouteBlob for streamed transactions.
- Receive-window and cumulative/selective ACK state.
- Final logical length/digest.
- Error/cancellation details.

Unknown optional fields may be ignored. A future required extension must be negotiated
or rejected cleanly.

## Transaction opening

### Atomic fast path

When request metadata plus the complete compressed body fit one frame, a non-streaming
caller may send:

```text
AppCall RequestOpen
```

The server can answer through the same AppCall with:

```text
AtomicResponse
```

Both sides remain bounded by the Veilid application-message limit. If the upstream
response cannot fit, the current atomic bridge returns a clear response requiring the
caller to retry using the streamed opening. It does not silently change modes after
executing the request.

The generic Electron client uses the stream-capable opening for ordinary website
requests so body/response size does not need to be predicted.

### Streaming path

```text
client -> server private route:
  AppCall RequestOpen(server HTTP metadata, client return RouteBlob)

server -> AppCall reply:
  RequestAccepted(request receive window)

client -> server private route:
  AppMessage RequestData × N / RequestEnd

server -> client private return route:
  AppMessage Ack
  AppMessage ResponseOpen
  AppMessage ResponseData × N / ResponseEnd

client -> server private route:
  AppMessage Ack / Cancel / Error
```

A bodyless request moves from `RequestOpen`/`RequestAccepted` directly to the upstream
request and streamed response.

## Compression and batching

Each request and response direction owns an independent streaming Zstandard context.
Compression occurs before fragmentation. `none` remains available for diagnostics and
future negotiation.

Logical bytes are accumulated into a conservative frame payload. Current senders flush
at explicit chunk boundaries and stream end; future adaptive timers may improve
compression/latency tuning without changing the wire format.

VHTTP defines a length-prefixed `VHB1` transport bundle. Several independently encoded
frames can share one Veilid AppMessage while the complete bundle remains below 32,768
bytes. Transactions keep independent compression, sequence, retry, and cancellation
state.

## Reliability and flow control

A sender owns a bounded pending plus retransmission-byte budget and a frame window. The
receiver reports:

- Highest contiguous sequence received.
- A 64-bit selective bitmap after that point.
- Current receive-window capacity.

Only missing/unacknowledged frames are retried, with bounded exponential delay. A
receiver delivers each sequence only once and delays completion until missing earlier
frames arrive.

Backpressure works by delaying local consumption and therefore delaying ACK progress.
The advertised receive-window size is bounded and may be extended in future versions;
V1 does not need to send a new window value for every local queue change.

Retrying a transaction reuses its 128-bit ID. The bridge persists recent completion
records:

- Concurrent duplicates do not create another upstream execution.
- Small atomic responses may be replayed.
- Large/streamed completions leave tombstones.
- A retained completed ID is not forwarded again.

This is at-most-once forwarding within retained state, not magical exactly-once backend
side effects.

## Streaming and backpressure

Request and response streams are independently sequenced. Receivers deliver contiguous
logical chunks immediately and retain only bounded compressed out-of-order data.
Senders retain only bounded pending and in-flight encoded bytes.

When Chromium stops consuming a response:

1. Its `ReadableStream` fills.
2. Electron pauses sidecar socket reads.
3. The bounded native IPC writer/channel stops accepting another chunk.
4. The client runtime does not finish consuming that VHTTP output.
5. ACK progress stops.
6. The bridge's response send window fills.
7. The bridge stops reading the upstream response stream aggressively.

Uploads apply the same chain in reverse. A complete large object is never required as an
in-memory or on-disk intermediate representation.

## HTTP mapping

The client carries:

- Method token.
- Absolute path and query only.
- End-to-end headers.
- Streaming body.

The client does not carry the upstream authority or port. The server uses its one
configured upstream base URL. Hop-by-hop headers are stripped and regenerated as
necessary.

Normal GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS, and other non-upgrade method tokens
can be forwarded. `CONNECT`, WebSocket upgrades, SignalR-specific behavior,
WebTransport, and raw TCP/UDP tunnels are outside V1.

## Timeouts and cancellation

Browsers do not define a general request-timeout header. `AbortSignal` maps directly to
`Cancel`. Defaults are intentionally long because multi-hop routes may be slow. An
actively progressing large stream is not rejected merely for being large.

Cancellation is idempotent. Repeated cancellation for an already completed transaction
is ignored and never causes the upstream request to execute again.

## Integrity

Veilid protects its own messages. VHTTP additionally completes each logical body stream
with its decompressed byte length and BLAKE3 digest, detecting bad reassembly,
truncation, decompression mistakes, or implementation defects.
