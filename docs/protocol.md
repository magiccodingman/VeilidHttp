# VHTTP/1 Protocol

VHTTP/1 maps one ordinary HTTP transaction onto Veilid AppCall and AppMessage
primitives. AppCall opens or completes compact exchanges; AppMessage pipelines bulk
request/response data, acknowledgements, status, and cancellation.

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
- `vhttp`: compression, deadline hints, priority, flush behavior.
- `extensions`: namespaced optional future values.

Unknown optional extensions are ignored. Features marked required must be negotiated
or rejected cleanly.

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
- Future receive-window/backpressure information.

Only missing frames are retried. Retrying a transaction reuses its 128-bit ID. The
server journal therefore knows not to forward the exact same transported POST twice
merely because an answer was lost.

This is retry-safe translation, not magical exactly-once execution across arbitrary
backend side effects.

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

## Integrity

Veilid protects its own messages. VHTTP additionally completes each logical stream
with its decompressed byte length and BLAKE3 digest, detecting bad reassembly,
truncation, or implementation defects.
