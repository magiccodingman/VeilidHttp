# Streaming and Large Objects

VHTTP is designed so a large upload or download never has to exist as one object in
memory or on disk at any translation point.

```text
Chromium request/response ReadableStream
  ↕ authenticated bounded binary IPC
native VHTTP request/response channels
  ↕ streaming Zstandard
bounded VHTTP pending + retransmission window
  ↕ bundled Veilid AppMessages
bounded VHTTP reassembly + streaming Zstandard
  ↕ bounded HTTP body channel
one configured upstream
```

## Memory bound

The sender owns a single byte budget covering both:

- Compressed payloads waiting outside the send window.
- Complete encoded frames retained for retransmission.

Construction fails unless the configured full frame window can fit inside that byte
budget. ACK processing releases the exact encoded frame bytes. The receiver emits
contiguous decoded bytes immediately and stores only a configured amount of compressed
out-of-order data.

With 32 frames near 30 KiB, a direction requires roughly one MiB for its complete
retransmission window before compression and channel overhead. The default pending
budget is eight MiB. A 70 GB model and a 70 MB archive follow the same per-transaction
memory model; concurrency and configured windows determine peak process memory.

Both the Electron sidecar and bridge cap simultaneously active transactions so a
malicious site cannot multiply the per-transaction bounds without limit.

## Backpressure

Backpressure crosses every boundary:

1. Chromium stops pulling from the response `ReadableStream`.
2. Electron pauses the sidecar socket when its stream high-water mark is full.
3. The native IPC writer blocks on its bounded outbound queue.
4. The client runtime does not accept another response chunk into its bounded channel.
5. That response frame is not acknowledged yet.
6. The bridge's response window stops advancing.
7. The bridge stops reading the upstream response stream aggressively.

Uploads apply the same logic in reverse: Node socket drain, bounded sidecar request
channel, VHTTP send window, bounded bridge request channel, then upstream HTTP body.

## Compression and framing

Each request direction and response direction has its own Zstandard stream. Compression
happens before fragmentation. Unrelated HTTP transactions never share a compression
history, retry state, or cancellation boundary.

Tiny logical writes are coalesced into VHTTP data frames. Several complete frames—even
from ACK handling within one incoming message—may be encoded into one Veilid transport
bundle without exceeding the 32,768-byte Veilid message ceiling. This reduces multi-hop
overhead while retaining independent transaction IDs and sequence spaces.

## Reliability

- AppCall opens a streamed transaction and returns `RequestAccepted`.
- The opening includes a private client return RouteBlob.
- AppMessages carry request data, response open/data/end, ACKs, errors, and cancellation.
- Cumulative plus 64-bit selective ACKs release received frames.
- Missing frames are retransmitted with bounded exponential delay.
- Duplicate data frames are not emitted twice.
- Final logical byte length and BLAKE3 digest verify decompression/reassembly.
- AbortSignal cancellation propagates to the upstream request.

## Persistence and restart

V1 deliberately does **not** spool a complete stream to disk. Restarting either endpoint
fails in-progress transfers rather than keeping a potentially enormous hidden copy.
Applications may retry according to their own semantics.

The bridge does persist completed transaction records:

- Small atomic replies may be replayed.
- Large and streamed transactions leave tombstones.
- A duplicate retained transaction ID is not silently forwarded upstream again.

Full mid-stream crash resume would require durable sender/receiver journals and a bounded
spool policy. That remains a separate future protocol capability rather than a hidden
V1 promise.
