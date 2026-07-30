# Streaming and Large Objects

VHTTP is designed so a large upload or download does not need to exist as one object
in memory or on disk at any translation point.

```text
Chromium ReadableStream
  ↕ bounded Electron/native IPC
streaming Zstandard encoder/decoder
  ↕ bounded VHTTP send/receive windows
Veilid AppMessages
  ↕ bounded VHTTP send/receive windows
streaming Zstandard encoder/decoder
  ↕ HTTP body stream
configured upstream
```

## Memory bound

The sender retains only unacknowledged encoded frames. With a 32-frame window and
roughly 30 KiB frames, one active direction retains around one MiB plus metadata—not
the complete file. The receiver emits contiguous decoded bytes immediately and stores
only bounded out-of-order frames.

A 70 GB model and a 70 MB archive follow the same memory model. Concurrency, frame
size, compression buffers, and window sizes determine peak memory, not total object
size.

## Backpressure

Backpressure crosses every boundary:

1. Chromium stops pulling from the response `ReadableStream`.
2. Electron stops reading response chunks from the native sidecar.
3. The client advertises a smaller/zero receive window.
4. The server stops advancing its send window.
5. The bridge stops aggressively reading the upstream response body.

The reverse direction applies to uploads.

## Disk spooling

Disk spooling is a bounded recovery tool for out-of-order/retry state, not the normal
way to assemble an entire object. Administrators may cap spool bytes and active
transactions. Once a contiguous chunk is handed downstream and no longer needed for
retry, it can be released.

## Adaptive batching

Tiny writes from one stream are briefly coalesced before framing. Tiny frames from
several streams can then be placed in one transport bundle. This reduces multi-hop
message overhead without applying a long artificial delay or coupling unrelated Zstd
histories.

## Resume semantics

V1 retains transaction IDs, ACK state, and recently completed responses long enough
to survive ordinary retries. Full process-crash resume for arbitrary multi-gigabyte
streams is a separate milestone because it requires durable sender and receiver
journals. The in-memory algorithms are already structured around that future state.
