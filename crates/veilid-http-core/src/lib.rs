//! Shared VHTTP transfer state, bounded batching, acknowledgements, and compression.

use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;
use veilid_http_wire::{DEFAULT_FRAME_LIMIT, HEADER_LEN};

/// Maximum selective acknowledgement width.
pub const ACK_BITMAP_BITS: u32 = 64;

/// Compact acknowledgement state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckSnapshot {
    /// Highest contiguous sequence received. `None` means sequence zero is absent.
    pub cumulative: Option<u32>,
    /// Bits for the next 64 sequence numbers after `cumulative`.
    pub selective: u64,
}

/// Receive-side sequence tracker.
#[derive(Debug, Default)]
pub struct ReceiveWindow {
    received: BTreeSet<u32>,
    next_contiguous: u32,
}

impl ReceiveWindow {
    /// Record a sequence number. Returns false for a duplicate.
    pub fn record(&mut self, sequence: u32) -> bool {
        let inserted = self.received.insert(sequence);
        while self.received.remove(&self.next_contiguous) {
            self.next_contiguous = self.next_contiguous.saturating_add(1);
        }
        inserted
    }

    /// Build a cumulative plus selective acknowledgement snapshot.
    pub fn snapshot(&self) -> AckSnapshot {
        let cumulative = self.next_contiguous.checked_sub(1);
        let base = self.next_contiguous;
        let mut selective = 0_u64;
        for offset in 0..ACK_BITMAP_BITS {
            if self.received.contains(&base.saturating_add(offset)) {
                selective |= 1_u64 << offset;
            }
        }
        AckSnapshot {
            cumulative,
            selective,
        }
    }

    /// Determine whether a particular sequence is acknowledged.
    pub fn acknowledges(snapshot: AckSnapshot, sequence: u32) -> bool {
        if snapshot.cumulative.is_some_and(|value| sequence <= value) {
            return true;
        }
        let base = snapshot
            .cumulative
            .map_or(0, |value| value.saturating_add(1));
        let Some(offset) = sequence.checked_sub(base) else {
            return false;
        };
        offset < ACK_BITMAP_BITS && (snapshot.selective & (1_u64 << offset)) != 0
    }
}

/// Bounded ordered reassembly result.
#[derive(Debug, Default)]
pub struct Reassembler {
    next_sequence: u32,
    pending: BTreeMap<u32, Bytes>,
    pending_bytes: usize,
    max_pending_bytes: usize,
}

impl Reassembler {
    /// Create a reassembler with a hard out-of-order byte limit.
    #[must_use]
    pub fn new(max_pending_bytes: usize) -> Self {
        Self {
            max_pending_bytes,
            ..Self::default()
        }
    }

    /// Insert a frame and return newly contiguous chunks in delivery order.
    pub fn push(&mut self, sequence: u32, bytes: Bytes) -> Result<Vec<Bytes>, CoreError> {
        if sequence < self.next_sequence || self.pending.contains_key(&sequence) {
            return Ok(Vec::new());
        }
        let new_total = self.pending_bytes.saturating_add(bytes.len());
        if new_total > self.max_pending_bytes {
            return Err(CoreError::PendingLimit {
                actual: new_total,
                limit: self.max_pending_bytes,
            });
        }
        self.pending_bytes = new_total;
        self.pending.insert(sequence, bytes);

        let mut ready = Vec::new();
        while let Some(chunk) = self.pending.remove(&self.next_sequence) {
            self.pending_bytes -= chunk.len();
            ready.push(chunk);
            self.next_sequence = self.next_sequence.saturating_add(1);
        }
        Ok(ready)
    }
}

/// Coalesces small writes without crossing a logical transaction boundary.
#[derive(Debug)]
pub struct FrameBatcher {
    target_payload: usize,
    buffer: Vec<u8>,
}

impl FrameBatcher {
    /// Construct using a complete-frame limit and reserved metadata bytes.
    #[must_use]
    pub fn new(frame_limit: usize, reserved_metadata: usize) -> Self {
        let target_payload = frame_limit
            .min(DEFAULT_FRAME_LIMIT)
            .saturating_sub(HEADER_LEN + reserved_metadata)
            .max(1);
        Self {
            target_payload,
            buffer: Vec::with_capacity(target_payload),
        }
    }

    /// Push bytes and return every complete payload frame produced.
    pub fn push(&mut self, mut input: &[u8]) -> Vec<Bytes> {
        let mut output = Vec::new();
        while !input.is_empty() {
            let remaining = self.target_payload - self.buffer.len();
            let take = remaining.min(input.len());
            self.buffer.extend_from_slice(&input[..take]);
            input = &input[take..];
            if self.buffer.len() == self.target_payload {
                output.push(Bytes::from(std::mem::take(&mut self.buffer)));
                self.buffer = Vec::with_capacity(self.target_payload);
            }
        }
        output
    }

    /// Flush the current partial frame.
    pub fn flush(&mut self) -> Option<Bytes> {
        (!self.buffer.is_empty()).then(|| {
            let bytes = std::mem::take(&mut self.buffer);
            self.buffer = Vec::with_capacity(self.target_payload);
            Bytes::from(bytes)
        })
    }

    /// Payload target after header and metadata reservation.
    #[must_use]
    pub const fn target_payload(&self) -> usize {
        self.target_payload
    }
}

/// One frame retained until acknowledged.
#[derive(Debug, Clone)]
pub struct InFlightFrame {
    /// Per-direction sequence number.
    pub sequence: u32,
    /// Encoded frame bytes ready for transport.
    pub bytes: Bytes,
    /// Host-supplied monotonic time of the most recent send.
    pub last_sent_ms: Option<u64>,
    /// Number of dispatch attempts.
    pub attempts: u32,
}

/// Bounded sender-side window. Large streams never retain more than `capacity` frames.
#[derive(Debug)]
pub struct SendWindow {
    capacity: usize,
    next_sequence: u32,
    in_flight: BTreeMap<u32, InFlightFrame>,
    in_flight_bytes: usize,
}

impl SendWindow {
    /// Create a send window with a non-zero frame capacity.
    pub fn new(capacity: usize) -> Result<Self, CoreError> {
        if capacity == 0 {
            return Err(CoreError::InvalidWindowCapacity);
        }
        Ok(Self {
            capacity,
            next_sequence: 0,
            in_flight: BTreeMap::new(),
            in_flight_bytes: 0,
        })
    }

    /// Whether another frame can enter the send window.
    #[must_use]
    pub fn has_capacity(&self) -> bool {
        self.in_flight.len() < self.capacity
    }

    /// Allocate a sequence and retain a frame until it is acknowledged.
    pub fn enqueue(&mut self, bytes: Bytes) -> Result<u32, CoreError> {
        if !self.has_capacity() {
            return Err(CoreError::SendWindowFull {
                capacity: self.capacity,
            });
        }
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CoreError::SequenceExhausted)?;
        self.in_flight_bytes = self.in_flight_bytes.saturating_add(bytes.len());
        self.in_flight.insert(
            sequence,
            InFlightFrame {
                sequence,
                bytes,
                last_sent_ms: None,
                attempts: 0,
            },
        );
        Ok(sequence)
    }

    /// Mark a retained frame as dispatched at `now_ms`.
    pub fn mark_sent(&mut self, sequence: u32, now_ms: u64) -> bool {
        let Some(frame) = self.in_flight.get_mut(&sequence) else {
            return false;
        };
        frame.last_sent_ms = Some(now_ms);
        frame.attempts = frame.attempts.saturating_add(1);
        true
    }

    /// Apply cumulative/selective acknowledgement and return removed sequence numbers.
    pub fn acknowledge(&mut self, snapshot: AckSnapshot) -> Vec<u32> {
        let acknowledged: Vec<u32> = self
            .in_flight
            .keys()
            .copied()
            .filter(|sequence| ReceiveWindow::acknowledges(snapshot, *sequence))
            .collect();
        for sequence in &acknowledged {
            if let Some(frame) = self.in_flight.remove(sequence) {
                self.in_flight_bytes -= frame.bytes.len();
            }
        }
        acknowledged
    }

    /// Frames never sent or whose retransmission deadline elapsed.
    #[must_use]
    pub fn ready_to_send(&self, now_ms: u64, retry_after_ms: u64) -> Vec<&InFlightFrame> {
        self.in_flight
            .values()
            .filter(|frame| {
                frame
                    .last_sent_ms
                    .is_none_or(|sent| now_ms.saturating_sub(sent) >= retry_after_ms)
            })
            .collect()
    }

    /// Number of retained frames.
    #[must_use]
    pub fn len(&self) -> usize {
        self.in_flight.len()
    }

    /// Whether no frames await acknowledgement.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    /// Total retained encoded bytes.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.in_flight_bytes
    }
}

/// Hostile-network retry delay with bounded exponential growth.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// First retry delay.
    pub initial_ms: u64,
    /// Maximum retry delay.
    pub maximum_ms: u64,
}

impl RetryPolicy {
    /// Delay for a one-based attempt count. Attempt zero uses the initial delay.
    #[must_use]
    pub fn delay_ms(self, attempts: u32) -> u64 {
        let shift = attempts.min(20);
        self.initial_ms
            .saturating_mul(1_u64 << shift)
            .min(self.maximum_ms)
    }
}

/// Lifecycle state retained for retry-safe forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Receiving request metadata/body.
    Receiving,
    /// Forwarded to the configured HTTP upstream.
    Forwarding,
    /// Sending the upstream response.
    Responding,
    /// Response completed and temporarily retained.
    Completed,
    /// Cancelled by either side.
    Cancelled,
    /// Expired after inactivity or overall deadline.
    Expired,
}

/// Minimal transaction journal used by both adapters.
#[derive(Debug, Default)]
pub struct TransactionJournal {
    entries: HashMap<[u8; 16], JournalEntry>,
}

/// Journal entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalEntry {
    /// Current lifecycle state.
    pub state: TransactionState,
    /// Last activity as monotonic milliseconds supplied by the host.
    pub last_activity_ms: u64,
}

impl TransactionJournal {
    /// Insert a new transaction; returns false if it already exists.
    pub fn begin(&mut self, id: [u8; 16], now_ms: u64) -> bool {
        self.entries
            .insert(
                id,
                JournalEntry {
                    state: TransactionState::Receiving,
                    last_activity_ms: now_ms,
                },
            )
            .is_none()
    }

    /// Update state and activity timestamp.
    pub fn transition(&mut self, id: &[u8; 16], state: TransactionState, now_ms: u64) -> bool {
        let Some(entry) = self.entries.get_mut(id) else {
            return false;
        };
        entry.state = state;
        entry.last_activity_ms = now_ms;
        true
    }

    /// Read the current entry.
    #[must_use]
    pub fn get(&self, id: &[u8; 16]) -> Option<JournalEntry> {
        self.entries.get(id).copied()
    }

    /// Expire non-completed entries idle for at least `idle_ms`.
    pub fn expire_idle(&mut self, now_ms: u64, idle_ms: u64) -> Vec<[u8; 16]> {
        let mut expired = Vec::new();
        for (id, entry) in &mut self.entries {
            if !matches!(
                entry.state,
                TransactionState::Completed | TransactionState::Cancelled
            ) && now_ms.saturating_sub(entry.last_activity_ms) >= idle_ms
            {
                entry.state = TransactionState::Expired;
                expired.push(*id);
            }
        }
        expired
    }
}

/// Compress one bounded buffer. Streaming adapters use the same zstd stream format.
pub fn compress(bytes: &[u8], level: i32) -> Result<Vec<u8>, CoreError> {
    zstd::stream::encode_all(bytes, level).map_err(CoreError::Compression)
}

/// Decompress with a hard logical output limit.
pub fn decompress_bounded(bytes: &[u8], max_output: usize) -> Result<Vec<u8>, CoreError> {
    use std::io::Read;
    let decoder = zstd::stream::read::Decoder::new(bytes).map_err(CoreError::Compression)?;
    let mut limited = decoder.take(max_output.saturating_add(1) as u64);
    let mut output = Vec::new();
    limited
        .read_to_end(&mut output)
        .map_err(CoreError::Compression)?;
    if output.len() > max_output {
        return Err(CoreError::DecompressedLimit {
            actual: output.len(),
            limit: max_output,
        });
    }
    Ok(output)
}

/// Compute a final logical-stream digest.
#[must_use]
pub fn stream_digest(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// Core transfer errors.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A send window must retain at least one frame.
    #[error("send window capacity must be non-zero")]
    InvalidWindowCapacity,
    /// No more frames fit in the current send window.
    #[error("send window is full at {capacity} frames")]
    SendWindowFull {
        /// Maximum number of frames the window can retain.
        capacity: usize,
    },
    /// Per-direction sequence number space was exhausted.
    #[error("sequence number space exhausted")]
    SequenceExhausted,
    /// Out-of-order buffering exceeded its configured bound.
    #[error("pending reassembly bytes {actual} exceed limit {limit}")]
    PendingLimit {
        /// Pending bytes observed after the rejected insertion.
        actual: usize,
        /// Configured maximum pending bytes.
        limit: usize,
    },
    /// Zstandard operation failed.
    #[error("zstandard operation failed: {0}")]
    Compression(std::io::Error),
    /// Decoded data exceeded its configured logical bound.
    #[error("decompressed bytes {actual} exceed limit {limit}")]
    DecompressedLimit {
        /// Decoded bytes observed or declared.
        actual: usize,
        /// Configured maximum decoded bytes.
        limit: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_window_tracks_gaps() {
        let mut window = ReceiveWindow::default();
        assert!(window.record(0));
        assert!(window.record(2));
        let snapshot = window.snapshot();
        assert_eq!(snapshot.cumulative, Some(0));
        assert!(!ReceiveWindow::acknowledges(snapshot, 1));
        assert!(ReceiveWindow::acknowledges(snapshot, 2));
        assert!(window.record(1));
        assert_eq!(window.snapshot().cumulative, Some(2));
    }

    #[test]
    fn reassembler_delivers_in_order_and_deduplicates() {
        let mut reassembler = Reassembler::new(1024);
        assert!(
            reassembler
                .push(1, Bytes::from_static(b"b"))
                .unwrap()
                .is_empty()
        );
        let ready = reassembler.push(0, Bytes::from_static(b"a")).unwrap();
        assert_eq!(
            ready,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
        assert!(
            reassembler
                .push(1, Bytes::from_static(b"b"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn batcher_keeps_memory_bounded_to_frame_target() {
        let mut batcher = FrameBatcher::new(1024, 128);
        let frames = batcher.push(&vec![7; 5000]);
        assert!(
            frames
                .iter()
                .all(|frame| frame.len() == batcher.target_payload())
        );
        assert!(batcher.flush().is_some());
    }

    #[test]
    fn send_window_stays_bounded_for_arbitrarily_long_streams() {
        let mut window = SendWindow::new(4).unwrap();
        let mut produced = 0_u32;
        for _ in 0..10_000 {
            while window.has_capacity() {
                let sequence = window.enqueue(Bytes::from(vec![0_u8; 1024])).unwrap();
                assert!(window.mark_sent(sequence, u64::from(sequence)));
                produced += 1;
            }
            assert_eq!(window.len(), 4);
            assert_eq!(window.retained_bytes(), 4096);
            let first = *window.in_flight.keys().next().unwrap();
            window.acknowledge(AckSnapshot {
                cumulative: Some(first),
                selective: 0,
            });
        }
        assert!(produced > 10_000);
    }

    #[test]
    fn send_window_selectively_releases_frames() {
        let mut window = SendWindow::new(8).unwrap();
        for _ in 0..4 {
            window.enqueue(Bytes::from_static(b"frame")).unwrap();
        }
        let removed = window.acknowledge(AckSnapshot {
            cumulative: Some(0),
            selective: 0b10,
        });
        assert_eq!(removed, vec![0, 2]);
        assert_eq!(window.len(), 2);
    }

    #[test]
    fn retry_policy_caps_exponential_backoff() {
        let policy = RetryPolicy {
            initial_ms: 500,
            maximum_ms: 30_000,
        };
        assert_eq!(policy.delay_ms(0), 500);
        assert_eq!(policy.delay_ms(3), 4000);
        assert_eq!(policy.delay_ms(30), 30_000);
    }

    #[test]
    fn zstd_round_trip_and_limit() {
        let original = vec![42_u8; 256 * 1024];
        let compressed = compress(&original, 3).unwrap();
        assert_eq!(
            decompress_bounded(&compressed, original.len()).unwrap(),
            original
        );
        assert!(matches!(
            decompress_bounded(&compressed, 1024),
            Err(CoreError::DecompressedLimit { .. })
        ));
    }

    #[test]
    fn journal_prevents_duplicate_begin() {
        let id = [9; 16];
        let mut journal = TransactionJournal::default();
        assert!(journal.begin(id, 10));
        assert!(!journal.begin(id, 11));
        assert!(journal.transition(&id, TransactionState::Forwarding, 12));
        assert_eq!(
            journal.get(&id).unwrap().state,
            TransactionState::Forwarding
        );
    }
}
