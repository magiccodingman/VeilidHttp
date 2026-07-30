//! Host-neutral bounded send/receive state for VHTTP request and response bodies.

use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;
use veilid_http_core::{FrameBatcher, ReceiveWindow, Reassembler, RetryPolicy};
use veilid_http_stream::{
    Ack, CompressionMode, DecodedFrame, StreamDecoder, StreamDirection, StreamEncoder,
    StreamEnd, decode, encode_data, encode_end,
};

/// One encoded frame retained until its cumulative/selective acknowledgement arrives.
#[derive(Debug, Clone)]
pub struct RetainedFrame {
    /// Per-direction sequence number, including the final end frame.
    pub sequence: u32,
    /// Complete encoded VHTTP frame.
    pub encoded: Bytes,
    /// Monotonic timestamp of the most recent dispatch.
    pub last_sent_ms: Option<u64>,
    /// Number of dispatch attempts.
    pub attempts: u32,
}

#[derive(Debug)]
enum PendingItem {
    Data(Bytes),
    End(StreamEnd),
}

impl PendingItem {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Data(bytes) => bytes.len(),
            Self::End(_) => 64,
        }
    }
}

/// Bounded outgoing logical body stream.
#[derive(Debug)]
pub struct OutboundBody {
    transaction_id: [u8; 16],
    direction: StreamDirection,
    encoder: Option<StreamEncoder>,
    batcher: FrameBatcher,
    pending: VecDeque<PendingItem>,
    pending_bytes: usize,
    max_pending_bytes: usize,
    local_window: usize,
    peer_window: usize,
    next_sequence: u32,
    in_flight: BTreeMap<u32, RetainedFrame>,
    input_finished: bool,
}

impl OutboundBody {
    /// Create one independently compressed bounded body sender.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or greater-than-64 frame window, an invalid pending
    /// byte bound, or failure to initialize compression.
    pub fn new(
        transaction_id: [u8; 16],
        direction: StreamDirection,
        compression: CompressionMode,
        zstd_level: i32,
        frame_limit: usize,
        reserved_metadata: usize,
        window_frames: usize,
        max_pending_bytes: usize,
    ) -> Result<Self, EngineError> {
        if window_frames == 0 || window_frames > 64 {
            return Err(EngineError::InvalidWindow(window_frames));
        }
        let batcher = FrameBatcher::new(frame_limit, reserved_metadata);
        if max_pending_bytes < batcher.target_payload() {
            return Err(EngineError::InvalidPendingBound(max_pending_bytes));
        }
        Ok(Self {
            transaction_id,
            direction,
            encoder: Some(StreamEncoder::new(direction, compression, zstd_level)?),
            batcher,
            pending: VecDeque::new(),
            pending_bytes: 0,
            max_pending_bytes,
            local_window: window_frames,
            peer_window: window_frames,
            next_sequence: 0,
            in_flight: BTreeMap::new(),
            input_finished: false,
        })
    }

    /// Maximum logical input chunk accepted by one call without bypassing backpressure.
    #[must_use]
    pub const fn max_input_chunk(&self) -> usize {
        self.batcher.target_payload()
    }

    /// Whether the caller should stop reading its source until ACKs release capacity.
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        self.pending_bytes >= self.max_pending_bytes
            || self.in_flight.len() >= self.effective_window()
    }

    /// Add logical body bytes and produce newly retained frames as capacity allows.
    ///
    /// `flush` is intended for adaptive latency deadlines and explicit immediate mode.
    ///
    /// # Errors
    ///
    /// Returns an error after input completion, for oversized input chunks, compression
    /// failure, pending-buffer overflow, frame encoding failure, or sequence exhaustion.
    pub fn push(&mut self, logical: &[u8], flush: bool) -> Result<(), EngineError> {
        if self.input_finished {
            return Err(EngineError::AlreadyFinished);
        }
        if logical.len() > self.max_input_chunk() {
            return Err(EngineError::InputChunkTooLarge {
                actual: logical.len(),
                limit: self.max_input_chunk(),
            });
        }
        let encoded = self
            .encoder
            .as_mut()
            .ok_or(EngineError::AlreadyFinished)?
            .push(logical, flush)?;
        self.queue_compressed(&encoded)?;
        if flush {
            if let Some(payload) = self.batcher.flush() {
                self.queue(PendingItem::Data(payload))?;
            }
        }
        self.pump()?;
        Ok(())
    }

    /// Complete compression and queue the final stream-integrity frame.
    ///
    /// # Errors
    ///
    /// Returns an error when compression finalization, buffering, encoding, or sequence
    /// allocation fails.
    pub fn finish_input(&mut self) -> Result<(), EngineError> {
        if self.input_finished {
            return Ok(());
        }
        let encoder = self.encoder.take().ok_or(EngineError::AlreadyFinished)?;
        let (trailing, end) = encoder.finish()?;
        self.queue_compressed(&trailing)?;
        if let Some(payload) = self.batcher.flush() {
            self.queue(PendingItem::Data(payload))?;
        }
        self.queue(PendingItem::End(end))?;
        self.input_finished = true;
        self.pump()?;
        Ok(())
    }

    /// Update the peer-advertised frame capacity and enqueue pending frames if possible.
    ///
    /// # Errors
    ///
    /// Returns an error for a peer window greater than 64 or frame/sequence failure.
    pub fn set_peer_window(&mut self, receive_window: u32) -> Result<(), EngineError> {
        let receive_window = usize::try_from(receive_window)
            .map_err(|_| EngineError::InvalidWindow(usize::MAX))?;
        if receive_window > 64 {
            return Err(EngineError::InvalidWindow(receive_window));
        }
        self.peer_window = receive_window;
        self.pump()
    }

    /// Apply a cumulative/selective ACK and enqueue additional pending frames.
    ///
    /// # Errors
    ///
    /// Returns an error for a direction mismatch or frame/sequence failure while pumping.
    pub fn acknowledge(&mut self, ack: Ack) -> Result<Vec<u32>, EngineError> {
        if ack.direction != self.direction {
            return Err(EngineError::DirectionMismatch);
        }
        self.set_peer_window(ack.receive_window)?;
        let snapshot = ack.snapshot();
        let acknowledged = self
            .in_flight
            .keys()
            .copied()
            .filter(|sequence| ReceiveWindow::acknowledges(snapshot, *sequence))
            .collect::<Vec<_>>();
        for sequence in &acknowledged {
            self.in_flight.remove(sequence);
        }
        self.pump()?;
        Ok(acknowledged)
    }

    /// Return never-sent or expired retained frames and mark this dispatch attempt.
    #[must_use]
    pub fn take_sendable(
        &mut self,
        now_ms: u64,
        retry_policy: RetryPolicy,
    ) -> Vec<RetainedFrame> {
        let sequences = self
            .in_flight
            .iter()
            .filter_map(|(sequence, frame)| {
                let ready = frame.last_sent_ms.is_none_or(|last| {
                    let retries = frame.attempts.saturating_sub(1);
                    now_ms.saturating_sub(last) >= retry_policy.delay_ms(retries)
                });
                ready.then_some(*sequence)
            })
            .collect::<Vec<_>>();
        let mut output = Vec::with_capacity(sequences.len());
        for sequence in sequences {
            if let Some(frame) = self.in_flight.get_mut(&sequence) {
                frame.last_sent_ms = Some(now_ms);
                frame.attempts = frame.attempts.saturating_add(1);
                output.push(frame.clone());
            }
        }
        output
    }

    /// Whether the end frame has been acknowledged and no bytes remain buffered.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.input_finished && self.pending.is_empty() && self.in_flight.is_empty()
    }

    /// Encoded/compressed bytes waiting outside the retransmission window.
    #[must_use]
    pub const fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Frames retained for retransmission.
    #[must_use]
    pub fn in_flight_frames(&self) -> usize {
        self.in_flight.len()
    }

    fn effective_window(&self) -> usize {
        self.local_window.min(self.peer_window)
    }

    fn queue_compressed(&mut self, compressed: &[u8]) -> Result<(), EngineError> {
        for payload in self.batcher.push(compressed) {
            self.queue(PendingItem::Data(payload))?;
        }
        Ok(())
    }

    fn queue(&mut self, item: PendingItem) -> Result<(), EngineError> {
        let retained = item.retained_bytes();
        let actual = self.pending_bytes.saturating_add(retained);
        if actual > self.max_pending_bytes {
            return Err(EngineError::PendingLimit {
                actual,
                limit: self.max_pending_bytes,
            });
        }
        self.pending_bytes = actual;
        self.pending.push_back(item);
        Ok(())
    }

    fn pump(&mut self) -> Result<(), EngineError> {
        while self.in_flight.len() < self.effective_window() {
            let Some(item) = self.pending.pop_front() else { break };
            self.pending_bytes = self.pending_bytes.saturating_sub(item.retained_bytes());
            let sequence = self.next_sequence;
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or(EngineError::SequenceExhausted)?;
            let encoded = match item {
                PendingItem::Data(payload) => {
                    encode_data(self.transaction_id, self.direction, sequence, payload)?
                }
                PendingItem::End(end) => encode_end(self.transaction_id, sequence, &end)?,
            };
            self.in_flight.insert(
                sequence,
                RetainedFrame {
                    sequence,
                    encoded,
                    last_sent_ms: None,
                    attempts: 0,
                },
            );
        }
        Ok(())
    }
}

/// Logical bytes and ACK state produced by one receive operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiveOutput {
    /// Newly contiguous decompressed chunks in delivery order.
    pub logical_chunks: Vec<Bytes>,
    /// Current cumulative/selective acknowledgement.
    pub ack: Ack,
    /// True after the final end frame was verified.
    pub completed: bool,
}

/// Bounded incoming logical body stream.
#[derive(Debug)]
pub struct InboundBody {
    transaction_id: [u8; 16],
    direction: StreamDirection,
    decoder: Option<StreamDecoder>,
    receive: ReceiveWindow,
    reassembler: Reassembler,
    capacity: u32,
    pending_end: Option<(u32, StreamEnd)>,
    completed: bool,
}

impl InboundBody {
    /// Create a bounded body receiver.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or greater-than-64 frame capacity or failure to
    /// initialize decompression.
    pub fn new(
        transaction_id: [u8; 16],
        direction: StreamDirection,
        compression: CompressionMode,
        capacity: u32,
        max_out_of_order_bytes: usize,
        max_logical_length: u64,
    ) -> Result<Self, EngineError> {
        if capacity == 0 || capacity > 64 {
            return Err(EngineError::InvalidWindow(usize::try_from(capacity).unwrap_or(usize::MAX)));
        }
        Ok(Self {
            transaction_id,
            direction,
            decoder: Some(StreamDecoder::new(direction, compression, max_logical_length)?),
            receive: ReceiveWindow::default(),
            reassembler: Reassembler::new(max_out_of_order_bytes),
            capacity,
            pending_end: None,
            completed: false,
        })
    }

    /// Receive one encoded VHTTP data or end frame.
    ///
    /// # Errors
    ///
    /// Returns an error for transaction/direction mismatch, malformed frames, bounded
    /// reassembly/decompression failure, duplicate conflicting end frames, or integrity
    /// mismatch.
    pub fn receive(&mut self, encoded: Bytes) -> Result<ReceiveOutput, EngineError> {
        let mut logical_chunks = Vec::new();
        match decode(encoded)? {
            DecodedFrame::Data { transaction_id, direction, sequence, payload } => {
                self.validate(transaction_id, direction)?;
                if self.completed || !self.receive.record(sequence) {
                    return Ok(self.output(logical_chunks));
                }
                for compressed in self.reassembler.push(sequence, payload)? {
                    let logical = self
                        .decoder
                        .as_mut()
                        .ok_or(EngineError::AlreadyFinished)?
                        .push(&compressed, false)?;
                    if !logical.is_empty() {
                        logical_chunks.push(logical);
                    }
                }
            }
            DecodedFrame::End { transaction_id, sequence, value } => {
                self.validate(transaction_id, value.direction)?;
                if self.completed {
                    return Ok(self.output(logical_chunks));
                }
                if let Some((existing_sequence, existing)) = &self.pending_end {
                    if *existing_sequence != sequence || existing != &value {
                        return Err(EngineError::ConflictingEnd);
                    }
                } else {
                    self.pending_end = Some((sequence, value));
                }
            }
            _ => return Err(EngineError::UnexpectedFrame),
        }
        self.try_finish(&mut logical_chunks)?;
        Ok(self.output(logical_chunks))
    }

    /// Current ACK without consuming another frame.
    #[must_use]
    pub fn ack(&self) -> Ack {
        let snapshot = self.receive.snapshot();
        let buffered = snapshot.selective.count_ones();
        Ack::from_snapshot(
            self.direction,
            snapshot,
            self.capacity.saturating_sub(buffered),
        )
    }

    /// Whether final decompressed length and digest were verified.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.completed
    }

    fn validate(
        &self,
        transaction_id: [u8; 16],
        direction: StreamDirection,
    ) -> Result<(), EngineError> {
        if transaction_id != self.transaction_id {
            return Err(EngineError::TransactionMismatch);
        }
        if direction != self.direction {
            return Err(EngineError::DirectionMismatch);
        }
        Ok(())
    }

    fn try_finish(&mut self, output: &mut Vec<Bytes>) -> Result<(), EngineError> {
        let Some((end_sequence, end)) = self.pending_end.clone() else { return Ok(()) };
        let contiguous = self.receive.snapshot().cumulative;
        let data_complete = match contiguous {
            Some(sequence) => sequence.checked_add(1) == Some(end_sequence),
            None => end_sequence == 0,
        };
        if !data_complete {
            return Ok(());
        }
        let decoder = self.decoder.take().ok_or(EngineError::AlreadyFinished)?;
        let trailing = decoder.finish(&end)?;
        if !trailing.is_empty() {
            output.push(trailing);
        }
        self.receive.record(end_sequence);
        self.completed = true;
        Ok(())
    }

    fn output(&self, logical_chunks: Vec<Bytes>) -> ReceiveOutput {
        ReceiveOutput {
            logical_chunks,
            ack: self.ack(),
            completed: self.completed,
        }
    }
}

/// Transaction-engine failures.
#[derive(Debug, Error)]
pub enum EngineError {
    /// Stream protocol framing/compression failed.
    #[error(transparent)]
    Stream(#[from] veilid_http_stream::StreamProtocolError),
    /// Bounded reassembly failed.
    #[error(transparent)]
    Core(#[from] veilid_http_core::CoreError),
    /// Frame window must be in the range 1 through 64.
    #[error("VHTTP frame window {0} is outside 1..=64")]
    InvalidWindow(usize),
    /// Pending encoded-byte bound must hold at least one frame payload.
    #[error("VHTTP pending-byte bound {0} is too small")]
    InvalidPendingBound(usize),
    /// One logical input chunk exceeded the advertised bounded-ingress hint.
    #[error("logical input chunk {actual} exceeds limit {limit}")]
    InputChunkTooLarge { actual: usize, limit: usize },
    /// Pending compressed bytes exceeded the configured hard bound.
    #[error("pending encoded bytes {actual} exceed limit {limit}")]
    PendingLimit { actual: usize, limit: usize },
    /// Sequence space was exhausted.
    #[error("VHTTP stream sequence space exhausted")]
    SequenceExhausted,
    /// Input or decoder was already finalized.
    #[error("VHTTP stream is already finished")]
    AlreadyFinished,
    /// Transaction IDs do not match.
    #[error("VHTTP stream transaction identifier mismatch")]
    TransactionMismatch,
    /// Request/response stream directions do not match.
    #[error("VHTTP stream direction mismatch")]
    DirectionMismatch,
    /// Frame is not a data/end frame for this body receiver.
    #[error("unexpected VHTTP frame for body receiver")]
    UnexpectedFrame,
    /// More than one different end frame arrived for the same transaction direction.
    #[error("conflicting VHTTP stream end frames")]
    ConflictingEnd,
}

#[cfg(test)]
mod tests {
    use super::*;
    use veilid_http_core::RetryPolicy;
    use veilid_http_stream::DecodedFrame;

    #[test]
    fn end_waits_for_missing_data_and_duplicate_frames_deliver_once() {
        let transaction = [4; 16];
        let mut sender = OutboundBody::new(
            transaction,
            StreamDirection::Response,
            CompressionMode::None,
            0,
            1024,
            128,
            8,
            64 * 1024,
        )
        .unwrap();
        sender.push(b"alpha", true).unwrap();
        sender.push(b"beta", true).unwrap();
        sender.finish_input().unwrap();
        let mut frames = sender
            .take_sendable(0, RetryPolicy { initial_ms: 10, maximum_ms: 100 })
            .into_iter()
            .map(|frame| frame.encoded)
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 3);

        let end = frames.pop().unwrap();
        let first = frames.remove(0);
        let second = frames.remove(0);
        let mut receiver = InboundBody::new(
            transaction,
            StreamDirection::Response,
            CompressionMode::None,
            8,
            16 * 1024,
            1024,
        )
        .unwrap();
        assert!(!receiver.receive(end).unwrap().completed);
        let first_output = receiver.receive(first.clone()).unwrap();
        assert_eq!(first_output.logical_chunks, vec![Bytes::from_static(b"alpha")]);
        assert!(receiver.receive(first).unwrap().logical_chunks.is_empty());
        let second_output = receiver.receive(second).unwrap();
        assert_eq!(second_output.logical_chunks, vec![Bytes::from_static(b"beta")]);
        assert!(second_output.completed);
    }

    #[test]
    fn dropped_frame_is_selectively_retried_and_large_stream_stays_bounded() {
        let transaction = [9; 16];
        let input = (0..120_000_u32)
            .flat_map(|value| value.wrapping_mul(2_654_435_761).to_le_bytes())
            .collect::<Vec<_>>();
        let mut sender = OutboundBody::new(
            transaction,
            StreamDirection::Request,
            CompressionMode::Zstd,
            3,
            2048,
            256,
            8,
            1024 * 1024,
        )
        .unwrap();
        for chunk in input.chunks(sender.max_input_chunk()) {
            sender.push(chunk, false).unwrap();
        }
        sender.finish_input().unwrap();
        let mut receiver = InboundBody::new(
            transaction,
            StreamDirection::Request,
            CompressionMode::Zstd,
            8,
            64 * 1024,
            u64::try_from(input.len()).unwrap(),
        )
        .unwrap();
        let policy = RetryPolicy { initial_ms: 10, maximum_ms: 100 };
        let mut now = 0;
        let mut dropped_one = false;
        let mut output = Vec::new();

        for _ in 0..20_000 {
            let mut sent = sender.take_sendable(now, policy);
            sent.reverse();
            for retained in sent {
                let sequence = match decode(retained.encoded.clone()).unwrap() {
                    DecodedFrame::Data { sequence, .. } => sequence,
                    DecodedFrame::End { sequence, .. } => sequence,
                    _ => unreachable!(),
                };
                if sequence == 1 && !dropped_one {
                    dropped_one = true;
                    continue;
                }
                let received = receiver.receive(retained.encoded).unwrap();
                for chunk in received.logical_chunks {
                    output.extend_from_slice(&chunk);
                }
                sender.acknowledge(received.ack).unwrap();
            }
            assert!(sender.in_flight_frames() <= 8);
            if sender.is_complete() && receiver.is_complete() {
                break;
            }
            now += 25;
        }

        assert!(dropped_one);
        assert!(sender.is_complete());
        assert!(receiver.is_complete());
        assert_eq!(output, input);
    }

    #[test]
    fn peer_zero_window_stops_new_frames_but_keeps_retries_retained() {
        let mut sender = OutboundBody::new(
            [1; 16],
            StreamDirection::Response,
            CompressionMode::None,
            0,
            1024,
            128,
            4,
            16 * 1024,
        )
        .unwrap();
        sender.set_peer_window(0).unwrap();
        sender.push(b"hello", true).unwrap();
        sender.finish_input().unwrap();
        assert_eq!(sender.in_flight_frames(), 0);
        assert!(sender.pending_bytes() > 0);
        sender.set_peer_window(4).unwrap();
        assert!(sender.in_flight_frames() > 0);
    }
}
