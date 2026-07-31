use crate::EngineError;
use bytes::Bytes;
use std::collections::{BTreeMap, VecDeque};
use veilid_http_core::{FrameBatcher, ReceiveWindow, RetryPolicy};
use veilid_http_stream::{
    Ack, CompressionMode, StreamDirection, StreamEncoder, StreamEnd, encode_data, encode_end,
};
use veilid_http_wire::DEFAULT_FRAME_LIMIT;

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

#[derive(Debug, Clone)]
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
    in_flight_bytes: usize,
    max_buffered_bytes: usize,
    local_window: usize,
    peer_window: usize,
    next_sequence: u32,
    in_flight: BTreeMap<u32, RetainedFrame>,
    input_finished: bool,
}

impl OutboundBody {
    /// Create one independently compressed bounded body sender.
    ///
    /// The byte bound covers both compressed frames waiting for the send window and
    /// complete encoded frames retained for retransmission.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid frame window/buffer bound or compression setup.
    #[allow(clippy::too_many_arguments)]
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
        let conservative_frame_bytes = frame_limit.min(DEFAULT_FRAME_LIMIT);
        let minimum_window_bytes = conservative_frame_bytes
            .checked_mul(window_frames)
            .ok_or(EngineError::InvalidPendingBound(max_pending_bytes))?;
        if max_pending_bytes < minimum_window_bytes || max_pending_bytes < batcher.target_payload()
        {
            return Err(EngineError::InvalidPendingBound(max_pending_bytes));
        }
        Ok(Self {
            transaction_id,
            direction,
            encoder: Some(StreamEncoder::new(direction, compression, zstd_level)?),
            batcher,
            pending: VecDeque::new(),
            pending_bytes: 0,
            in_flight_bytes: 0,
            max_buffered_bytes: max_pending_bytes,
            local_window: window_frames,
            peer_window: window_frames,
            next_sequence: 0,
            in_flight: BTreeMap::new(),
            input_finished: false,
        })
    }

    /// Maximum logical input chunk accepted by one call.
    #[must_use]
    pub const fn max_input_chunk(&self) -> usize {
        self.batcher.target_payload()
    }

    /// Whether the source should stop reading until ACKs release capacity.
    #[must_use]
    pub fn is_backpressured(&self) -> bool {
        self.buffered_bytes() >= self.max_buffered_bytes
            || self.in_flight.len() >= self.effective_window()
    }

    /// Add logical body bytes and retain newly framed bytes as capacity allows.
    ///
    /// # Errors
    ///
    /// Returns an error after completion, for an oversized call, buffer overflow,
    /// compression/framing failure, or sequence exhaustion.
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
        if flush && let Some(payload) = self.batcher.flush() {
            self.queue(PendingItem::Data(payload))?;
        }
        self.pump()
    }

    /// Complete compression and queue the final stream-integrity frame.
    ///
    /// # Errors
    ///
    /// Returns an error when finalization, buffering, framing, or sequencing fails.
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
        self.pump()
    }

    /// Update peer-advertised capacity and enqueue waiting frames.
    ///
    /// # Errors
    ///
    /// Returns an error for a peer window greater than 64 or framing/sequence failure.
    pub fn set_peer_window(&mut self, receive_window: u32) -> Result<(), EngineError> {
        let receive_window =
            usize::try_from(receive_window).map_err(|_| EngineError::InvalidWindow(usize::MAX))?;
        if receive_window > 64 {
            return Err(EngineError::InvalidWindow(receive_window));
        }
        self.peer_window = receive_window;
        self.pump()
    }

    /// Apply a cumulative/selective ACK and then fill newly released window capacity.
    ///
    /// # Errors
    ///
    /// Returns an error for a direction mismatch or framing/sequence failure.
    pub fn acknowledge(&mut self, ack: Ack) -> Result<Vec<u32>, EngineError> {
        if ack.direction != self.direction {
            return Err(EngineError::DirectionMismatch);
        }
        let peer_window = usize::try_from(ack.receive_window)
            .map_err(|_| EngineError::InvalidWindow(usize::MAX))?;
        if peer_window > 64 {
            return Err(EngineError::InvalidWindow(peer_window));
        }
        self.peer_window = peer_window;
        let snapshot = ack.snapshot();
        let acknowledged = self
            .in_flight
            .keys()
            .copied()
            .filter(|sequence| ReceiveWindow::acknowledges(snapshot, *sequence))
            .collect::<Vec<_>>();
        for sequence in &acknowledged {
            if let Some(frame) = self.in_flight.remove(sequence) {
                self.in_flight_bytes = self.in_flight_bytes.saturating_sub(frame.encoded.len());
            }
        }
        self.pump()?;
        Ok(acknowledged)
    }

    /// Return never-sent or expired retained frames and mark this dispatch attempt.
    #[must_use]
    pub fn take_sendable(&mut self, now_ms: u64, retry_policy: RetryPolicy) -> Vec<RetainedFrame> {
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

    /// Whether end was acknowledged and no bytes remain buffered.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.input_finished && self.pending.is_empty() && self.in_flight.is_empty()
    }

    /// Compressed payload bytes waiting outside the retransmission window.
    #[must_use]
    pub const fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    /// Complete encoded frame bytes retained for retransmission.
    #[must_use]
    pub const fn in_flight_bytes(&self) -> usize {
        self.in_flight_bytes
    }

    /// Total pending plus retransmission bytes owned by this stream.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.pending_bytes.saturating_add(self.in_flight_bytes)
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
        let actual = self.buffered_bytes().saturating_add(retained);
        if actual > self.max_buffered_bytes {
            return Err(EngineError::PendingLimit {
                actual,
                limit: self.max_buffered_bytes,
            });
        }
        self.pending_bytes = self.pending_bytes.saturating_add(retained);
        self.pending.push_back(item);
        Ok(())
    }

    fn pump(&mut self) -> Result<(), EngineError> {
        while self.in_flight.len() < self.effective_window() {
            let Some(item) = self.pending.front().cloned() else {
                break;
            };
            let sequence = self.next_sequence;
            let encoded = match &item {
                PendingItem::Data(payload) => encode_data(
                    self.transaction_id,
                    self.direction,
                    sequence,
                    payload.clone(),
                )?,
                PendingItem::End(end) => encode_end(self.transaction_id, sequence, end)?,
            };
            let projected = self
                .buffered_bytes()
                .saturating_sub(item.retained_bytes())
                .saturating_add(encoded.len());
            if projected > self.max_buffered_bytes {
                return Err(EngineError::PendingLimit {
                    actual: projected,
                    limit: self.max_buffered_bytes,
                });
            }

            self.pending.pop_front();
            self.pending_bytes = self.pending_bytes.saturating_sub(item.retained_bytes());
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or(EngineError::SequenceExhausted)?;
            self.in_flight_bytes = self.in_flight_bytes.saturating_add(encoded.len());
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
