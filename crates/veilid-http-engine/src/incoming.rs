use crate::EngineError;
use bytes::Bytes;
use veilid_http_core::{Reassembler, ReceiveWindow};
use veilid_http_stream::{
    Ack, CompressionMode, DecodedFrame, StreamDecoder, StreamDirection, StreamEnd, decode,
};

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
    /// Returns an error for an invalid frame capacity or decompressor setup failure.
    pub fn new(
        transaction_id: [u8; 16],
        direction: StreamDirection,
        compression: CompressionMode,
        capacity: u32,
        max_out_of_order_bytes: usize,
        max_logical_length: u64,
    ) -> Result<Self, EngineError> {
        if capacity == 0 || capacity > 64 {
            return Err(EngineError::InvalidWindow(
                usize::try_from(capacity).unwrap_or(usize::MAX),
            ));
        }
        Ok(Self {
            transaction_id,
            direction,
            decoder: Some(StreamDecoder::new(
                direction,
                compression,
                max_logical_length,
            )?),
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
    /// Returns an error for identity/direction mismatch, malformed frames, bounded
    /// reassembly/decompression failure, conflicting end frames, or integrity mismatch.
    pub fn receive(&mut self, encoded: Bytes) -> Result<ReceiveOutput, EngineError> {
        let mut logical_chunks = Vec::new();
        match decode(encoded)? {
            DecodedFrame::Data {
                transaction_id,
                direction,
                sequence,
                payload,
            } => {
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
            DecodedFrame::End {
                transaction_id,
                sequence,
                value,
            } => {
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
        let Some((end_sequence, end)) = self.pending_end.clone() else {
            return Ok(());
        };
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
