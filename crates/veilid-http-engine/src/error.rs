use thiserror::Error;

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
    /// One logical input chunk exceeded the bounded-ingress hint.
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
