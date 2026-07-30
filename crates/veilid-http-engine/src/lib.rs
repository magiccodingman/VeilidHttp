//! Host-neutral bounded send/receive state for VHTTP request and response bodies.

mod error;
mod incoming;
mod outgoing;

pub use error::EngineError;
pub use incoming::{InboundBody, ReceiveOutput};
pub use outgoing::{OutboundBody, RetainedFrame};
