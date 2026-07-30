//! Environment-neutral Veilid AppCall/AppMessage contracts.

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/// Opaque local target created by importing a RouteBlob.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteTarget(pub String);

/// Inbound application event delivered by a Veilid adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportEvent {
    /// One-way AppMessage payload.
    AppMessage { route: Option<RouteTarget>, payload: Bytes },
    /// AppCall expecting a reply through the adapter-specific call identifier.
    AppCall { call_id: String, route: Option<RouteTarget>, payload: Bytes },
    /// A route died or was released.
    RouteChanged { route: RouteTarget, dead: bool },
    /// The underlying Veilid node shut down.
    Shutdown,
}

/// Errors normalized across native and remote Veilid adapters.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Route or network is temporarily unavailable and may be retried.
    #[error("transport temporarily unavailable: {0}")]
    Retryable(String),
    /// AppCall reply deadline elapsed.
    #[error("transport call timed out")]
    Timeout,
    /// Target is malformed or unavailable.
    #[error("invalid route target: {0}")]
    InvalidTarget(String),
    /// Adapter was shut down.
    #[error("transport adapter is shut down")]
    Shutdown,
    /// Non-retryable adapter failure.
    #[error("transport failure: {0}")]
    Fatal(String),
}

/// Minimum transport surface required by VHTTP.
#[async_trait]
pub trait VeilidTransport: Send + Sync {
    /// Import a publishable private RouteBlob and return a local target handle.
    async fn import_route(&self, route_blob: Bytes) -> Result<RouteTarget, TransportError>;
    /// Allocate a reliable private route and return local target plus publishable blob.
    async fn allocate_route(&self) -> Result<(RouteTarget, Bytes), TransportError>;
    /// Release a locally allocated or remotely imported private route.
    async fn release_route(&self, target: &RouteTarget) -> Result<(), TransportError>;
    /// Send an AppCall and await its application reply.
    async fn app_call(&self, target: &RouteTarget, payload: Bytes) -> Result<Bytes, TransportError>;
    /// Dispatch a one-way AppMessage.
    async fn app_message(&self, target: &RouteTarget, payload: Bytes) -> Result<(), TransportError>;
    /// Reply exactly once to an inbound AppCall.
    async fn app_call_reply(&self, call_id: &str, payload: Bytes) -> Result<(), TransportError>;
    /// Receive the next adapter event.
    async fn next_event(&self) -> Result<TransportEvent, TransportError>;
}
