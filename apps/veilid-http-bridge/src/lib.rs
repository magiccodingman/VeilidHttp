//! Reusable bridge runtime components used by the Docker binary and integration tests.

/// Persistent at-most-once completion coordination.
pub mod completion;
/// Stream-capable VHTTP-to-HTTP bridge runtime.
pub mod streaming;
