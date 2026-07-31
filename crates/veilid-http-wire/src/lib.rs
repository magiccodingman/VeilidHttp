//! VHTTP/1 deterministic frame and metadata encoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// Veilid's hard application payload ceiling.
pub const VEILID_MESSAGE_LIMIT: usize = 32_768;
/// Conservative complete-frame target used by VHTTP/1.
pub const DEFAULT_FRAME_LIMIT: usize = 30 * 1024;
/// Fixed VHTTP/1 header size.
pub const HEADER_LEN: usize = 40;
/// Maximum MessagePack metadata accepted by the decoder.
pub const MAX_METADATA_LEN: usize = 8 * 1024;
const MAGIC: [u8; 4] = *b"VHTP";
const VERSION: u8 = 1;
const BUNDLE_MAGIC: [u8; 4] = *b"VHB1";
const BUNDLE_HEADER_LEN: usize = 8;

/// VHTTP/1 frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FrameType {
    /// Capability negotiation.
    Negotiate = 1,
    /// Request metadata and optional first body bytes.
    RequestOpen = 2,
    /// Request session accepted.
    RequestAccepted = 3,
    /// Request body data.
    RequestData = 4,
    /// Request body completed.
    RequestEnd = 5,
    /// Response status and headers.
    ResponseOpen = 6,
    /// Response body data.
    ResponseData = 7,
    /// Response body completed.
    ResponseEnd = 8,
    /// Complete small response returned through AppCall.
    AtomicResponse = 9,
    /// Cumulative and selective acknowledgement.
    Ack = 10,
    /// Transaction cancellation.
    Cancel = 11,
    /// Protocol or upstream error.
    Error = 12,
    /// Transfer status query or reply.
    Status = 13,
}

impl TryFrom<u8> for FrameType {
    type Error = WireError;

    fn try_from(value: u8) -> Result<Self, WireError> {
        match value {
            1 => Ok(Self::Negotiate),
            2 => Ok(Self::RequestOpen),
            3 => Ok(Self::RequestAccepted),
            4 => Ok(Self::RequestData),
            5 => Ok(Self::RequestEnd),
            6 => Ok(Self::ResponseOpen),
            7 => Ok(Self::ResponseData),
            8 => Ok(Self::ResponseEnd),
            9 => Ok(Self::AtomicResponse),
            10 => Ok(Self::Ack),
            11 => Ok(Self::Cancel),
            12 => Ok(Self::Error),
            13 => Ok(Self::Status),
            other => Err(WireError::UnknownFrameType(other)),
        }
    }
}

/// Extensible protocol metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// HTTP-specific fields.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub http: BTreeMap<String, String>,
    /// VHTTP transport fields.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub vhttp: BTreeMap<String, String>,
    /// Namespaced optional extensions encoded as MessagePack bytes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Vec<u8>>,
}

/// One VHTTP/1 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Semantic frame type.
    pub frame_type: FrameType,
    /// Protocol flags.
    pub flags: u16,
    /// Random 128-bit transaction identifier.
    pub transaction_id: [u8; 16],
    /// Per-direction sequence number.
    pub sequence: u32,
    /// Highest contiguous sequence acknowledged by the sender.
    pub cumulative_ack: u32,
    /// Extensible structured metadata.
    pub metadata: Metadata,
    /// Raw payload bytes.
    pub payload: Bytes,
}

/// A transport-level bundle of independently encoded VHTTP frames.
///
/// Bundling allows either side to coalesce several tiny frames into one Veilid
/// AppMessage without sharing compression state or coupling transaction recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameBundle {
    /// Complete encoded VHTTP frames.
    pub frames: Vec<Bytes>,
}

impl FrameBundle {
    /// Encode a bundle while enforcing Veilid's complete application-message limit.
    pub fn encode(&self) -> Result<Bytes, WireError> {
        if self.frames.is_empty() {
            return Err(WireError::EmptyBundle);
        }
        let total = BUNDLE_HEADER_LEN
            + self.frames.len() * 4
            + self.frames.iter().map(Bytes::len).sum::<usize>();
        if total > VEILID_MESSAGE_LIMIT {
            return Err(WireError::FrameTooLarge {
                actual: total,
                limit: VEILID_MESSAGE_LIMIT,
            });
        }
        let count = u16::try_from(self.frames.len()).map_err(|_| WireError::BundleCount)?;
        let mut output = BytesMut::with_capacity(total);
        output.put_slice(&BUNDLE_MAGIC);
        output.put_u16(count);
        output.put_u16(0);
        for frame in &self.frames {
            let length = u32::try_from(frame.len()).map_err(|_| WireError::BundleLength)?;
            output.put_u32(length);
        }
        for frame in &self.frames {
            output.put_slice(frame);
        }
        Ok(output.freeze())
    }

    /// Decode a bundle and validate every contained VHTTP frame.
    pub fn decode(input: Bytes) -> Result<Self, WireError> {
        if input.len() < BUNDLE_HEADER_LEN {
            return Err(WireError::TruncatedBundle);
        }
        if input.len() > VEILID_MESSAGE_LIMIT {
            return Err(WireError::FrameTooLarge {
                actual: input.len(),
                limit: VEILID_MESSAGE_LIMIT,
            });
        }
        let mut cursor = input.clone();
        let mut magic = [0_u8; 4];
        cursor.copy_to_slice(&mut magic);
        if magic != BUNDLE_MAGIC {
            return Err(WireError::InvalidBundleMagic);
        }
        let count = cursor.get_u16() as usize;
        let reserved = cursor.get_u16();
        if count == 0 {
            return Err(WireError::EmptyBundle);
        }
        if reserved != 0 {
            return Err(WireError::ReservedField);
        }
        let lengths_bytes = count.checked_mul(4).ok_or(WireError::BundleLength)?;
        if cursor.remaining() < lengths_bytes {
            return Err(WireError::TruncatedBundle);
        }
        let mut lengths = Vec::with_capacity(count);
        for _ in 0..count {
            lengths.push(cursor.get_u32() as usize);
        }
        let expected = lengths
            .iter()
            .try_fold(0_usize, |sum, length| sum.checked_add(*length))
            .ok_or(WireError::BundleLength)?;
        if cursor.remaining() != expected {
            return Err(WireError::LengthMismatch);
        }
        let mut frames = Vec::with_capacity(count);
        for length in lengths {
            let frame = cursor.copy_to_bytes(length);
            Frame::decode(frame.clone())?;
            frames.push(frame);
        }
        Ok(Self { frames })
    }
}

/// Wire-format failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WireError {
    /// Input is shorter than the fixed header.
    #[error("frame is shorter than the {HEADER_LEN}-byte header")]
    TruncatedHeader,
    /// Bundle input is shorter than its fixed header or length table.
    #[error("truncated VHTTP frame bundle")]
    TruncatedBundle,
    /// Bundle magic is invalid.
    #[error("invalid VHTTP frame bundle magic")]
    InvalidBundleMagic,
    /// A bundle must contain at least one frame.
    #[error("VHTTP frame bundle cannot be empty")]
    EmptyBundle,
    /// A bundle contained too many frames for its count field.
    #[error("VHTTP frame bundle contains too many frames")]
    BundleCount,
    /// A bundle length could not be represented or safely summed.
    #[error("invalid VHTTP frame bundle length")]
    BundleLength,
    /// Magic bytes do not identify VHTTP.
    #[error("invalid VHTTP frame magic")]
    InvalidMagic,
    /// Unsupported protocol version.
    #[error("unsupported VHTTP version {0}")]
    UnsupportedVersion(u8),
    /// Unknown frame type.
    #[error("unknown VHTTP frame type {0}")]
    UnknownFrameType(u8),
    /// Reserved bits are not zero.
    #[error("reserved header field must be zero")]
    ReservedField,
    /// Metadata exceeds its bound.
    #[error("metadata length {0} exceeds maximum {MAX_METADATA_LEN}")]
    MetadataTooLarge(usize),
    /// Complete frame exceeds the configured bound.
    #[error("frame length {actual} exceeds limit {limit}")]
    FrameTooLarge {
        /// Actual encoded frame length.
        actual: usize,
        /// Configured or transport frame limit.
        limit: usize,
    },
    /// Declared and actual frame lengths differ.
    #[error("declared frame length does not match available bytes")]
    LengthMismatch,
    /// MessagePack metadata could not be encoded.
    #[error("metadata encoding failed: {0}")]
    MetadataEncode(String),
    /// MessagePack metadata could not be decoded.
    #[error("metadata decoding failed: {0}")]
    MetadataDecode(String),
}

impl Frame {
    /// Encode using the conservative VHTTP frame limit.
    pub fn encode(&self) -> Result<Bytes, WireError> {
        self.encode_with_limit(DEFAULT_FRAME_LIMIT)
    }

    /// Encode with a caller-supplied complete-frame limit.
    pub fn encode_with_limit(&self, limit: usize) -> Result<Bytes, WireError> {
        let metadata = rmp_serde::to_vec_named(&self.metadata)
            .map_err(|error| WireError::MetadataEncode(error.to_string()))?;
        if metadata.len() > MAX_METADATA_LEN {
            return Err(WireError::MetadataTooLarge(metadata.len()));
        }
        let total = HEADER_LEN + metadata.len() + self.payload.len();
        if total > limit.min(VEILID_MESSAGE_LIMIT) {
            return Err(WireError::FrameTooLarge {
                actual: total,
                limit,
            });
        }

        let metadata_len = u16::try_from(metadata.len()).map_err(|_| WireError::MetadataTooLarge(metadata.len()))?;
        let payload_len = u32::try_from(self.payload.len()).map_err(|_| WireError::FrameTooLarge {
            actual: total,
            limit,
        })?;
        let mut output = BytesMut::with_capacity(total);
        output.put_slice(&MAGIC);
        output.put_u8(VERSION);
        output.put_u8(self.frame_type as u8);
        output.put_u16(self.flags);
        output.put_slice(&self.transaction_id);
        output.put_u32(self.sequence);
        output.put_u32(self.cumulative_ack);
        output.put_u16(metadata_len);
        output.put_u16(0);
        output.put_u32(payload_len);
        output.put_slice(&metadata);
        output.put_slice(&self.payload);
        Ok(output.freeze())
    }

    /// Decode and validate a complete VHTTP frame.
    pub fn decode(input: Bytes) -> Result<Self, WireError> {
        if input.len() < HEADER_LEN {
            return Err(WireError::TruncatedHeader);
        }
        if input.len() > VEILID_MESSAGE_LIMIT {
            return Err(WireError::FrameTooLarge {
                actual: input.len(),
                limit: VEILID_MESSAGE_LIMIT,
            });
        }

        let mut cursor = input.clone();
        let mut magic = [0_u8; 4];
        cursor.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(WireError::InvalidMagic);
        }
        let version = cursor.get_u8();
        if version != VERSION {
            return Err(WireError::UnsupportedVersion(version));
        }
        let frame_type = FrameType::try_from(cursor.get_u8())?;
        let flags = cursor.get_u16();
        let mut transaction_id = [0_u8; 16];
        cursor.copy_to_slice(&mut transaction_id);
        let sequence = cursor.get_u32();
        let cumulative_ack = cursor.get_u32();
        let metadata_len = cursor.get_u16() as usize;
        let reserved = cursor.get_u16();
        if reserved != 0 {
            return Err(WireError::ReservedField);
        }
        let payload_len = cursor.get_u32() as usize;
        if metadata_len > MAX_METADATA_LEN {
            return Err(WireError::MetadataTooLarge(metadata_len));
        }
        if HEADER_LEN + metadata_len + payload_len != input.len() {
            return Err(WireError::LengthMismatch);
        }

        let metadata_bytes = cursor.copy_to_bytes(metadata_len);
        let metadata = rmp_serde::from_slice(&metadata_bytes)
            .map_err(|error| WireError::MetadataDecode(error.to_string()))?;
        let payload = cursor.copy_to_bytes(payload_len);

        Ok(Self {
            frame_type,
            flags,
            transaction_id,
            sequence,
            cumulative_ack,
            metadata,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let mut metadata = Metadata::default();
        metadata.http.insert("method".into(), "GET".into());
        metadata.http.insert("path".into(), "/api/items".into());
        let frame = Frame {
            frame_type: FrameType::RequestOpen,
            flags: 0,
            transaction_id: [7; 16],
            sequence: 4,
            cumulative_ack: 2,
            metadata,
            payload: Bytes::from_static(b"hello"),
        };
        let encoded = frame.encode().expect("encode");
        assert_eq!(Frame::decode(encoded).expect("decode"), frame);
    }

    #[test]
    fn rejects_declared_length_mismatch() {
        let frame = Frame {
            frame_type: FrameType::Ack,
            flags: 0,
            transaction_id: [0; 16],
            sequence: 0,
            cumulative_ack: 0,
            metadata: Metadata::default(),
            payload: Bytes::new(),
        };
        let mut encoded = frame.encode().expect("encode").to_vec();
        encoded.push(0);
        assert_eq!(
            Frame::decode(Bytes::from(encoded)),
            Err(WireError::LengthMismatch)
        );
    }

    #[test]
    fn bundle_round_trip_keeps_transactions_independent() {
        let make = |id: u8, payload: &'static [u8]| {
            Frame {
                frame_type: FrameType::RequestData,
                flags: 0,
                transaction_id: [id; 16],
                sequence: 0,
                cumulative_ack: 0,
                metadata: Metadata::default(),
                payload: Bytes::from_static(payload),
            }
            .encode()
            .unwrap()
        };
        let bundle = FrameBundle {
            frames: vec![make(1, b"a"), make(2, b"b")],
        };
        let encoded = bundle.encode().unwrap();
        let decoded = FrameBundle::decode(encoded).unwrap();
        assert_eq!(decoded, bundle);
        assert_ne!(
            Frame::decode(decoded.frames[0].clone())
                .unwrap()
                .transaction_id,
            Frame::decode(decoded.frames[1].clone())
                .unwrap()
                .transaction_id
        );
    }

    #[test]
    fn rejects_frames_over_limit() {
        let frame = Frame {
            frame_type: FrameType::RequestData,
            flags: 0,
            transaction_id: [1; 16],
            sequence: 0,
            cumulative_ack: 0,
            metadata: Metadata::default(),
            payload: Bytes::from(vec![0; DEFAULT_FRAME_LIMIT]),
        };
        assert!(matches!(
            frame.encode(),
            Err(WireError::FrameTooLarge { .. })
        ));
    }
}
