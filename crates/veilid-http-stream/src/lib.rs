//! Shared VHTTP/1 streaming frame metadata, compression, and integrity state.

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::io::Write as _;
use thiserror::Error;
use veilid_http_core::AckSnapshot;
use veilid_http_http::{RequestHead, ResponseHead};
use veilid_http_wire::{Frame, FrameType, Metadata};

/// Required extension carrying the stream-capable request opening metadata.
pub const REQUEST_OPEN_EXTENSION: &str = "org.veilidhttp.request-open/v1";
/// Required extension carrying request acceptance and initial flow-control state.
pub const REQUEST_ACCEPTED_EXTENSION: &str = "org.veilidhttp.request-accepted/v1";
/// Required extension carrying the streamed response head.
pub const RESPONSE_OPEN_EXTENSION: &str = "org.veilidhttp.response-open/v1";
/// Required extension carrying final stream length and digest.
pub const STREAM_END_EXTENSION: &str = "org.veilidhttp.stream-end/v1";
/// Required extension carrying selective acknowledgement state.
pub const ACK_EXTENSION: &str = "org.veilidhttp.ack/v1";
/// Required extension carrying cancellation information.
pub const CANCEL_EXTENSION: &str = "org.veilidhttp.cancel/v1";
/// Required extension carrying a protocol or upstream error.
pub const ERROR_EXTENSION: &str = "org.veilidhttp.error/v1";

/// Logical stream direction within one HTTP transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamDirection {
    /// Browser/client request body flowing to the server.
    Request,
    /// Upstream response body flowing to the client.
    Response,
}

/// Compression negotiated independently for each logical direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionMode {
    /// Zstandard streaming compression.
    Zstd,
    /// No application compression.
    None,
}

/// Metadata sent in a stream-capable `RequestOpen` AppCall.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestOpen {
    /// Normal HTTP request head.
    pub head: RequestHead,
    /// Publishable client private RouteBlob used for ACKs and response frames.
    pub return_route_blob: Vec<u8>,
    /// Request-body compression mode.
    pub request_compression: CompressionMode,
    /// Whether request body frames will follow.
    pub request_body: bool,
    /// Maximum unacknowledged response frames initially accepted by the client.
    pub response_receive_window: u32,
}

/// Metadata returned when the server accepts a streamed request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestAccepted {
    /// Maximum request data frames the server is currently prepared to receive.
    pub request_receive_window: u32,
}

/// Metadata opening a streamed response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseOpen {
    /// Normal HTTP response status and headers.
    pub head: ResponseHead,
    /// Response-body compression mode.
    pub response_compression: CompressionMode,
    /// Whether response body frames will follow.
    pub response_body: bool,
    /// Request receive-window state piggybacked onto the response opening.
    pub request_receive_window: u32,
}

/// Final integrity metadata for one logical request or response stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEnd {
    /// Which body direction completed.
    pub direction: StreamDirection,
    /// Decompressed logical byte length.
    pub logical_length: u64,
    /// BLAKE3 digest of the complete decompressed stream.
    pub blake3: [u8; 32],
}

/// Cumulative/selective acknowledgement and current receive capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    /// Direction whose data frames are acknowledged.
    pub direction: StreamDirection,
    /// Highest contiguous sequence, or none when sequence zero is missing.
    pub cumulative: Option<u32>,
    /// Selective bitmap for the next 64 sequence numbers.
    pub selective: u64,
    /// Additional frames currently accepted by the receiver.
    pub receive_window: u32,
}

impl Ack {
    /// Construct from the shared core receive-window snapshot.
    #[must_use]
    pub const fn from_snapshot(
        direction: StreamDirection,
        snapshot: AckSnapshot,
        receive_window: u32,
    ) -> Self {
        Self {
            direction,
            cumulative: snapshot.cumulative,
            selective: snapshot.selective,
            receive_window,
        }
    }

    /// Convert to the shared core acknowledgement representation.
    #[must_use]
    pub const fn snapshot(self) -> AckSnapshot {
        AckSnapshot {
            cumulative: self.cumulative,
            selective: self.selective,
        }
    }
}

/// Idempotent transaction cancellation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cancel {
    /// Human-readable reason suitable for diagnostics.
    pub reason: String,
}

/// Structured protocol or upstream error metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamError {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable diagnostic.
    pub message: String,
    /// Whether retrying the transaction may succeed.
    pub retryable: bool,
}

/// Decoded VHTTP streaming frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedFrame {
    /// Stream-capable request opening.
    RequestOpen {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Decoded request opening metadata.
        value: RequestOpen,
        /// Optional initial encoded body bytes carried by the opening frame.
        initial_payload: Bytes,
    },
    /// Server accepted a streamed request body.
    RequestAccepted {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Server acceptance and negotiated receive-window metadata.
        value: RequestAccepted,
    },
    /// Streamed response opening.
    ResponseOpen {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Decoded response opening metadata.
        value: ResponseOpen,
        /// Optional initial encoded response-body bytes.
        initial_payload: Bytes,
    },
    /// Request or response compressed data frame.
    Data {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Logical body direction.
        direction: StreamDirection,
        /// Per-direction sequence number.
        sequence: u32,
        /// Compressed bytes.
        payload: Bytes,
    },
    /// Final stream integrity frame.
    End {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Sequence number immediately following the final data frame.
        sequence: u32,
        /// Final length, digest, and direction metadata.
        value: StreamEnd,
    },
    /// Selective acknowledgement.
    Ack {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Cumulative and selective acknowledgement metadata.
        value: Ack,
    },
    /// Cancellation.
    Cancel {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Cancellation reason metadata.
        value: Cancel,
    },
    /// Structured error.
    Error {
        /// Transaction identifier.
        transaction_id: [u8; 16],
        /// Structured protocol or upstream error metadata.
        value: StreamError,
    },
    /// Frame not owned by the streaming layer.
    Other(Frame),
}

/// Streaming metadata, compression, or integrity failure.
#[derive(Debug, Error)]
pub enum StreamProtocolError {
    /// VHTTP wire framing failed.
    #[error(transparent)]
    Wire(#[from] veilid_http_wire::WireError),
    /// MessagePack metadata encoding failed.
    #[error("stream metadata encoding failed: {0}")]
    MetadataEncode(String),
    /// Required MessagePack metadata was absent or malformed.
    #[error("stream metadata decoding failed: {0}")]
    MetadataDecode(String),
    /// Zstandard stream operation failed.
    #[error("zstandard stream operation failed: {0}")]
    Compression(#[from] std::io::Error),
    /// Stream logical length did not match the end frame.
    #[error("logical stream length mismatch")]
    LengthMismatch,
    /// Stream BLAKE3 digest did not match the end frame.
    #[error("logical stream digest mismatch")]
    DigestMismatch,
    /// Logical stream exceeded its configured receiver bound.
    #[error("logical stream bytes {actual} exceed limit {limit}")]
    LogicalLimit {
        /// Logical bytes observed or declared.
        actual: u64,
        /// Configured maximum logical bytes.
        limit: u64,
    },
    /// Frame direction and frame type conflict.
    #[error("stream direction does not match frame type")]
    DirectionMismatch,
}

/// Encode a stream-capable request opening.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_request_open(
    transaction_id: [u8; 16],
    value: &RequestOpen,
    initial_payload: Bytes,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::RequestOpen,
        transaction_id,
        0,
        REQUEST_OPEN_EXTENSION,
        value,
        initial_payload,
    )
}

/// Encode request acceptance.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_request_accepted(
    transaction_id: [u8; 16],
    value: RequestAccepted,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::RequestAccepted,
        transaction_id,
        0,
        REQUEST_ACCEPTED_EXTENSION,
        &value,
        Bytes::new(),
    )
}

/// Encode a streamed response opening.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_response_open(
    transaction_id: [u8; 16],
    value: &ResponseOpen,
    initial_payload: Bytes,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::ResponseOpen,
        transaction_id,
        0,
        RESPONSE_OPEN_EXTENSION,
        value,
        initial_payload,
    )
}

/// Encode one compressed request or response data frame.
///
/// # Errors
///
/// Returns an error when the complete frame exceeds its bound.
pub fn encode_data(
    transaction_id: [u8; 16],
    direction: StreamDirection,
    sequence: u32,
    payload: Bytes,
) -> Result<Bytes, StreamProtocolError> {
    Frame {
        frame_type: match direction {
            StreamDirection::Request => FrameType::RequestData,
            StreamDirection::Response => FrameType::ResponseData,
        },
        flags: 0,
        transaction_id,
        sequence,
        cumulative_ack: 0,
        metadata: Metadata::default(),
        payload,
    }
    .encode()
    .map_err(Into::into)
}

/// Encode a request or response stream end frame.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_end(
    transaction_id: [u8; 16],
    sequence: u32,
    value: &StreamEnd,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        match value.direction {
            StreamDirection::Request => FrameType::RequestEnd,
            StreamDirection::Response => FrameType::ResponseEnd,
        },
        transaction_id,
        sequence,
        STREAM_END_EXTENSION,
        value,
        Bytes::new(),
    )
}

/// Encode a selective acknowledgement.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_ack(transaction_id: [u8; 16], value: Ack) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::Ack,
        transaction_id,
        0,
        ACK_EXTENSION,
        &value,
        Bytes::new(),
    )
}

/// Encode an idempotent cancellation frame.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_cancel(
    transaction_id: [u8; 16],
    value: &Cancel,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::Cancel,
        transaction_id,
        0,
        CANCEL_EXTENSION,
        value,
        Bytes::new(),
    )
}

/// Encode a structured error frame.
///
/// # Errors
///
/// Returns an error when metadata or the complete frame exceeds its bound.
pub fn encode_error(
    transaction_id: [u8; 16],
    value: &StreamError,
) -> Result<Bytes, StreamProtocolError> {
    extension_frame(
        FrameType::Error,
        transaction_id,
        0,
        ERROR_EXTENSION,
        value,
        Bytes::new(),
    )
}

/// Decode one VHTTP frame into its streaming semantic representation.
///
/// # Errors
///
/// Returns an error for malformed wire frames, missing required extensions, or a
/// direction/frame-type mismatch.
pub fn decode(encoded: Bytes) -> Result<DecodedFrame, StreamProtocolError> {
    let frame = Frame::decode(encoded)?;
    let transaction_id = frame.transaction_id;
    match frame.frame_type {
        FrameType::RequestOpen
            if frame
                .metadata
                .extensions
                .contains_key(REQUEST_OPEN_EXTENSION) =>
        {
            Ok(DecodedFrame::RequestOpen {
                transaction_id,
                value: decode_extension(&frame, REQUEST_OPEN_EXTENSION)?,
                initial_payload: frame.payload,
            })
        }
        FrameType::RequestAccepted => Ok(DecodedFrame::RequestAccepted {
            transaction_id,
            value: decode_extension(&frame, REQUEST_ACCEPTED_EXTENSION)?,
        }),
        FrameType::ResponseOpen => Ok(DecodedFrame::ResponseOpen {
            transaction_id,
            value: decode_extension(&frame, RESPONSE_OPEN_EXTENSION)?,
            initial_payload: frame.payload,
        }),
        FrameType::RequestData => Ok(DecodedFrame::Data {
            transaction_id,
            direction: StreamDirection::Request,
            sequence: frame.sequence,
            payload: frame.payload,
        }),
        FrameType::ResponseData => Ok(DecodedFrame::Data {
            transaction_id,
            direction: StreamDirection::Response,
            sequence: frame.sequence,
            payload: frame.payload,
        }),
        FrameType::RequestEnd | FrameType::ResponseEnd => {
            let value: StreamEnd = decode_extension(&frame, STREAM_END_EXTENSION)?;
            let expected = if frame.frame_type == FrameType::RequestEnd {
                StreamDirection::Request
            } else {
                StreamDirection::Response
            };
            if value.direction != expected {
                return Err(StreamProtocolError::DirectionMismatch);
            }
            Ok(DecodedFrame::End {
                transaction_id,
                sequence: frame.sequence,
                value,
            })
        }
        FrameType::Ack => Ok(DecodedFrame::Ack {
            transaction_id,
            value: decode_extension(&frame, ACK_EXTENSION)?,
        }),
        FrameType::Cancel => Ok(DecodedFrame::Cancel {
            transaction_id,
            value: decode_extension(&frame, CANCEL_EXTENSION)?,
        }),
        FrameType::Error => Ok(DecodedFrame::Error {
            transaction_id,
            value: decode_extension(&frame, ERROR_EXTENSION)?,
        }),
        _ => Ok(DecodedFrame::Other(frame)),
    }
}

fn extension_frame<T: Serialize>(
    frame_type: FrameType,
    transaction_id: [u8; 16],
    sequence: u32,
    extension_name: &str,
    value: &T,
    payload: Bytes,
) -> Result<Bytes, StreamProtocolError> {
    let encoded = rmp_serde::to_vec_named(value)
        .map_err(|error| StreamProtocolError::MetadataEncode(error.to_string()))?;
    let mut metadata = Metadata::default();
    metadata
        .extensions
        .insert(extension_name.to_owned(), encoded);
    Frame {
        frame_type,
        flags: 0,
        transaction_id,
        sequence,
        cumulative_ack: 0,
        metadata,
        payload,
    }
    .encode()
    .map_err(Into::into)
}

fn decode_extension<T: DeserializeOwned>(
    frame: &Frame,
    extension_name: &str,
) -> Result<T, StreamProtocolError> {
    let bytes = frame
        .metadata
        .extensions
        .get(extension_name)
        .ok_or_else(|| StreamProtocolError::MetadataDecode(format!("missing {extension_name}")))?;
    rmp_serde::from_slice(bytes)
        .map_err(|error| StreamProtocolError::MetadataDecode(error.to_string()))
}

#[derive(Debug, Default)]
struct ChunkSink(Vec<u8>);

impl ChunkSink {
    fn take(&mut self) -> Bytes {
        Bytes::from(std::mem::take(&mut self.0))
    }
}

impl std::io::Write for ChunkSink {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Incremental per-direction compressor with final logical integrity state.
pub struct StreamEncoder {
    direction: StreamDirection,
    compression: EncoderState,
    hasher: blake3::Hasher,
    logical_length: u64,
}

enum EncoderState {
    None(ChunkSink),
    Zstd(zstd::stream::write::Encoder<'static, ChunkSink>),
}

impl std::fmt::Debug for StreamEncoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamEncoder")
            .field("direction", &self.direction)
            .field("logical_length", &self.logical_length)
            .finish_non_exhaustive()
    }
}

impl StreamEncoder {
    /// Construct one independent request or response compression stream.
    ///
    /// # Errors
    ///
    /// Returns an error when a Zstandard encoder cannot be initialized.
    pub fn new(
        direction: StreamDirection,
        compression: CompressionMode,
        zstd_level: i32,
    ) -> Result<Self, StreamProtocolError> {
        let compression = match compression {
            CompressionMode::None => EncoderState::None(ChunkSink::default()),
            CompressionMode::Zstd => EncoderState::Zstd(zstd::stream::write::Encoder::new(
                ChunkSink::default(),
                zstd_level,
            )?),
        };
        Ok(Self {
            direction,
            compression,
            hasher: blake3::Hasher::new(),
            logical_length: 0,
        })
    }

    /// Add logical bytes and return compressed bytes emitted so far.
    ///
    /// # Errors
    ///
    /// Returns an error when compression or flushing fails or length exceeds `u64`.
    pub fn push(&mut self, input: &[u8], flush: bool) -> Result<Bytes, StreamProtocolError> {
        let additional =
            u64::try_from(input.len()).map_err(|_| StreamProtocolError::LogicalLimit {
                actual: u64::MAX,
                limit: u64::MAX - 1,
            })?;
        self.logical_length = self.logical_length.checked_add(additional).ok_or(
            StreamProtocolError::LogicalLimit {
                actual: u64::MAX,
                limit: u64::MAX - 1,
            },
        )?;
        self.hasher.update(input);
        match &mut self.compression {
            EncoderState::None(sink) => {
                sink.write_all(input)?;
                Ok(sink.take())
            }
            EncoderState::Zstd(encoder) => {
                encoder.write_all(input)?;
                if flush {
                    encoder.flush()?;
                }
                Ok(encoder.get_mut().take())
            }
        }
    }

    /// Finish compression and return final compressed bytes plus end integrity metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the compressor cannot finish.
    pub fn finish(self) -> Result<(Bytes, StreamEnd), StreamProtocolError> {
        let Self {
            direction,
            compression,
            hasher,
            logical_length,
        } = self;
        let trailing = match compression {
            EncoderState::None(mut sink) => sink.take(),
            EncoderState::Zstd(encoder) => {
                let mut sink = encoder.finish()?;
                sink.take()
            }
        };
        Ok((
            trailing,
            StreamEnd {
                direction,
                logical_length,
                blake3: *hasher.finalize().as_bytes(),
            },
        ))
    }
}

/// Incremental decompressor with a hard logical-output bound and final verification.
pub struct StreamDecoder {
    direction: StreamDirection,
    decompression: DecoderState,
    hasher: blake3::Hasher,
    logical_length: u64,
    max_logical_length: u64,
}

enum DecoderState {
    None(ChunkSink),
    Zstd(zstd::stream::write::Decoder<'static, ChunkSink>),
}

impl std::fmt::Debug for StreamDecoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamDecoder")
            .field("direction", &self.direction)
            .field("logical_length", &self.logical_length)
            .field("max_logical_length", &self.max_logical_length)
            .finish_non_exhaustive()
    }
}

impl StreamDecoder {
    /// Construct one bounded independent decompression stream.
    ///
    /// # Errors
    ///
    /// Returns an error when a Zstandard decoder cannot be initialized.
    pub fn new(
        direction: StreamDirection,
        compression: CompressionMode,
        max_logical_length: u64,
    ) -> Result<Self, StreamProtocolError> {
        let decompression = match compression {
            CompressionMode::None => DecoderState::None(ChunkSink::default()),
            CompressionMode::Zstd => {
                DecoderState::Zstd(zstd::stream::write::Decoder::new(ChunkSink::default())?)
            }
        };
        Ok(Self {
            direction,
            decompression,
            hasher: blake3::Hasher::new(),
            logical_length: 0,
            max_logical_length,
        })
    }

    /// Add compressed bytes and return newly emitted logical bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for decompression failure or logical-output overflow.
    pub fn push(&mut self, input: &[u8], flush: bool) -> Result<Bytes, StreamProtocolError> {
        let output = match &mut self.decompression {
            DecoderState::None(sink) => {
                sink.write_all(input)?;
                sink.take()
            }
            DecoderState::Zstd(decoder) => {
                decoder.write_all(input)?;
                if flush {
                    decoder.flush()?;
                }
                decoder.get_mut().take()
            }
        };
        record_output(
            &mut self.hasher,
            &mut self.logical_length,
            self.max_logical_length,
            &output,
        )?;
        Ok(output)
    }

    /// Finish decompression and verify the final end frame.
    ///
    /// # Errors
    ///
    /// Returns an error for decompression failure, output overflow, direction mismatch,
    /// logical-length mismatch, or digest mismatch.
    pub fn finish(self, expected: &StreamEnd) -> Result<Bytes, StreamProtocolError> {
        let Self {
            direction,
            decompression,
            mut hasher,
            mut logical_length,
            max_logical_length,
        } = self;
        if expected.direction != direction {
            return Err(StreamProtocolError::DirectionMismatch);
        }
        let trailing = match decompression {
            DecoderState::None(mut sink) => sink.take(),
            DecoderState::Zstd(mut decoder) => {
                decoder.flush()?;
                let mut sink = decoder.into_inner();
                sink.take()
            }
        };
        record_output(
            &mut hasher,
            &mut logical_length,
            max_logical_length,
            &trailing,
        )?;
        if logical_length != expected.logical_length {
            return Err(StreamProtocolError::LengthMismatch);
        }
        if hasher.finalize().as_bytes() != &expected.blake3 {
            return Err(StreamProtocolError::DigestMismatch);
        }
        Ok(trailing)
    }
}

fn record_output(
    hasher: &mut blake3::Hasher,
    logical_length: &mut u64,
    max_logical_length: u64,
    output: &[u8],
) -> Result<(), StreamProtocolError> {
    let additional =
        u64::try_from(output.len()).map_err(|_| StreamProtocolError::LogicalLimit {
            actual: u64::MAX,
            limit: max_logical_length,
        })?;
    let actual = logical_length.saturating_add(additional);
    if actual > max_logical_length {
        return Err(StreamProtocolError::LogicalLimit {
            actual,
            limit: max_logical_length,
        });
    }
    *logical_length = actual;
    hasher.update(output);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use veilid_http_core::{Reassembler, ReceiveWindow};
    use veilid_http_http::HeaderField;

    #[test]
    fn request_open_round_trip_carries_private_return_route() {
        let value = RequestOpen {
            head: RequestHead {
                method: "POST".to_owned(),
                path_and_query: "/upload?q=1".to_owned(),
                headers: vec![HeaderField {
                    name: "content-type".to_owned(),
                    value: "application/octet-stream".to_owned(),
                }],
            },
            return_route_blob: vec![0, 1, 2, 255],
            request_compression: CompressionMode::Zstd,
            request_body: true,
            response_receive_window: 32,
        };
        let encoded = encode_request_open([7; 16], &value, Bytes::from_static(b"first")).unwrap();
        assert_eq!(
            decode(encoded).unwrap(),
            DecodedFrame::RequestOpen {
                transaction_id: [7; 16],
                value,
                initial_payload: Bytes::from_static(b"first"),
            }
        );
    }

    #[test]
    fn selective_ack_survives_messagepack_round_trip() {
        let mut receive = ReceiveWindow::default();
        receive.record(0);
        receive.record(2);
        let ack = Ack::from_snapshot(StreamDirection::Request, receive.snapshot(), 14);
        let decoded = decode(encode_ack([3; 16], ack).unwrap()).unwrap();
        assert_eq!(
            decoded,
            DecodedFrame::Ack {
                transaction_id: [3; 16],
                value: ack
            }
        );
    }

    #[test]
    fn zstd_stream_round_trip_across_arbitrary_boundaries() {
        let input = (0..300_000_u32)
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        let mut encoder =
            StreamEncoder::new(StreamDirection::Response, CompressionMode::Zstd, 3).unwrap();
        let mut compressed = Vec::new();
        for chunk in input.chunks(7919) {
            compressed.extend_from_slice(&encoder.push(chunk, false).unwrap());
        }
        let (trailing, end) = encoder.finish().unwrap();
        compressed.extend_from_slice(&trailing);

        let mut decoder = StreamDecoder::new(
            StreamDirection::Response,
            CompressionMode::Zstd,
            u64::try_from(input.len()).unwrap(),
        )
        .unwrap();
        let mut output = Vec::new();
        for chunk in compressed.chunks(1543) {
            output.extend_from_slice(&decoder.push(chunk, false).unwrap());
        }
        output.extend_from_slice(&decoder.finish(&end).unwrap());
        assert_eq!(output, input);
    }

    #[test]
    fn reordered_and_duplicate_data_delivers_once_in_order() {
        let frames = [
            encode_data(
                [1; 16],
                StreamDirection::Request,
                0,
                Bytes::from_static(b"a"),
            )
            .unwrap(),
            encode_data(
                [1; 16],
                StreamDirection::Request,
                1,
                Bytes::from_static(b"b"),
            )
            .unwrap(),
            encode_data(
                [1; 16],
                StreamDirection::Request,
                2,
                Bytes::from_static(b"c"),
            )
            .unwrap(),
        ];
        let mut reassembler = Reassembler::new(1024);
        let mut output = Vec::new();
        for index in [2_usize, 0, 2, 1] {
            let DecodedFrame::Data {
                sequence, payload, ..
            } = decode(frames[index].clone()).unwrap()
            else {
                panic!("expected data frame");
            };
            for ready in reassembler.push(sequence, payload).unwrap() {
                output.extend_from_slice(&ready);
            }
        }
        assert_eq!(output, b"abc");
    }

    #[test]
    fn corrupted_end_digest_is_rejected() {
        let mut encoder =
            StreamEncoder::new(StreamDirection::Request, CompressionMode::None, 0).unwrap();
        let compressed = encoder.push(b"hello", false).unwrap();
        let (_, mut end) = encoder.finish().unwrap();
        end.blake3[0] ^= 1;
        let mut decoder =
            StreamDecoder::new(StreamDirection::Request, CompressionMode::None, 10).unwrap();
        assert_eq!(
            decoder.push(&compressed, false).unwrap(),
            Bytes::from_static(b"hello")
        );
        assert!(matches!(
            decoder.finish(&end),
            Err(StreamProtocolError::DigestMismatch)
        ));
    }
}
