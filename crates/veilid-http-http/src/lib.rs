//! HTTP head translation without caching or reverse-proxy routing.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;
use url::Url;

/// Ordered HTTP header field preserving repeated header names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderField {
    /// Header name as received.
    pub name: String,
    /// Header value bytes represented losslessly as ISO-8859-1-compatible text in V1 metadata.
    pub value: String,
}

/// HTTP request head carried by VHTTP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHead {
    /// HTTP method token.
    pub method: String,
    /// Absolute path plus optional query. Scheme/host/port are server policy.
    pub path_and_query: String,
    /// End-to-end HTTP fields.
    pub headers: Vec<HeaderField>,
}

/// HTTP response head carried by VHTTP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseHead {
    /// HTTP status code.
    pub status: u16,
    /// End-to-end HTTP fields.
    pub headers: Vec<HeaderField>,
}

/// HTTP translation failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HttpTranslationError {
    /// Method token is invalid.
    #[error("invalid HTTP method")]
    InvalidMethod,
    /// Client attempted to choose scheme, authority, or port.
    #[error("request target must be an absolute path, not an absolute URI")]
    AbsoluteUriForbidden,
    /// CONNECT would create a raw tunnel, which is outside VHTTP/1.
    #[error("HTTP CONNECT is not supported by VHTTP/1")]
    ConnectUnsupported,
    /// Upstream base URL is invalid.
    #[error("invalid upstream URL")]
    InvalidUpstream,
}

/// Validate and normalize a request head.
pub fn normalize_request(mut request: RequestHead) -> Result<RequestHead, HttpTranslationError> {
    let method = http::Method::from_bytes(request.method.as_bytes())
        .map_err(|_| HttpTranslationError::InvalidMethod)?;
    if method == http::Method::CONNECT {
        return Err(HttpTranslationError::ConnectUnsupported);
    }
    if !request.path_and_query.starts_with('/') || request.path_and_query.starts_with("//") {
        return Err(HttpTranslationError::AbsoluteUriForbidden);
    }
    request.headers = strip_hop_by_hop(request.headers);
    Ok(request)
}

/// Remove RFC hop-by-hop fields and tokens named by `Connection`.
#[must_use]
pub fn strip_hop_by_hop(headers: Vec<HeaderField>) -> Vec<HeaderField> {
    let mut denied: HashSet<String> = [
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();

    for header in &headers {
        if header.name.eq_ignore_ascii_case("connection") {
            denied.extend(
                header
                    .value
                    .split(',')
                    .map(|value| value.trim().to_ascii_lowercase()),
            );
        }
    }

    headers
        .into_iter()
        .filter(|header| !denied.contains(&header.name.to_ascii_lowercase()))
        .collect()
}

/// Build the one configured upstream URL. Client scheme/host/port are intentionally ignored.
pub fn upstream_url(base: &str, path_and_query: &str) -> Result<Url, HttpTranslationError> {
    let mut base = Url::parse(base).map_err(|_| HttpTranslationError::InvalidUpstream)?;
    if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
        return Err(HttpTranslationError::InvalidUpstream);
    }
    base.set_path("");
    base.set_query(None);
    base.join(path_and_query)
        .map_err(|_| HttpTranslationError::InvalidUpstream)
}

/// Remove spoofed route headers and add bridge-trusted values.
pub fn attach_route_headers(
    mut headers: Vec<HeaderField>,
    fingerprint: &str,
    route_header: &str,
) -> Vec<HeaderField> {
    headers.retain(|header| {
        !header.name.eq_ignore_ascii_case(route_header)
            && !header.name.eq_ignore_ascii_case("X-Veilid-Origin")
    });
    headers.push(HeaderField {
        name: route_header.to_owned(),
        value: fingerprint.to_owned(),
    });
    headers.push(HeaderField {
        name: "X-Veilid-Origin".to_owned(),
        value: format!("veilid://{fingerprint}"),
    });
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_connection_declared_headers() {
        let headers = vec![
            HeaderField {
                name: "Connection".into(),
                value: "X-Remove".into(),
            },
            HeaderField {
                name: "X-Remove".into(),
                value: "bad".into(),
            },
            HeaderField {
                name: "Content-Type".into(),
                value: "text/plain".into(),
            },
        ];
        let result = strip_hop_by_hop(headers);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Content-Type");
    }

    #[test]
    fn supports_normal_methods_but_not_raw_connect_tunnels() {
        for method in [
            "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE",
        ] {
            let request = RequestHead {
                method: method.into(),
                path_and_query: "/".into(),
                headers: Vec::new(),
            };
            assert!(normalize_request(request).is_ok(), "{method}");
        }
        let connect = RequestHead {
            method: "CONNECT".into(),
            path_and_query: "/".into(),
            headers: Vec::new(),
        };
        assert_eq!(
            normalize_request(connect),
            Err(HttpTranslationError::ConnectUnsupported)
        );
    }

    #[test]
    fn client_cannot_select_upstream_port() {
        let request = RequestHead {
            method: "GET".into(),
            path_and_query: "https://evil.example:9000/".into(),
            headers: Vec::new(),
        };
        assert_eq!(
            normalize_request(request),
            Err(HttpTranslationError::AbsoluteUriForbidden)
        );
    }

    #[test]
    fn joins_path_to_single_upstream() {
        assert_eq!(
            upstream_url("http://proxy:8080/base", "/api?q=1")
                .unwrap()
                .as_str(),
            "http://proxy:8080/api?q=1"
        );
    }

    #[test]
    fn overwrites_spoofed_route_metadata() {
        let result = attach_route_headers(
            vec![HeaderField {
                name: "X-Veilid-Route-Fingerprint".into(),
                value: "fake".into(),
            }],
            "real",
            "X-Veilid-Route-Fingerprint",
        );
        assert!(result.iter().any(|header| header.value == "real"));
        assert!(!result.iter().any(|header| header.value == "fake"));
    }
}

/// Metadata extension containing a MessagePack HTTP request head.
pub const REQUEST_HEAD_EXTENSION: &str = "org.veilidhttp.request-head/v1";
/// Metadata extension containing a MessagePack HTTP response head.
pub const RESPONSE_HEAD_EXTENSION: &str = "org.veilidhttp.response-head/v1";
const COMPRESSION_KEY: &str = "compression";
const LOGICAL_LENGTH_KEY: &str = "logical-length";
const DIGEST_KEY: &str = "blake3";

/// Decoded atomic request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicRequest {
    /// VHTTP transaction identifier.
    pub transaction_id: [u8; 16],
    /// Normalized HTTP request head.
    pub head: RequestHead,
    /// Complete logical request body.
    pub body: bytes::Bytes,
}

/// Decoded atomic response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtomicResponse {
    /// VHTTP transaction identifier.
    pub transaction_id: [u8; 16],
    /// HTTP response head.
    pub head: ResponseHead,
    /// Complete logical response body.
    pub body: bytes::Bytes,
}

/// Atomic VHTTP request/response encoding failures.
#[derive(Debug, Error)]
pub enum AtomicCodecError {
    /// VHTTP wire framing failed.
    #[error(transparent)]
    Wire(#[from] veilid_http_wire::WireError),
    /// HTTP translation validation failed.
    #[error(transparent)]
    Http(#[from] HttpTranslationError),
    /// MessagePack HTTP metadata encoding failed.
    #[error("HTTP metadata encoding failed: {0}")]
    MetadataEncode(String),
    /// MessagePack HTTP metadata decoding failed.
    #[error("HTTP metadata decoding failed: {0}")]
    MetadataDecode(String),
    /// Zstandard compression or bounded decompression failed.
    #[error(transparent)]
    Core(#[from] veilid_http_core::CoreError),
    /// Frame type does not match the requested operation.
    #[error("unexpected VHTTP frame type")]
    UnexpectedFrame,
    /// Required HTTP metadata is absent.
    #[error("required VHTTP HTTP metadata is missing")]
    MissingMetadata,
    /// Body metadata is malformed.
    #[error("invalid VHTTP body metadata")]
    InvalidBodyMetadata,
    /// Reassembled body length does not match the declared logical length.
    #[error("VHTTP body length does not match declared logical length")]
    LengthMismatch,
    /// Reassembled body digest does not match the declared BLAKE3 digest.
    #[error("VHTTP body digest mismatch")]
    DigestMismatch,
}

/// Encode one complete request into an AppCall-sized `REQUEST_OPEN` frame.
///
/// Compression is applied before the frame-size check. Callers should fall back to
/// the streaming transaction path when this returns `WireError::FrameTooLarge`.
///
/// # Errors
///
/// Returns an error for invalid HTTP targets, metadata serialization, compression,
/// or when the complete frame does not fit the configured VHTTP frame limit.
pub fn encode_atomic_request(
    transaction_id: [u8; 16],
    head: RequestHead,
    body: &[u8],
    compress_body: bool,
) -> Result<bytes::Bytes, AtomicCodecError> {
    let head = normalize_request(head)?;
    encode_atomic_frame(
        veilid_http_wire::FrameType::RequestOpen,
        transaction_id,
        REQUEST_HEAD_EXTENSION,
        &head,
        body,
        compress_body,
    )
}

/// Decode one complete `REQUEST_OPEN` AppCall frame.
///
/// # Errors
///
/// Returns an error when framing, metadata, compression, length, digest, or HTTP
/// validation fails.
pub fn decode_atomic_request(
    encoded: bytes::Bytes,
    max_logical_body: usize,
) -> Result<AtomicRequest, AtomicCodecError> {
    let frame = veilid_http_wire::Frame::decode(encoded)?;
    if frame.frame_type != veilid_http_wire::FrameType::RequestOpen {
        return Err(AtomicCodecError::UnexpectedFrame);
    }
    let head: RequestHead = decode_head(&frame, REQUEST_HEAD_EXTENSION)?;
    let head = normalize_request(head)?;
    let body = decode_body(&frame, max_logical_body)?;
    Ok(AtomicRequest {
        transaction_id: frame.transaction_id,
        head,
        body,
    })
}

/// Encode one complete response into an AppCall reply `ATOMIC_RESPONSE` frame.
///
/// # Errors
///
/// Returns an error for metadata serialization, compression, or when the complete
/// response cannot fit the VHTTP frame limit.
pub fn encode_atomic_response(
    transaction_id: [u8; 16],
    head: ResponseHead,
    body: &[u8],
    compress_body: bool,
) -> Result<bytes::Bytes, AtomicCodecError> {
    let head = ResponseHead {
        status: head.status,
        headers: strip_hop_by_hop(head.headers),
    };
    encode_atomic_frame(
        veilid_http_wire::FrameType::AtomicResponse,
        transaction_id,
        RESPONSE_HEAD_EXTENSION,
        &head,
        body,
        compress_body,
    )
}

/// Decode one complete `ATOMIC_RESPONSE` AppCall reply.
///
/// # Errors
///
/// Returns an error when framing, metadata, compression, length, or digest validation fails.
pub fn decode_atomic_response(
    encoded: bytes::Bytes,
    expected_transaction_id: [u8; 16],
    max_logical_body: usize,
) -> Result<AtomicResponse, AtomicCodecError> {
    let frame = veilid_http_wire::Frame::decode(encoded)?;
    if frame.frame_type != veilid_http_wire::FrameType::AtomicResponse
        || frame.transaction_id != expected_transaction_id
    {
        return Err(AtomicCodecError::UnexpectedFrame);
    }
    let mut head: ResponseHead = decode_head(&frame, RESPONSE_HEAD_EXTENSION)?;
    head.headers = strip_hop_by_hop(head.headers);
    let body = decode_body(&frame, max_logical_body)?;
    Ok(AtomicResponse {
        transaction_id: frame.transaction_id,
        head,
        body,
    })
}

fn encode_atomic_frame<T: Serialize>(
    frame_type: veilid_http_wire::FrameType,
    transaction_id: [u8; 16],
    extension_name: &str,
    head: &T,
    body: &[u8],
    compress_body: bool,
) -> Result<bytes::Bytes, AtomicCodecError> {
    use base64::Engine as _;

    let encoded_head = rmp_serde::to_vec_named(head)
        .map_err(|error| AtomicCodecError::MetadataEncode(error.to_string()))?;
    let payload = if compress_body && !body.is_empty() {
        bytes::Bytes::from(veilid_http_core::compress(body, 3)?)
    } else {
        bytes::Bytes::copy_from_slice(body)
    };
    let digest = veilid_http_core::stream_digest(body);
    let mut metadata = veilid_http_wire::Metadata::default();
    metadata
        .extensions
        .insert(extension_name.to_owned(), encoded_head);
    metadata.vhttp.insert(
        COMPRESSION_KEY.to_owned(),
        if compress_body && !body.is_empty() {
            "zstd"
        } else {
            "none"
        }
        .to_owned(),
    );
    metadata
        .vhttp
        .insert(LOGICAL_LENGTH_KEY.to_owned(), body.len().to_string());
    metadata.vhttp.insert(
        DIGEST_KEY.to_owned(),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest),
    );
    veilid_http_wire::Frame {
        frame_type,
        flags: 0,
        transaction_id,
        sequence: 0,
        cumulative_ack: 0,
        metadata,
        payload,
    }
    .encode()
    .map_err(AtomicCodecError::from)
}

fn decode_head<T: for<'de> Deserialize<'de>>(
    frame: &veilid_http_wire::Frame,
    extension_name: &str,
) -> Result<T, AtomicCodecError> {
    let bytes = frame
        .metadata
        .extensions
        .get(extension_name)
        .ok_or(AtomicCodecError::MissingMetadata)?;
    rmp_serde::from_slice(bytes)
        .map_err(|error| AtomicCodecError::MetadataDecode(error.to_string()))
}

fn decode_body(
    frame: &veilid_http_wire::Frame,
    max_logical_body: usize,
) -> Result<bytes::Bytes, AtomicCodecError> {
    use base64::Engine as _;

    let logical_length = frame
        .metadata
        .vhttp
        .get(LOGICAL_LENGTH_KEY)
        .ok_or(AtomicCodecError::InvalidBodyMetadata)?
        .parse::<usize>()
        .map_err(|_| AtomicCodecError::InvalidBodyMetadata)?;
    if logical_length > max_logical_body {
        return Err(AtomicCodecError::Core(
            veilid_http_core::CoreError::DecompressedLimit {
                actual: logical_length,
                limit: max_logical_body,
            },
        ));
    }
    let body = match frame
        .metadata
        .vhttp
        .get(COMPRESSION_KEY)
        .map(String::as_str)
    {
        Some("none") => frame.payload.to_vec(),
        Some("zstd") => veilid_http_core::decompress_bounded(&frame.payload, max_logical_body)?,
        _ => return Err(AtomicCodecError::InvalidBodyMetadata),
    };
    if body.len() != logical_length {
        return Err(AtomicCodecError::LengthMismatch);
    }
    let expected_digest = frame
        .metadata
        .vhttp
        .get(DIGEST_KEY)
        .ok_or(AtomicCodecError::InvalidBodyMetadata)
        .and_then(|encoded| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| AtomicCodecError::InvalidBodyMetadata)
        })?;
    let actual_digest = veilid_http_core::stream_digest(&body);
    if expected_digest.as_slice() != actual_digest.as_slice() {
        return Err(AtomicCodecError::DigestMismatch);
    }
    Ok(bytes::Bytes::from(body))
}

#[cfg(test)]
mod atomic_tests {
    use super::*;

    #[test]
    fn atomic_request_round_trip_preserves_binary_body() {
        let head = RequestHead {
            method: "POST".to_owned(),
            path_and_query: "/api/items?q=1".to_owned(),
            headers: vec![HeaderField {
                name: "content-type".to_owned(),
                value: "application/octet-stream".to_owned(),
            }],
        };
        let body = vec![0_u8, 1, 2, 3, 255, 0, 9];
        let encoded = encode_atomic_request([9; 16], head.clone(), &body, true).unwrap();
        let decoded = decode_atomic_request(encoded, 1024).unwrap();
        assert_eq!(decoded.transaction_id, [9; 16]);
        assert_eq!(decoded.head, head);
        assert_eq!(decoded.body.as_ref(), body);
    }

    #[test]
    fn atomic_response_rejects_wrong_transaction() {
        let encoded = encode_atomic_response(
            [1; 16],
            ResponseHead {
                status: 200,
                headers: Vec::new(),
            },
            b"hello",
            false,
        )
        .unwrap();
        assert!(matches!(
            decode_atomic_response(encoded, [2; 16], 1024),
            Err(AtomicCodecError::UnexpectedFrame)
        ));
    }
}
