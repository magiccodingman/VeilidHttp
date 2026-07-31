//! RouteBlob encoding and stable deterministic identity helpers.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use data_encoding::BASE32_NOPAD;
use thiserror::Error;

/// Number of BLAKE3 digest bytes retained for a route fingerprint.
pub const ROUTE_FINGERPRINT_BYTES: usize = 16;
/// Lowercase unpadded RFC 4648 Base32 length for a 128-bit fingerprint.
pub const ROUTE_FINGERPRINT_LEN: usize = 26;

/// Route identity failures.
#[derive(Debug, Error)]
pub enum RouteIdentityError {
    /// The supplied RouteBlob was not valid unpadded Base64URL.
    #[error("RouteBlob must be unpadded Base64URL: {0}")]
    InvalidBase64(#[from] base64::DecodeError),
}

/// Compute the deterministic 128-bit BLAKE3 fingerprint used as the V1 site ID.
#[must_use]
pub fn fingerprint(route_blob: &[u8]) -> String {
    let digest = blake3::hash(route_blob);
    BASE32_NOPAD
        .encode(&digest.as_bytes()[..ROUTE_FINGERPRINT_BYTES])
        .to_ascii_lowercase()
}

/// Validate a V1 site identifier.
#[must_use]
pub fn is_site_id(value: &str) -> bool {
    value.len() == ROUTE_FINGERPRINT_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || matches!(byte, b'2'..=b'7'))
}

/// Decode an unpadded Base64URL RouteBlob.
pub fn decode_route_blob(value: &str) -> Result<Vec<u8>, RouteIdentityError> {
    URL_SAFE_NO_PAD.decode(value.as_bytes()).map_err(Into::into)
}

/// Encode a RouteBlob as unpadded Base64URL for descriptors and CLI output.
#[must_use]
pub fn encode_route_blob(route_blob: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(route_blob)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_deterministic_and_hostname_safe() {
        let first = fingerprint(b"route blob");
        let second = fingerprint(b"route blob");
        assert_eq!(first, second);
        assert_eq!(first.len(), ROUTE_FINGERPRINT_LEN);
        assert!(is_site_id(&first));
        assert_ne!(first, fingerprint(b"another route blob"));
    }

    #[test]
    fn route_blob_encoding_round_trips() {
        let blob = b"\0private route\xff";
        let encoded = encode_route_blob(blob);
        assert_eq!(decode_route_blob(&encoded).unwrap(), blob);
    }

    #[test]
    fn site_id_rejects_ambiguous_or_wrong_length_values() {
        assert!(!is_site_id("ABC"));
        assert!(!is_site_id("0aaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!is_site_id("aaaaaaaaaaaaaaaaaaaaaaaaaa_"));
    }
}
