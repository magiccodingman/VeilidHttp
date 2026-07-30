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
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailer", "transfer-encoding", "upgrade",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();

    for header in &headers {
        if header.name.eq_ignore_ascii_case("connection") {
            denied.extend(header.value.split(',').map(|value| value.trim().to_ascii_lowercase()));
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
    base.join(path_and_query).map_err(|_| HttpTranslationError::InvalidUpstream)
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
    headers.push(HeaderField { name: route_header.to_owned(), value: fingerprint.to_owned() });
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
            HeaderField { name: "Connection".into(), value: "X-Remove".into() },
            HeaderField { name: "X-Remove".into(), value: "bad".into() },
            HeaderField { name: "Content-Type".into(), value: "text/plain".into() },
        ];
        let result = strip_hop_by_hop(headers);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Content-Type");
    }

    #[test]
    fn supports_normal_methods_but_not_raw_connect_tunnels() {
        for method in ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"] {
            let request = RequestHead { method: method.into(), path_and_query: "/".into(), headers: Vec::new() };
            assert!(normalize_request(request).is_ok(), "{method}");
        }
        let connect = RequestHead { method: "CONNECT".into(), path_and_query: "/".into(), headers: Vec::new() };
        assert_eq!(normalize_request(connect), Err(HttpTranslationError::ConnectUnsupported));
    }

    #[test]
    fn client_cannot_select_upstream_port() {
        let request = RequestHead {
            method: "GET".into(),
            path_and_query: "https://evil.example:9000/".into(),
            headers: Vec::new(),
        };
        assert_eq!(normalize_request(request), Err(HttpTranslationError::AbsoluteUriForbidden));
    }

    #[test]
    fn joins_path_to_single_upstream() {
        assert_eq!(
            upstream_url("http://proxy:8080/base", "/api?q=1").unwrap().as_str(),
            "http://proxy:8080/api?q=1"
        );
    }

    #[test]
    fn overwrites_spoofed_route_metadata() {
        let result = attach_route_headers(
            vec![HeaderField { name: "X-Veilid-Route-Fingerprint".into(), value: "fake".into() }],
            "real",
            "X-Veilid-Route-Fingerprint",
        );
        assert!(result.iter().any(|header| header.value == "real"));
        assert!(!result.iter().any(|header| header.value == "fake"));
    }
}
