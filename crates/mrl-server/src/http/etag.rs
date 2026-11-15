//! `ETag` / `If-Match` helpers for optimistic concurrency.
//!
//! ```text
//! let current = etag::from_version(project.version);          // "\"7\""
//! etag::check_if_match(&headers, &current, true)?;             // 428 if missing, 412 if stale
//! Ok(([(header::ETAG, current)], Json(updated)))
//! ```

use crate::error::{ApiError, ErrorCode};
use axum::http::{header, HeaderMap, HeaderValue};
use sha2::Digest;

/// A strong `ETag` for a resource version (a counter, hash or timestamp).
pub fn from_version(version: impl std::fmt::Display) -> HeaderValue {
    let tag = format!("\"{version}\"");
    // Versions are counters or hashes; anything else falls back to a hash of the text.
    HeaderValue::from_str(&tag).unwrap_or_else(|_| {
        let digest = sha2::Sha256::digest(tag.as_bytes());
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        HeaderValue::from_str(&format!("\"{hex}\"")).unwrap_or(HeaderValue::from_static("\"0\""))
    })
}

/// Checks `If-Match` against `current` (strong comparison, `*` matches anything).
///
/// Missing header: 428 MRL-CONFLICT-002 when `required`, otherwise allowed.
/// No listed tag matches: 412 MRL-CONFLICT-001.
pub fn check_if_match(
    headers: &HeaderMap,
    current: &HeaderValue,
    required: bool,
) -> Result<(), ApiError> {
    let values: Vec<&str> = headers
        .get_all(header::IF_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .collect();
    if values.is_empty() {
        return if required {
            Err(ApiError::new(
                ErrorCode::PreconditionRequired,
                "Send the item's ETag in If-Match to change it.",
            ))
        } else {
            Ok(())
        };
    }
    let current = current.to_str().unwrap_or("");
    let matches = values
        .iter()
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .any(|tag| tag == "*" || (!tag.starts_with("W/") && tag == current));
    if matches {
        Ok(())
    } else {
        Err(ApiError::new(
            ErrorCode::PreconditionFailed,
            "The item changed since you loaded it.",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(if_match: &[&str]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for v in if_match {
            h.append(header::IF_MATCH, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn etag_is_quoted() {
        assert_eq!(from_version(7), "\"7\"");
        assert_eq!(from_version("abc"), "\"abc\"");
        assert!(from_version("bad\nvalue").to_str().unwrap().len() == 66);
    }

    #[test]
    fn if_match_cases() {
        let cur = from_version(3);
        assert!(check_if_match(&headers(&["\"3\""]), &cur, true).is_ok());
        assert!(check_if_match(&headers(&["\"1\", \"3\""]), &cur, true).is_ok());
        assert!(check_if_match(&headers(&["\"1\"", "\"3\""]), &cur, true).is_ok());
        assert!(check_if_match(&headers(&["*"]), &cur, true).is_ok());
        assert!(check_if_match(&headers(&[]), &cur, false).is_ok());
        let stale = check_if_match(&headers(&["\"2\""]), &cur, true).unwrap_err();
        assert_eq!(
            (stale.code, stale.code.status().as_u16()),
            (ErrorCode::PreconditionFailed, 412)
        );
        let weak = check_if_match(&headers(&["W/\"3\""]), &cur, true).unwrap_err();
        assert_eq!(weak.code, ErrorCode::PreconditionFailed);
        let missing = check_if_match(&headers(&[]), &cur, true).unwrap_err();
        assert_eq!(
            (missing.code, missing.code.status().as_u16()),
            (ErrorCode::PreconditionRequired, 428)
        );
    }
}
