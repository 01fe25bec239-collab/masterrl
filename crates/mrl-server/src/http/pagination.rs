//! Cursor pagination: `?limit=50&cursor=…` → `{ "items": [...], "next_cursor": "…" | null }`.
//!
//! ```text
//! async fn list(Query(q): Query<PageQuery>) -> Result<Json<Page<Run>>, ApiError> {
//!     let page = q.resolve()?;                                  // limit 1–200, decoded cursor
//!     let rows = repo.runs_after(page.after.as_deref(), page.fetch()).await?; // limit + 1 rows
//!     Ok(Json(Page::from_overfetch(rows, &page, |r| r.id.clone())))
//! }
//! ```

use crate::error::{ApiError, ErrorCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

/// Default page size.
pub const DEFAULT_LIMIT: usize = 50;
/// Largest page size.
pub const MAX_LIMIT: usize = 200;
const CURSOR_PREFIX: &str = "mrl1:";

/// Raw query parameters. Strings, so bad values get our error instead of the framework's.
#[derive(Debug, Clone, Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PageQuery {
    /// Page size, 1–200 (default 50).
    pub limit: Option<String>,
    /// `next_cursor` from the previous page.
    pub cursor: Option<String>,
}

/// A checked page request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    /// Page size.
    pub limit: usize,
    /// Position to continue after (the decoded cursor).
    pub after: Option<String>,
}

impl PageRequest {
    /// Rows to fetch: one more than `limit`, to know whether another page exists.
    pub fn fetch(&self) -> usize {
        self.limit + 1
    }
}

impl PageQuery {
    /// Checks `limit` and decodes `cursor` (400 MRL-VAL-005 otherwise).
    pub fn resolve(&self) -> Result<PageRequest, ApiError> {
        let limit = match self.limit.as_deref() {
            None => DEFAULT_LIMIT,
            Some(s) => match s.parse::<usize>() {
                Ok(n) if (1..=MAX_LIMIT).contains(&n) => n,
                _ => {
                    return Err(ApiError::new(
                        ErrorCode::Pagination,
                        format!("limit {s:?} must be a whole number from 1 to {MAX_LIMIT}."),
                    ))
                }
            },
        };
        let after = self.cursor.as_deref().map(decode_cursor).transpose()?;
        Ok(PageRequest { limit, after })
    }
}

/// One page of results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct Page<T> {
    /// The items on this page.
    pub items: Vec<T>,
    /// Pass as `cursor` for the next page; `null` on the last page.
    pub next_cursor: Option<String>,
}

impl<T> Page<T> {
    /// Builds a page from `limit + 1` fetched rows; `key` gives a row's position.
    pub fn from_overfetch(mut rows: Vec<T>, req: &PageRequest, key: impl Fn(&T) -> String) -> Self {
        let more = rows.len() > req.limit;
        rows.truncate(req.limit);
        let next_cursor = if more {
            rows.last().map(|r| encode_cursor(&key(r)))
        } else {
            None
        };
        Page {
            items: rows,
            next_cursor,
        }
    }
}

/// Wraps a position into an opaque cursor.
pub fn encode_cursor(position: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{CURSOR_PREFIX}{position}"))
}

/// Unwraps a cursor made by `encode_cursor`.
pub fn decode_cursor(cursor: &str) -> Result<String, ApiError> {
    let bad = || {
        ApiError::new(
            ErrorCode::Pagination,
            "cursor must be the next_cursor value from an earlier page, unchanged.",
        )
    };
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| bad())?;
    let text = String::from_utf8(bytes).map_err(|_| bad())?;
    text.strip_prefix(CURSOR_PREFIX)
        .map(str::to_owned)
        .ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(limit: Option<&str>, cursor: Option<&str>) -> PageQuery {
        PageQuery {
            limit: limit.map(str::to_owned),
            cursor: cursor.map(str::to_owned),
        }
    }

    #[test]
    fn cursor_round_trips_and_is_opaque() {
        for pos in ["", "run_0193", "2026-11-15T00:00:00Z|run_1", "ünïcode/+="] {
            let c = encode_cursor(pos);
            assert!(!c.contains(pos) || pos.is_empty());
            assert!(c
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
            assert_eq!(decode_cursor(&c).unwrap(), pos);
        }
    }

    #[test]
    fn foreign_cursors_are_rejected() {
        for bad in ["!!", "cnVuXzE", &URL_SAFE_NO_PAD.encode([0xff, 0xfe])] {
            assert_eq!(
                decode_cursor(bad).unwrap_err().code,
                ErrorCode::Pagination,
                "{bad}"
            );
        }
    }

    #[test]
    fn limit_defaults_and_bounds() {
        assert_eq!(q(None, None).resolve().unwrap().limit, 50);
        assert_eq!(q(Some("1"), None).resolve().unwrap().limit, 1);
        assert_eq!(q(Some("200"), None).resolve().unwrap().limit, 200);
        for bad in ["0", "201", "-1", "ten", ""] {
            assert_eq!(
                q(Some(bad), None).resolve().unwrap_err().code,
                ErrorCode::Pagination
            );
        }
        let c = encode_cursor("run_9");
        assert_eq!(
            q(None, Some(&c)).resolve().unwrap().after.as_deref(),
            Some("run_9")
        );
    }

    #[test]
    fn overfetch_sets_next_cursor_only_when_more() {
        let req = PageRequest {
            limit: 2,
            after: None,
        };
        let page = Page::from_overfetch(vec![1, 2, 3], &req, |n| n.to_string());
        assert_eq!(page.items, vec![1, 2]);
        assert_eq!(
            decode_cursor(page.next_cursor.as_deref().unwrap()).unwrap(),
            "2"
        );
        let last = Page::from_overfetch(vec![1, 2], &req, |n| n.to_string());
        assert_eq!(last.next_cursor, None);
        let json = serde_json::to_value(&last).unwrap();
        assert!(json["next_cursor"].is_null());
    }
}
