//! In-memory token buckets per principal and per org (per replica). Over the limit → 429
//! MRL-QUOTA-003 with `Retry-After`.

use crate::app::AppState;
use crate::config::{BucketConfig, RateLimitConfig};
use crate::error::{ApiError, ErrorCode};
use crate::seams::RequestContext;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

const PRUNE_AT: usize = 10_000;

#[derive(Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Token buckets keyed by principal and org.
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    /// Limiter with these bucket sizes.
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Takes one token from the principal's bucket and the org's bucket.
    /// `Err(seconds)` when either is empty; then neither bucket is charged.
    pub fn check(&self, principal: &str, org: Option<&str>, now: Instant) -> Result<(), u64> {
        let Ok(mut map) = self.buckets.lock() else {
            // A poisoned lock means a panic mid-update; failing open keeps the server usable.
            return Ok(());
        };
        if map.len() > PRUNE_AT {
            // ponytail: full sweep when large; drop buckets that have refilled completely.
            let burst = f64::from(self.config.principal.burst.max(self.config.org.burst));
            let rate = f64::from(
                self.config
                    .principal
                    .per_second
                    .min(self.config.org.per_second),
            );
            map.retain(|_, b| b.tokens + now.duration_since(b.last).as_secs_f64() * rate < burst);
        }
        let mut keys = vec![(format!("p:{principal}"), self.config.principal)];
        if let Some(org) = org {
            keys.push((format!("o:{org}"), self.config.org));
        }
        let mut wait = 0.0f64;
        let mut refilled = Vec::with_capacity(keys.len());
        for (key, cfg) in &keys {
            let b = refill(map.get(key).copied(), *cfg, now);
            if b.tokens < 1.0 {
                wait = wait.max((1.0 - b.tokens) / f64::from(cfg.per_second));
            }
            refilled.push(b);
        }
        if wait > 0.0 {
            return Err(wait.ceil().max(1.0) as u64);
        }
        for ((key, _), mut b) in keys.into_iter().zip(refilled) {
            b.tokens -= 1.0;
            map.insert(key, b);
        }
        Ok(())
    }
}

fn refill(bucket: Option<Bucket>, cfg: BucketConfig, now: Instant) -> Bucket {
    let burst = f64::from(cfg.burst);
    match bucket {
        None => Bucket {
            tokens: burst,
            last: now,
        },
        Some(b) => Bucket {
            tokens: (b.tokens
                + now.duration_since(b.last).as_secs_f64() * f64::from(cfg.per_second))
            .min(burst),
            last: now,
        },
    }
}

/// The middleware. Runs after authentication.
pub async fn middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    if let Some(ctx) = req.extensions().get::<RequestContext>() {
        if let Err(secs) =
            state
                .limiter()
                .check(&ctx.principal.id, ctx.org.as_deref(), Instant::now())
        {
            return ApiError::new(
                ErrorCode::RateLimited,
                "Too many requests for this account or org.",
            )
            .retry_after(secs)
            .into_response();
        }
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limiter(p: (u32, u32), o: (u32, u32)) -> RateLimiter {
        RateLimiter::new(RateLimitConfig {
            principal: BucketConfig {
                per_second: p.0,
                burst: p.1,
            },
            org: BucketConfig {
                per_second: o.0,
                burst: o.1,
            },
        })
    }

    #[test]
    fn burst_then_refill() {
        let l = limiter((1, 2), (100, 100));
        let t0 = Instant::now();
        assert!(l.check("a", None, t0).is_ok());
        assert!(l.check("a", None, t0).is_ok());
        assert_eq!(l.check("a", None, t0), Err(1));
        assert!(l.check("b", None, t0).is_ok(), "buckets are per principal");
        assert!(l.check("a", None, t0 + Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn org_bucket_is_shared_and_denials_cost_nothing() {
        let l = limiter((100, 100), (1, 2));
        let t0 = Instant::now();
        assert!(l.check("a", Some("acme"), t0).is_ok());
        assert!(l.check("b", Some("acme"), t0).is_ok());
        assert_eq!(l.check("c", Some("acme"), t0), Err(1));
        assert!(l.check("c", Some("other"), t0).is_ok());
        // "c" was denied above, so its own bucket is still full.
        let l2 = limiter((1, 1), (1, 1));
        assert!(l2.check("x", Some("o"), t0).is_ok());
        assert!(l2.check("x", Some("o"), t0).is_err());
    }

    #[test]
    fn retry_after_rounds_up() {
        let l = limiter((1, 1), (100, 100));
        let t0 = Instant::now();
        assert!(l.check("a", None, t0).is_ok());
        assert_eq!(l.check("a", None, t0 + Duration::from_millis(100)), Err(1));
        let slow = limiter((1, 1), (100, 100));
        assert!(slow.check("a", None, t0).is_ok());
        assert_eq!(slow.check("a", None, t0), Err(1));
    }
}
