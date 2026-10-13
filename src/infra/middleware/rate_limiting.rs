//! Axum rate limiting middleware
//!
//! In-memory sliding window rate limiting using DashMap.
//! Provides per-path limit configuration and standard rate limit headers.


use axum::{
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Rate limit configuration
#[derive(Clone)]
pub struct AxumRateLimitConfig {
    pub counters: Arc<DashMap<String, Vec<u64>>>,
    pub default_limit: u32,
    pub search_limit: u32,
    pub sources_limit: u32,
    pub sync_limit: u32,
    pub window_secs: u64,
    pub skip_rate_limiting: bool,
}

impl AxumRateLimitConfig {
    pub fn new(
        default_limit: u32,
        search_limit: u32,
        sources_limit: u32,
        sync_limit: u32,
        window_secs: u64,
        skip_rate_limiting: bool,
    ) -> Self {
        Self {
            counters: Arc::new(DashMap::new()),
            default_limit,
            search_limit,
            sources_limit,
            sync_limit,
            window_secs,
            skip_rate_limiting,
        }
    }

    /// Create with defaults suitable for most services
    pub fn default_for_service(default_limit: u32) -> Self {
        Self::new(default_limit, default_limit / 2, default_limit, default_limit / 5, 60, false)
    }

    /// Get limit for an endpoint path
    pub fn get_limit_for_path(&self, path: &str) -> u32 {
        if path.contains("/search") {
            self.search_limit
        } else if path.contains("/sources") {
            self.sources_limit
        } else if path.contains("/sync") {
            self.sync_limit
        } else {
            self.default_limit
        }
    }
}

/// Rate limit info for response headers
pub struct RateLimitInfo {
    pub limit: u32,
    pub remaining: u32,
    pub reset: u64,
}

/// Rate limiting middleware for Axum

pub async fn axum_rate_limit_middleware(
    State(config): State<AxumRateLimitConfig>,
    request: Request,
    next: Next,
) -> Response {
    // Skip rate limiting if toggle is enabled
    if config.skip_rate_limiting {
        return next.run(request).await;
    }

    // Get client identifier (IP or user ID)
    let client_id = get_client_id(&request);
    let path = request.uri().path().to_string();
    let limit = config.get_limit_for_path(&path);

    match check_rate_limit(&config, &client_id, &path, limit) {
        Ok(info) => {
            let mut response = next.run(request).await;

            // Add rate limit headers
            if let Ok(val) = HeaderValue::from_str(&info.limit.to_string()) {
                response.headers_mut().insert("X-RateLimit-Limit", val);
            }
            if let Ok(val) = HeaderValue::from_str(&info.remaining.to_string()) {
                response.headers_mut().insert("X-RateLimit-Remaining", val);
            }
            if let Ok(val) = HeaderValue::from_str(&info.reset.to_string()) {
                response.headers_mut().insert("X-RateLimit-Reset", val);
            }

            response
        }
        Err(_) => {
            // Rate limit exceeded
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                serde_json::json!({
                    "error": {
                        "code": "RATE_LIMITED",
                        "message": "Too many requests"
                    }
                })
                .to_string(),
            )
                .into_response();

            if let Ok(val) = HeaderValue::from_str(&limit.to_string()) {
                response.headers_mut().insert("X-RateLimit-Limit", val);
            }
            if let Ok(val) = HeaderValue::from_str("0") {
                response.headers_mut().insert("X-RateLimit-Remaining", val);
            }

            response
        }
    }
}

/// Get client identifier for rate limiting

fn get_client_id(request: &Request) -> String {
    // Try user ID from auth middleware
    if let Some(user) = request
        .extensions()
        .get::<super::auth::AuthenticatedUser>()
    {
        return format!("user:{}", user.id);
    }

    // Fall back to IP address
    request
        .headers()
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            request
                .headers()
                .get("X-Real-IP")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// Check rate limit using in-memory sliding window
fn check_rate_limit(
    config: &AxumRateLimitConfig,
    client_id: &str,
    path: &str,
    limit: u32,
) -> Result<RateLimitInfo, ()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after epoch")
        .as_secs();

    let window_start = now - config.window_secs;
    let key = format!("ratelimit:{}:{}", client_id, path.replace('/', "_"));

    let mut entry = config.counters.entry(key).or_default();

    // Remove old entries outside the window
    entry.retain(|&ts| ts > window_start);

    // Add current request timestamp
    entry.push(now);

    let count = entry.len() as u32;

    if count > limit {
        Err(())
    } else {
        Ok(RateLimitInfo {
            limit,
            remaining: limit.saturating_sub(count),
            reset: now + config.window_secs,
        })
    }
}
