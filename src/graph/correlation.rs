//! Per-request observability: correlation IDs and structured access logs.
//!
//! Why: every HTTP request produces exactly one access record classified by
//! outcome — `success` (2xx), `redirect` (3xx + target), `client_error` (4xx)
//! or `server_error` (5xx) — so failures and redirects are as visible as
//! successes. The `x-correlation-id` header is honoured from upstream callers
//! (or generated) and echoed on the response, so one request can be traced
//! across every service it touches (Kafka consumers reuse the same header).

use std::time::Instant;

use axum::{extract::Request, middleware::Next, response::Response};
use tracing::{error, info, warn, Instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// Correlation-ID header honoured from upstream callers and echoed back.
pub const CORRELATION_HEADER: &str = "x-correlation-id";

/// Read a required env var, failing fast and naming it (env-policy).
fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("Missing required environment variable: {name}"))
}

/// Redact secret-looking query values so access logs never leak credentials.
fn sanitize_query(query: Option<&str>) -> Option<String> {
    query.map(|raw| {
        raw.split('&')
            .map(|pair| {
                let key = pair.split('=').next().unwrap_or("");
                if ["token", "key", "secret", "password", "auth"]
                    .iter()
                    .any(|needle| key.to_ascii_lowercase().contains(needle))
                {
                    format!("{key}=[redacted]")
                } else {
                    pair.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("&")
    })
}

/// Initialise structured tracing from env (env-policy: no baked-in fallback).
///
/// `RUST_LOG` (full EnvFilter directives) overrides `LOG_LEVEL`; `LOG_FORMAT`
/// selects `json` (machine-parseable) or `console` (human-readable) output.
pub fn init_tracing() {
    let filter = std::env::var("RUST_LOG")
        .or_else(|_| std::env::var("LOG_LEVEL"))
        .unwrap_or_else(|_| {
            panic!("Missing required environment variable: LOG_LEVEL (or RUST_LOG)")
        });
    let format = required_env("LOG_FORMAT");
    let env_filter = tracing_subscriber::EnvFilter::new(filter);
    if format == "json" {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .flatten_event(true)
                    .with_current_span(true)
                    .with_span_list(false),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .init();
    }
}

/// Correlation + access-log middleware (wired globally in `core::routes`).
pub async fn correlation_middleware(mut request: Request, next: Next) -> Response {
    let correlation_id = request
        .headers()
        .get(CORRELATION_HEADER)
        .or_else(|| request.headers().get("x-request-id"))
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Inject back into request headers so downstream handlers/clients see it
    if let Ok(val) = correlation_id.parse() {
        request.headers_mut().insert(CORRELATION_HEADER, val);
    }

    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = sanitize_query(request.uri().query());
    let user_agent = request
        .headers()
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());

    let span = tracing::info_span!(
        "request",
        correlation_id = %correlation_id,
        method = %method,
        path = %path,
    );

    let start = Instant::now();
    info!(query = ?query, user_agent = ?user_agent, "request started");
    let mut response = next.run(request).instrument(span).await;
    let duration_ms = start.elapsed().as_millis() as u64;
    let status = response.status().as_u16();

    if let Ok(val) = correlation_id.parse() {
        response.headers_mut().insert(CORRELATION_HEADER, val);
    }

    match status {
        200..=299 => info!(status, duration_ms, outcome = "success", "request completed"),
        300..=399 => warn!(
            status,
            duration_ms,
            outcome = "redirect",
            location = ?response.headers().get("location"),
            "request redirected"
        ),
        400..=499 => warn!(status, duration_ms, outcome = "client_error", "request failed"),
        _ => error!(status, duration_ms, outcome = "server_error", "request failed"),
    }

    response
}
