# ==============================================================================
# Unified Processor Service - Dockerfile (Optimized Native Rust)
# ==============================================================================
# Multi-stage build for pure Rust service
# Port: 8090
# ==============================================================================

# ==============================================================================
# Stage 1: Rust builder
# ==============================================================================
FROM rust:1.94-slim-bookworm AS rust-builder

ARG RUST_VERSION=stable

# Install build-time dependencies (minimal for Rust with vendored SSL)
RUN rm -f /etc/apt/sources.list.d/debian.sources && \
    echo "deb http://deb.debian.org/debian bookworm main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian-security bookworm-security main" >> /etc/apt/sources.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    perl \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Use vendored SSL for reliability (reduces system dependencies)
ENV OPENSSL_NO_VENDOR=1 \
    OPENSSL_DIR=/usr \
    OPENSSL_LIB_DIR=/usr/lib/aarch64-linux-gnu \
    OPENSSL_INCLUDE_DIR=/usr/include/openssl \
    CARGO_NET_RETRY=10 \
    CARGO_NET_TIMEOUT=120

WORKDIR /app

# ---------------------------------------------------------------------------
# Dependency caching layer
# ---------------------------------------------------------------------------
COPY Cargo.toml Cargo.lock* ./

# Build a minimal stub that mirrors the actual lib.rs module layout so cargo
# can compile all crate dependencies without the real source files.
RUN mkdir -p \
        api \
        src/core \
        src/processors \
        src/infra \
        src/graph && \
    echo 'fn main() {}' > api/index.rs && \
    printf 'pub mod core;\npub mod processors;\npub mod infra;\npub mod graph;\n' > src/lib.rs && \
    touch \
        src/core/mod.rs \
        src/processors/mod.rs \
        src/infra/mod.rs \
        src/graph/mod.rs

# Cache dependencies
RUN cargo build --release 2>/dev/null; \
    cargo clean -p repo-uni-proc 2>/dev/null; \
    true

# ---------------------------------------------------------------------------
# Real build
# ---------------------------------------------------------------------------
RUN rm -rf src/* api/*
COPY src/ ./src/
COPY api/ ./api/

RUN cargo build --release --features kafka

# ==============================================================================
# Stage 2: Runtime image (Lightweight native Rust)
# ==============================================================================
FROM debian:bookworm-slim AS runtime

# Install only minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# SECURITY: Create a non-root user and group
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Copy the compiled Rust binary and set ownership
COPY --from=rust-builder --chown=appuser:appgroup /app/target/release/unified-processor /usr/local/bin/unified-processor

# Ensure the appuser owns the working directory
RUN chown -R appuser:appgroup /app

# SECURITY: Switch to the non-root user
USER appuser

# PORT is supplied at runtime via .map.env / .secret.env (see .cline/rules/env-policy.md)
EXPOSE 8090

# Remove health check to minimize dependencies - service monitoring can be external

ENTRYPOINT ["dumb-init", "--"]
CMD ["unified-processor"]

