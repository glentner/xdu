# syntax=docker/dockerfile:1

# Multi-stage build for xdu binaries.
#
# Usage:
#   docker build -t xdu .
#   docker run --rm xdu xdu --help
#
# Copy binaries into another image:
#   COPY --from=ghcr.io/glentner/xdu:latest /usr/local/bin/xdu* /usr/local/bin/

# =============================================================================
# Build stage
# =============================================================================
FROM rust:slim AS builder

WORKDIR /build

# Copy source and build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release && \
    strip target/release/xdu \
          target/release/xdu-find \
          target/release/xdu-view \
          target/release/xdu-rm

# =============================================================================
# Runtime stage
# =============================================================================
FROM debian:bookworm-slim

# Copy binaries
COPY --from=builder /build/target/release/xdu /usr/local/bin/
COPY --from=builder /build/target/release/xdu-find /usr/local/bin/
COPY --from=builder /build/target/release/xdu-view /usr/local/bin/
COPY --from=builder /build/target/release/xdu-rm /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/xdu"]
