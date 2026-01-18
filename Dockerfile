# syntax=docker/dockerfile:1

# Multi-stage build for static xdu binaries.
#
# Usage:
#   docker build -t xdu .
#   docker run --rm xdu xdu --help
#
# Copy binaries into another image:
#   COPY --from=ghcr.io/glentner/xdu:latest /xdu /xdu-find /xdu-view /usr/local/bin/

# =============================================================================
# Build stage: compile static musl binaries
# =============================================================================
FROM rust:alpine AS builder

# Install build dependencies (Alpine uses musl natively)
RUN apk add --no-cache musl-dev gcc g++

WORKDIR /build

# Copy source and build
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release && \
    strip target/release/xdu \
          target/release/xdu-find \
          target/release/xdu-view

# =============================================================================
# Release stage: minimal image with just the binaries
# =============================================================================
FROM scratch

# Copy static binaries to root for easy COPY --from= usage
COPY --from=builder /build/target/release/xdu /xdu
COPY --from=builder /build/target/release/xdu-find /xdu-find
COPY --from=builder /build/target/release/xdu-view /xdu-view

# Also place in /usr/local/bin for direct container usage
COPY --from=builder /build/target/release/xdu /usr/local/bin/xdu
COPY --from=builder /build/target/release/xdu-find /usr/local/bin/xdu-find
COPY --from=builder /build/target/release/xdu-view /usr/local/bin/xdu-view

ENTRYPOINT ["/usr/local/bin/xdu"]
