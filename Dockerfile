# syntax=docker/dockerfile:1

# Multi-stage build for the four xdu binaries (xdu, xdu-find, xdu-view, xdu-rm).
#
# Usage:
#   docker build -t xdu .
#   docker run --rm xdu xdu --help
#
# Copy binaries into another image:
#   COPY --from=ghcr.io/glentner/xdu:latest /usr/local/bin/xdu* /usr/local/bin/
#
# Base images are tag-pinned to Debian bookworm (builder/runtime glibc + libstdc++ match).
# For full supply-chain reproducibility, DIGEST-PIN both before the first container release:
#   docker buildx imagetools inspect rust:1-slim-bookworm    --format '{{.Manifest.Digest}}'
#   docker buildx imagetools inspect debian:bookworm-slim    --format '{{.Manifest.Digest}}'
# then append `@sha256:<digest>` to each FROM line.

# =============================================================================
# Build stage
# =============================================================================
FROM rust:1-slim-bookworm AS builder

WORKDIR /build

# Pin the toolchain first: rust-toolchain.toml is the single source of truth, so it must be
# present before any cargo invocation selects/downloads the pinned toolchain.
COPY rust-toolchain.toml ./
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# BuildKit cache mounts keep the cargo registry and the (heavy, bundled-DuckDB-C++) target
# directory warm across builds, so DuckDB is not recompiled from scratch every time. A cache
# mount is NOT persisted in the image layer, so the freshly built binaries are copied OUT of
# the cache-mounted target dir to /out within the same RUN, before this stage ends.
# --locked pins dependency resolution to the committed Cargo.lock.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked \
        --bin xdu --bin xdu-find --bin xdu-view --bin xdu-rm && \
    mkdir -p /out && \
    cp target/release/xdu \
       target/release/xdu-find \
       target/release/xdu-view \
       target/release/xdu-rm \
       /out/

# =============================================================================
# Runtime stage
# =============================================================================
FROM debian:bookworm-slim AS runtime

# The bundled DuckDB (C++) dynamically links libstdc++/libgcc; ca-certificates is general
# hygiene. bookworm-slim ships neither libstdc++6 by default, so install them explicitly.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libstdc++6 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Run as a non-root user rather than root.
RUN groupadd --system xdu && \
    useradd --system --gid xdu --home-dir /home/xdu --create-home xdu

# Install the four user-facing binaries (release profile already strips them).
COPY --from=builder /out/xdu \
                    /out/xdu-find \
                    /out/xdu-view \
                    /out/xdu-rm \
                    /usr/local/bin/

USER xdu

ENTRYPOINT ["/usr/local/bin/xdu"]
