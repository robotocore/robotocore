# syntax=docker/dockerfile:1
# Robotocore — pure-Rust AWS emulator image (no Python).
#
# Stage 1 builds the `robotocore-rust` binary with cargo; stage 2 is a
# distroless runtime that ships only the stripped binary plus a static
# busybox used solely for the HTTP healthcheck (distroless has no shell,
# curl or wget of its own).

FROM rust:1.89-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY rotocore/ rotocore/
COPY crates/ crates/
COPY src/ src/

RUN cargo build --release && strip target/release/robotocore-rust

FROM gcr.io/distroless/cc AS runtime

ARG ROBOTOCORE_VERSION=1.0.0
ENV ROBOTOCORE_VERSION=${ROBOTOCORE_VERSION}

COPY --from=builder /app/target/release/robotocore-rust /server/robotocore-rust
# Static busybox binary, only for the healthcheck below.
COPY --from=busybox:1.36.1 /bin/busybox /busybox

EXPOSE 4566

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
    CMD ["/busybox", "wget", "-q", "--spider", "-T", "2", "http://127.0.0.1:4566/_robotocore/health"]

ENTRYPOINT ["/server/robotocore-rust", "--host", "0.0.0.0", "--port", "4566"]
