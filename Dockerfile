# syntax=docker/dockerfile:1
# Robotocore — AWS twin: pure-Rust gateway on a distroless python3 runner.
#
# Stage 1 builds the `robotocore-rust` binary with cargo. The default stage
# (`runtime`, kept LAST: plain `docker build .` emits the final stage) is a
# distroless python3 base (bookworm) shipping the stripped binary, a python
# interpreter for the in-process python-family Lambda executor, and a static
# busybox used solely for the HTTP healthcheck (distroless has no shell,
# curl or wget of its own). Two optional variant stages are declared above
# it — `runtimes` (same runner on a Debian-slim base + Node.js/Ruby on
# PATH) and `java-and-dotnet` (runtimes + OpenJDK 21 + .NET SDK 8) — neither
# is part of an untargeted build; build them explicitly with `--target`.

FROM rust:1.99-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY rotocore/ rotocore/
COPY crates/ crates/
COPY src/ src/

RUN cargo build --locked --release && strip target/release/robotocore-rust

# ---- runtimes stage: Debian-slim runner + Node.js + Ruby on PATH ----
# Build with: docker build --target runtimes -t robotocore:runtimes .
#
# The mirror of the Python twin's "standard" image: same stripped binary,
# same /busybox healthcheck and ENTRYPOINT as the distroless `runtime`
# stage below, but on a Debian-slim base carrying nodejs (trixie ships
# 20.x) and ruby (trixie ships 3.3) under /usr/bin. The gateway's
# /_robotocore/runtimes probe and the nodejs Lambda executor look for
# `node` via std::env::var("PATH") (crates/lambda find_node), so the
# runtime dirs must be on the image's PATH ENV.
#
# The busybox shim is pulled from the same busybox:1.36.1 source the
# distroless stage uses rather than `COPY --from=runtime`: Dockerfile
# stages can only reference stages declared above them, and the distroless
# default must stay the last stage so untargeted builds keep emitting it.
FROM debian:trixie-slim AS runtimes

# Explicit ENV belt (also governs docker exec paths): the trixie base
# image does set a PATH env — spelling it out here pins the same value in
# this stage's config so `docker exec` and the runtime probes run against
# the same searchable dirs.
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates nodejs ruby \
    && rm -rf /var/lib/apt/lists/*

# Same stripped binary the distroless image ships.
COPY --from=builder /app/target/release/robotocore-rust /server/robotocore-rust
# Same glibc busybox:1.36.1 the stage always copied: the trixie base's
# glibc (>= 2.38) satisfies the dynamic /bin/busybox artifact, unlike the
# bookworm default stage's musl busybox (see below).
COPY --from=busybox:1.36.1 /bin/busybox /busybox

EXPOSE 4566

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
    CMD ["/busybox", "wget", "-q", "--spider", "-T", "2", "http://127.0.0.1:4566/_robotocore/health"]

ENTRYPOINT ["/server/robotocore-rust", "--host", "0.0.0.0", "--port", "4566"]

# ---- java-and-dotnet stage: runtimes + OpenJDK 21 + .NET SDK 8 ----
# Build with: docker build --target java-and-dotnet -t robotocore:java-and-dotnet .
#
# Mirror of the Python twin's java-and-dotnet image: identical ENTRYPOINT
# as the other stages, plus the two toolchains its Lambda runtimes use.
FROM runtimes AS java-and-dotnet

# openjdk-21-jdk-headless: javac (bootstrap compilation) + java (Lambda
# execution). Trixie ships OpenJDK 21 in its default repos — no backports.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# .NET SDK 8 via Microsoft's official install script (Debian ships no
# dotnet packages). --channel 8.0 tracks the SDK 8 patch line at build
# time, exactly like the Python twin's java-and-dotnet stage did; wget is
# dropped again after the download, ca-certificates stays.
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && wget -q https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh --channel 8.0 --install-dir /usr/share/dotnet \
    && ln -s /usr/share/dotnet/dotnet /usr/local/bin/dotnet \
    && rm -f /tmp/dotnet-install.sh \
    && apt-get remove -y wget && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.dotnet /tmp/NuGetScratch

# JAVA_HOME must not hardcode the Debian arch suffix (…-amd64 vs …-arm64)
# the way the Python twin did; symlink it arch-neutrally instead.
RUN ln -snf "/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)" \
            /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1
ENV DOTNET_NOLOGO=1

# ---- default stage: distroless python3 runner (bookworm) ----
# The python-wise default: base it on gcr.io/distroless/python3-debian12 so
# the in-process Lambda shim (crates/lambda/src/runtime.rs find_python3 -> a
# bare `python3` on PATH) can actually execute python-family handlers from
# the image itself — the behavior the default image claims on
# /_robotocore/runtimes (available includes "python") and the example in
# AGENTS.md (create+invoke a python lambda straight from `docker run`).
# The base is bookworm — the same distro line the builder's
# rust:1.99-slim-bookworm compiles against (so the binary links against the
# same ELF interpreter/libc set it was built with; verified live) — and
# carries /usr/bin/python3 (alias to python3.11) plus the libpython/ssl
# runtime libs, while staying distroless: no shell, no package manager.
# The only executables are python, our server binary and the healthcheck
# busybox below.
FROM gcr.io/distroless/python3-debian12 AS runtime

# Pin the same PATH the other stages spell out so the bare-name lookups in
# the exec paths (crates/lambda find_python3, server.rs which()) resolve the
# same on `docker run` and `docker exec` — this distroless base defines no
# PATH in its config.
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

COPY --from=builder /app/target/release/robotocore-rust /server/robotocore-rust
# Healthcheck busybox sourced from the `-musl` tag on purpose: on aarch64
# the busybox:1.36.1 glibc artifact is NOT static (/bin/busybox requires
# glibc >= 2.38), which a bookworm-class base like this one (or a
# bookworm-pinned cc) cannot satisfy — the old default only served the
# healthcheck because untagged `gcr.io/distroless/cc` currently tracks
# trixie (glibc 2.41). The musl variant is truly static and runs on every
# base and arch.
COPY --from=busybox:1.36.1-musl /bin/busybox /busybox

EXPOSE 4566

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=6 \
    CMD ["/busybox", "wget", "-q", "--spider", "-T", "2", "http://127.0.0.1:4566/_robotocore/health"]

ENTRYPOINT ["/server/robotocore-rust", "--host", "0.0.0.0", "--port", "4566"]
