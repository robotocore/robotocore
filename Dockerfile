# ---- Builder stage: install deps + project, then discard build artifacts ----
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

ARG HATCH_VCS_FALLBACK_VERSION=0.0.0.dev0
ENV HATCH_VCS_FALLBACK_VERSION=${HATCH_VCS_FALLBACK_VERSION}

# Copy moto source from vendor (no git history — .dockerignore excludes it)
COPY vendor/moto/ vendor/moto/

# Copy project metadata and lockfile
COPY .git/ .git/
COPY pyproject.toml uv.lock* README.md ./

# Rewrite moto source from git remote to local vendor copy (avoids ~123MB clone)
# Re-lock only moto (keeps all other packages at lockfile-pinned versions)
RUN sed -i 's|^moto = .*|moto = { path = "vendor/moto" }|' pyproject.toml \
    && uv lock --upgrade-package moto

# Install dependencies from local moto + PyPI (frozen = use exact lockfile versions)
RUN uv sync --frozen --no-dev --no-install-project \
    && uv cache clean

# Copy source and install the project itself
COPY src/ src/
RUN uv sync --frozen --no-dev

# Strip build artifacts and unused transitive deps (~90MB savings)
RUN find /app/.venv -name '.git' -type d -exec rm -rf {} + 2>/dev/null; \
    rm -rf /app/.git /app/.venv/src/*/.git /root/.cache/uv \
    /app/vendor \
    /app/.venv/lib/python3.12/site-packages/cfnlint \
    /app/.venv/lib/python3.12/site-packages/cfn_lint*.dist-info \
    /app/.venv/lib/python3.12/site-packages/sympy \
    /app/.venv/lib/python3.12/site-packages/sympy*.dist-info \
    /app/.venv/lib/python3.12/site-packages/networkx \
    /app/.venv/lib/python3.12/site-packages/networkx*.dist-info \
    /app/.venv/lib/python3.12/site-packages/mpmath \
    /app/.venv/lib/python3.12/site-packages/mpmath*.dist-info \
    /app/.venv/lib/python3.12/site-packages/setuptools \
    /app/.venv/lib/python3.12/site-packages/setuptools*.dist-info

# ---- Runtime stage: slim image with Python + Node.js + Ruby ----
FROM python:3.12-slim AS standard

# curl: health checks; libffi8/libyaml-0-2/libssl3/zlib1g: shared libs Ruby
# binaries link against (the ruby:X-slim images we copy below were built for
# the same Debian base, so these are the libraries their /usr/local/bin/ruby
# expects to load via the dynamic linker).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl libffi8 libyaml-0-2 libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Node.js runtimes — copy official versioned binaries so each nodejs* Lambda
# identifier runs on the matching Node.js version, matching AWS behavior.
# Node 20 (current LTS) is also the default "node" binary.
COPY --from=node:18-slim /usr/local/bin/node /usr/local/bin/node18
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node20
COPY --from=node:22-slim /usr/local/bin/node /usr/local/bin/node22
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node

# Ruby per-version installs. Unlike Node, Ruby binaries are dynamically linked
# (libruby.so + native gem extensions live alongside the binary), so a
# single-file COPY doesn't work — we copy the entire /usr/local prefix from
# each official ruby:X-slim image into a versioned root, then ship a small
# wrapper script per major.minor that sets LD_LIBRARY_PATH and GEM_PATH before
# exec-ing the right binary.
COPY --from=ruby:3.2-slim /usr/local /opt/ruby-3.2
COPY --from=ruby:3.3-slim /usr/local /opt/ruby-3.3
COPY --from=ruby:3.4-slim /usr/local /opt/ruby-3.4

# Wrapper scripts: ruby3.2/ruby3.3/ruby3.4 each exec their own interpreter
# with its own shared-library and gem-path set. The default `ruby` and `gem`
# point at 3.4 (latest stable) for paths that don't know the requested runtime
# (e.g. the bootstrap.rb fallback).
RUN for v in 3.2 3.3 3.4; do \
      printf '#!/bin/sh\nexport LD_LIBRARY_PATH="/opt/ruby-%s/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\nexport GEM_PATH="/opt/ruby-%s/lib/ruby/gems/%s.0${GEM_PATH:+:$GEM_PATH}"\nexec /opt/ruby-%s/bin/ruby "$@"\n' "$v" "$v" "$v" "$v" > /usr/local/bin/ruby"$v" && \
      chmod 755 /usr/local/bin/ruby"$v"; \
    done && \
    ln -sf /usr/local/bin/ruby3.4 /usr/local/bin/ruby && \
    ln -sf /opt/ruby-3.4/bin/gem  /usr/local/bin/gem

# Python per-version installs. Same shape as Ruby: libpythonX.Y.so and the
# stdlib live alongside the binary, so single-file COPY doesn't work. We pull
# each python:X.Y-slim image's /usr/local into a versioned prefix and write
# a wrapper that sets LD_LIBRARY_PATH before exec-ing. PythonExecutor uses
# the matching wrapper when the requested runtime differs from the in-process
# Python (so the in-process fast path stays available for the host version).
#
# 3.12 is intentionally NOT copied: it's the image's base Python (the builder
# stage is python:3.12-slim) and providing the in-process path is the whole
# point of "host Python is python3.12".
COPY --from=python:3.10-slim /usr/local /opt/python-3.10
COPY --from=python:3.11-slim /usr/local /opt/python-3.11
COPY --from=python:3.13-slim /usr/local /opt/python-3.13

RUN for v in 3.10 3.11 3.13; do \
      printf '#!/bin/sh\nexport LD_LIBRARY_PATH="/opt/python-%s/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\nexec /opt/python-%s/bin/python%s "$@"\n' "$v" "$v" "$v" > /usr/local/bin/python"$v" && \
      chmod 755 /usr/local/bin/python"$v"; \
    done

RUN groupadd -r robotocore && useradd -r -g robotocore -d /app robotocore

WORKDIR /app

# Copy the fully-built venv from the builder (no git, no uv, no caches)
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/pyproject.toml /app/

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create init hook directories
RUN mkdir -p /etc/robotocore/init/boot.d \
    /etc/robotocore/init/ready.d \
    /etc/robotocore/init/shutdown.d \
    /etc/robotocore/extensions \
    /tmp/robotocore/state

RUN chown -R robotocore:robotocore /app /tmp/robotocore /etc/robotocore

EXPOSE 4566

ARG HATCH_VCS_FALLBACK_VERSION=0.0.0.dev0
ENV ROBOTOCORE_HOST=0.0.0.0
ENV ROBOTOCORE_PORT=4566
ENV MOTO_ALLOW_NONEXISTENT_REGION=true
ENV ROBOTOCORE_VERSION=${HATCH_VCS_FALLBACK_VERSION}

USER robotocore

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:4566/_localstack/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]

# ---- java-and-dotnet stage: standard + JDK + .NET SDK ----
# Build with: docker build --target java-and-dotnet -t robotocore:java-and-dotnet .
FROM standard AS java-and-dotnet

USER root

# Java per-version installs from Eclipse Temurin official Docker images.
# Each Temurin image ships a self-contained JDK under /opt/java/openjdk;
# COPY-ing the whole tree per major version gives us real isolation
# (java8 sees Java 8 class-format rules, removed APIs, etc., rather than
# silently running on JVM 21). Mirrors the Node pattern from `standard`.
#
# Disk cost: each Temurin JDK is ~250-350MB. Four of them sits at ~1.2GB
# added to this stage; this is the "heavy runtimes" image, so the size is
# the trade-off for true version fidelity.
COPY --from=eclipse-temurin:8-jdk /opt/java/openjdk /opt/java/jdk-8
COPY --from=eclipse-temurin:11-jdk /opt/java/openjdk /opt/java/jdk-11
COPY --from=eclipse-temurin:17-jdk /opt/java/openjdk /opt/java/jdk-17
COPY --from=eclipse-temurin:21-jdk /opt/java/openjdk /opt/java/jdk-21

# Per-version wrapper scripts. `_RUNTIME_BINARY` in java.py maps:
#   java8     → java8        java11 → java11
#   java8.al2 → java8        java17 → java17
#                              java21 → java21
# Default `java` and `javac` point at 21 (latest LTS) for the bootstrap
# compilation path that doesn't know the user's requested runtime.
RUN for v in 8 11 17 21; do \
      printf '#!/bin/sh\nexec /opt/java/jdk-%s/bin/java "$@"\n' "$v" > /usr/local/bin/java"$v" && \
      chmod 755 /usr/local/bin/java"$v"; \
    done && \
    ln -sf /opt/java/jdk-21/bin/java  /usr/local/bin/java && \
    ln -sf /opt/java/jdk-21/bin/javac /usr/local/bin/javac

# .NET SDKs for every Lambda-supported runtime version. dotnet-install.sh
# accepts multiple --channel invocations that share an install prefix; the
# single `dotnet` host then dispatches builds and runs to the requested TFM
# (selectable via `<TargetFramework>` in the .csproj, which is exactly how
# DotnetExecutor picks per-runtime via `_detect_tfm(runtime)`).
#
# Three SDKs (not runtimes) so `dotnet build -f netX.0` works offline for any
# Lambda runtime ID: dotnet6 → net6.0, dotnet8 → net8.0, dotnet9 → net9.0.
# Runtime-only installs (`--runtime dotnet`) were the wrong fix earlier —
# they're enough to *run* an existing DLL but not to build a bootstrap that
# references one, which is what _run_with_bootstrap() does.
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && wget -q https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh --channel 6.0 --install-dir /usr/share/dotnet \
    && /tmp/dotnet-install.sh --channel 8.0 --install-dir /usr/share/dotnet \
    && /tmp/dotnet-install.sh --channel 9.0 --install-dir /usr/share/dotnet \
    && ln -s /usr/share/dotnet/dotnet /usr/local/bin/dotnet \
    && rm /tmp/dotnet-install.sh \
    && apt-get remove -y wget && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.dotnet /tmp/NuGetScratch

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1
ENV DOTNET_NOLOGO=1
ENV DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
# Invariant globalization: avoids the libicu dependency on slim Debian images.
# AWS Lambda's own .NET runtime uses this setting; locale-sensitive operations
# (e.g. culture-specific string comparison) fall back to ordinal behavior.
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
# Shared NuGet package cache writable by all users (primed during build)
ENV NUGET_PACKAGES=/opt/nuget-packages

# Pre-warm the NuGet package cache for every supported TFM so the first
# `dotnet build -f netX.0` invocation against a fresh container works offline.
# dotnet restore (not build) is sufficient: it downloads SDK reference packs
# and NuGet dependencies without invoking the compiler, using far less memory.
RUN mkdir -p /opt/nuget-packages /tmp/dotnet-prewarm \
    && for tfm in net6.0 net8.0 net9.0; do \
         mkdir -p /tmp/dotnet-prewarm/$tfm && \
         printf '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>%s</TargetFramework></PropertyGroup></Project>' "$tfm" \
           > /tmp/dotnet-prewarm/$tfm/prewarm.csproj && \
         dotnet restore /tmp/dotnet-prewarm/$tfm/prewarm.csproj --nologo; \
       done \
    && rm -rf /tmp/dotnet-prewarm \
    && chmod -R a+rX /opt/nuget-packages

USER robotocore

# ---- Default build target: standard image ----
# Placing this last ensures `docker build .` (without --target) produces the
# standard slim image, not the heavier java-and-dotnet stage.
FROM standard
