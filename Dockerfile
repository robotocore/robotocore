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

# curl: health checks; libffi8/libyaml-0-2/libssl3/zlib1g: shared libs the
# fault-in Ruby installs link against (we ship just the *default* Ruby and
# Node baked in; every other version is fetched on first use by the fault-in
# installer — see runtimes/install.py and runtimes/install_ruby.py).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl libffi8 libyaml-0-2 libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Default Node.js: just Node 20 (current LTS) baked in as both `node` and
# `node20`. Other Lambda Node runtimes (nodejs18.x, nodejs22.x) fault in on
# first invocation via runtimes/install_node.py. The static Node binary is
# tiny (~80MB) so we keep one baked for fast cold-start on the most common
# runtime; users on 18 or 22 pay ~30s on their first invocation.
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node20
RUN ln -sf /usr/local/bin/node20 /usr/local/bin/node

# Default Ruby: just 3.4 (latest stable) baked in. ruby3.2 and ruby3.3
# fault in via the Docker Registry pull in runtimes/install_ruby.py. The
# same wrapper shape (RUBYLIB-based stdlib relocation) is used for both
# baked-in and fault-in installs.
COPY --from=ruby:3.4-slim /usr/local /opt/ruby-3.4
RUN <<'SHELL'
set -e
cat > /usr/local/bin/ruby3.4 <<'WRAPPER'
#!/bin/sh
PREFIX=/opt/ruby-3.4
VER=3.4.0
export LD_LIBRARY_PATH="$PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
RUBYLIB_ADD="$PREFIX/lib/ruby/$VER:$PREFIX/lib/ruby/site_ruby/$VER:$PREFIX/lib/ruby/vendor_ruby/$VER"
for arch_dir in "$PREFIX/lib/ruby/$VER"/*-linux*; do
  [ -d "$arch_dir" ] && RUBYLIB_ADD="$RUBYLIB_ADD:$arch_dir"
done
export RUBYLIB="$RUBYLIB_ADD${RUBYLIB:+:$RUBYLIB}"
export GEM_PATH="$PREFIX/lib/ruby/gems/$VER${GEM_PATH:+:$GEM_PATH}"
exec "$PREFIX/bin/ruby" "$@"
WRAPPER
chmod 755 /usr/local/bin/ruby3.4
ln -sf /usr/local/bin/ruby3.4 /usr/local/bin/ruby
ln -sf /opt/ruby-3.4/bin/gem  /usr/local/bin/gem
SHELL

# Python: the host image is already python:3.12-slim, so python3.12 is
# satisfied in-process by PythonExecutor with zero subprocess cost.
# python3.10/3.11/3.13 fault in on demand via python-build-standalone
# (runtimes/install_python.py).

# Fault-in runtime cache + wrapper dir. Wrappers go on $PATH ahead of
# /usr/local/bin so a fault-in `ruby3.3` shadows the default `ruby` for
# that runtime ID's invocations. Owned by the unprivileged robotocore
# user so installs don't need root.
RUN mkdir -p /var/lib/robotocore/runtimes /var/lib/robotocore/bin
ENV PATH="/var/lib/robotocore/bin:${PATH}"

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

RUN chown -R robotocore:robotocore /app /tmp/robotocore /etc/robotocore /var/lib/robotocore

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

# Default Java: just Temurin JDK 21 baked in. java8/8.al2/11/17 fault in via
# Adoptium (runtimes/install_java.py). We keep the full JDK (not JRE) for 21
# because robotocore's runtime bootstrap currently uses `javac` to compile
# Bootstrap.java on first Lambda invoke; switching the bootstrap to a
# pre-compiled .class file would let this be JRE-only (a ~170MB further
# saving, follow-up).
COPY --from=eclipse-temurin:21-jdk /opt/java/openjdk /opt/java/jdk-21
RUN printf '#!/bin/sh\nexec /opt/java/jdk-21/bin/java "$@"\n' > /usr/local/bin/java21 \
    && chmod 755 /usr/local/bin/java21 \
    && ln -sf /opt/java/jdk-21/bin/java  /usr/local/bin/java \
    && ln -sf /opt/java/jdk-21/bin/javac /usr/local/bin/javac

# Default .NET: SDK 9.0 (latest GA) baked into the SAME DOTNET_ROOT that
# the fault-in installer (install_dotnet.py) writes to, so dotnet6 and
# dotnet8 installs on first invocation are visible to the existing host
# without any additional wiring. The dotnet host only walks ONE root for
# SDKs/runtimes, so unifying the baked + fault-in location is critical;
# splitting them (e.g. /usr/share/dotnet for baked, /var/lib/... for
# fault-in) makes the faulted-in SDKs invisible to the host.
ENV DOTNET_ROOT=/var/lib/robotocore/runtimes/dotnet
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && wget -q https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh --channel 9.0 --install-dir "$DOTNET_ROOT" \
    && ln -sf "$DOTNET_ROOT/dotnet" /usr/local/bin/dotnet \
    && rm /tmp/dotnet-install.sh \
    && apt-get remove -y wget && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.dotnet /tmp/NuGetScratch \
    && chown -R robotocore:robotocore "$DOTNET_ROOT"

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1
ENV DOTNET_NOLOGO=1
ENV DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
# Invariant globalization: avoids the libicu dependency on slim Debian images.
# AWS Lambda's own .NET runtime uses this setting; locale-sensitive operations
# (e.g. culture-specific string comparison) fall back to ordinal behavior.
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1
# Shared NuGet package cache writable by all users (primed during build)
ENV NUGET_PACKAGES=/opt/nuget-packages

# Pre-warm the NuGet package cache for the default TFM only (net9.0).
# Fault-in dotnet6/dotnet8 installs do their own NuGet restore on first
# Lambda build using the same shared cache dir.
RUN mkdir -p /opt/nuget-packages /tmp/dotnet-prewarm \
    && printf '<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net9.0</TargetFramework></PropertyGroup></Project>' \
       > /tmp/dotnet-prewarm/prewarm.csproj \
    && dotnet restore /tmp/dotnet-prewarm/prewarm.csproj --nologo \
    && rm -rf /tmp/dotnet-prewarm \
    && chmod -R a+rX /opt/nuget-packages

USER robotocore

# ---- Default build target: standard image ----
# Placing this last ensures `docker build .` (without --target) produces the
# standard slim image, not the heavier java-and-dotnet stage.
FROM standard
