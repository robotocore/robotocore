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

# curl: health checks; ruby: Lambda Ruby runtime support
# Node.js is installed via versioned COPY below, not apt.
RUN apt-get update && apt-get install -y --no-install-recommends curl ruby \
    && rm -rf /var/lib/apt/lists/*

# Node.js runtimes — copy official versioned binaries so each nodejs* Lambda
# identifier runs on the matching Node.js version, matching AWS behavior.
# Node 20 (current LTS) is also the default "node" binary.
COPY --from=node:18-slim /usr/local/bin/node /usr/local/bin/node18
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node20
COPY --from=node:22-slim /usr/local/bin/node /usr/local/bin/node22
COPY --from=node:20-slim /usr/local/bin/node /usr/local/bin/node

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

# openjdk-21-jdk-headless: javac (bootstrap compilation) + java (Lambda execution)
# dotnet-sdk-8.0: dotnet CLI for bootstrap compilation + Lambda execution
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Install .NET SDK via Microsoft's official script (apt repo has outdated versions on slim)
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && wget -q https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh --channel 8.0 --install-dir /usr/share/dotnet \
    && ln -s /usr/share/dotnet/dotnet /usr/local/bin/dotnet \
    && rm /tmp/dotnet-install.sh \
    && apt-get remove -y wget && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /root/.dotnet /tmp/NuGetScratch

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1
ENV DOTNET_NOLOGO=1

USER robotocore

# ---- Default build target: standard image ----
# Placing this last ensures `docker build .` (without --target) produces the
# standard slim image, not the heavier java-and-dotnet stage.
FROM standard
