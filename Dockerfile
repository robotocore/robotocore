FROM python:3.12-slim AS base

# Multi-arch support (ARM64 + AMD64)
# docker buildx build --platform linux/arm64,linux/amd64 -t robotocore .

# Security: create non-root user
RUN groupadd -r robotocore && useradd -r -g robotocore -d /app robotocore

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Version set by CI from CalVer git tag
ARG SETUPTOOLS_SCM_PRETEND_VERSION=dev

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --no-dev --no-install-project

# Copy source
COPY src/ src/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Install the project itself (SETUPTOOLS_SCM_PRETEND_VERSION tells hatch-vcs the version)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}
RUN uv sync --no-dev

# Create init hook directories
RUN mkdir -p /etc/robotocore/init/boot.d \
    /etc/robotocore/init/ready.d \
    /etc/robotocore/init/shutdown.d \
    /etc/robotocore/extensions \
    /tmp/robotocore/state

# Set ownership
RUN chown -R robotocore:robotocore /app /tmp/robotocore /etc/robotocore

EXPOSE 4566

ENV ROBOTOCORE_HOST=0.0.0.0
ENV ROBOTOCORE_PORT=4566
ENV MOTO_ALLOW_NONEXISTENT_REGION=true
ENV ROBOTOCORE_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

# Run as non-root user
USER robotocore

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:4566/_robotocore/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]
