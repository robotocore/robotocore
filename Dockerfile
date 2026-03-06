FROM python:3.12-slim AS base

# Supports both ARM and x86
# docker build -t robotocore .

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first for layer caching
COPY pyproject.toml uv.lock* README.md ./
RUN uv sync --no-dev --no-install-project

# Copy source
COPY src/ src/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Install the project itself
RUN uv sync --no-dev

EXPOSE 4566

ENV ROBOTOCORE_HOST=0.0.0.0
ENV ROBOTOCORE_PORT=4566
ENV MOTO_ALLOW_NONEXISTENT_REGION=true

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:4566/_robotocore/health || exit 1

ENTRYPOINT ["/entrypoint.sh"]
