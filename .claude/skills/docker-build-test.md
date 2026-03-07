# Skill: Build and Test Docker Container

Use this skill when building the robotocore Docker image and running tests against it.

## Build

```bash
docker build -t robotocore .
# Image size target: < 600MB (currently ~559MB)
docker image ls robotocore --format "{{.Size}}"
```

## Run

```bash
# Run the container on a test port
docker rm -f robotocore-test 2>/dev/null
docker run -d --name robotocore-test -p 14567:4566 robotocore
sleep 5
curl -s http://localhost:14567/_robotocore/health
```

## Test against the container

```bash
# Run all compatibility tests
ENDPOINT_URL=http://localhost:14567 uv run pytest tests/compatibility/ -q

# Generate parity report
uv run python scripts/parity_report.py
```

## Clean up

```bash
docker rm -f robotocore-test
```

## Notes
- Uses `python:3.12-slim` as base, supports ARM and x86
- Port 4566 exposed (same as LocalStack)
- `MOTO_ALLOW_NONEXISTENT_REGION=true` set in image
- Health check at `/_robotocore/health`
- Don't install dev dependencies in image (`--no-dev`)
