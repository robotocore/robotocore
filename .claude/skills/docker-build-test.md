# Skill: Build and Test Docker Container

Use this skill when building the robotocore Docker image and running tests against it.

## Build

```bash
docker build -t robotocore .
docker image ls robotocore --format "{{.Repository}} {{.Size}}"
```

The image's runtime base is distroless python3-debian12 (no package manager,
no shell): the Rust gateway binary plus a Python 3.11 interpreter for the
in-process python-family Lambda executor, and a static busybox healthcheck.
`--target runtimes` adds Node.js/Ruby, `--target java-and-dotnet` adds
JDK 21/.NET 8.

## Run

```bash
docker rm -f robotocore-test 2>/dev/null
docker run -d --name robotocore-test -p 14567:4566 robotocore
sleep 2
curl -s http://localhost:14567/_robotocore/health
```

## Test against the container

```bash
# Run all compatibility tests against the container
ENDPOINT_URL=http://localhost:14567 uv run pytest tests/compatibility/ -q

# Full parity run against the golden baseline
make parity
```

## Clean up

```bash
docker rm -f robotocore-test
```

## Notes
- Rust multi-stage build; supports ARM and x86
- Port 4566 exposed (same as LocalStack)
- Health check at `/_robotocore/health`
- The Rust binary inside is the same one `cargo run -p robotocore-rust` builds

## Docker Desktop extension (docker-extension/)

The extension backend lives in `docker-extension/vm/` (an axum crate) and
builds separately — its context is `docker-extension/`, not the repo root:

```bash
docker build -f docker-extension/Dockerfile -t robotocore/docker-desktop-extension:local docker-extension/
docker run --rm -p 8191:8191 -e EXTENSION_PORT=8191 robotocore/docker-desktop-extension:local
```
