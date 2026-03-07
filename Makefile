.PHONY: test test-all unit-test integration-test compat-test lint format docker-build docker-run parity-report clean start stop status smoke

N := $(shell python3 -c "import os; print(min(os.cpu_count() or 4, 12))")
DEV := uv run python scripts/dev.py

# Default: parallel unit tests (fast, no server needed)
test: unit-test

# Unit tests — parallel across $(N) cores
unit-test:
	uv run pytest tests/unit/ -n$(N) -q --tb=short

# Compat tests — auto-starts/stops server, parallel
compat-test:
	$(DEV) test-compat

# Compat tests — assumes server already running, parallel by file
compat-test-hot:
	uv run pytest tests/compatibility/ -n$(N) --dist=loadfile -q --tb=short

# Integration tests
integration-test:
	$(DEV) test-integration

# All tests: unit (parallel) + server + compat + integration
test-all:
	$(DEV) test-all

# Server lifecycle
start:
	$(DEV) server-start

stop:
	$(DEV) server-stop

status:
	$(DEV) server-status

# Smoke test (requires running server)
smoke:
	uv run python scripts/smoke_test.py

# Lint and format check
lint:
	uv run ruff check src/ tests/
	uv run ruff format --check src/ tests/

# Auto-format
format:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

# Build Docker image
docker-build:
	docker build -t robotocore .

# Run Docker container
docker-run: docker-build
	docker run -p 4566:4566 robotocore

# Run with LocalStack for comparison
docker-compare:
	docker compose --profile localstack up

# Generate parity report (auto-manages server)
parity-report:
	$(DEV) server-start
	ENDPOINT_URL=http://localhost:4566 uv run python scripts/generate_parity_report.py --output parity-report.json
	$(DEV) server-stop

# Clean build artifacts
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache dist build *.egg-info parity-report.json .robotocore.pid .robotocore.log
