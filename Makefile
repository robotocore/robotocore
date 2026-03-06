.PHONY: test test-all unit-test integration-test compat-test lint format docker-build docker-run parity-report clean

# Run all tests (unit + integration, no server needed)
test:
	uv run pytest tests/unit/ tests/integration/ -v

# Run unit tests
unit-test:
	uv run pytest tests/unit/ -v

# Run integration tests
integration-test:
	uv run pytest tests/integration/ -v

# Run all tests (unit + integration, no server needed)
test-all:
	uv run pytest tests/unit/ tests/integration/ -v

# Run compatibility tests (requires running server on port 4566)
compat-test:
	ENDPOINT_URL=http://localhost:4566 uv run pytest tests/compatibility/ -v

# Start server and run compatibility tests
compat-test-full:
	@echo "Starting robotocore..."
	@ROBOTOCORE_PORT=4566 uv run python -m robotocore.main & \
	SERVER_PID=$$!; \
	sleep 2; \
	ENDPOINT_URL=http://localhost:4566 uv run pytest tests/compatibility/ -v; \
	EXIT_CODE=$$?; \
	kill $$SERVER_PID 2>/dev/null; \
	exit $$EXIT_CODE

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

# Generate parity report (requires running server)
parity-report:
	@echo "Starting robotocore..."
	@ROBOTOCORE_PORT=4566 uv run python -m robotocore.main & \
	SERVER_PID=$$!; \
	sleep 2; \
	ENDPOINT_URL=http://localhost:4566 uv run python scripts/generate_parity_report.py --output parity-report.json; \
	kill $$SERVER_PID 2>/dev/null

# Clean build artifacts
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache dist build *.egg-info parity-report.json
