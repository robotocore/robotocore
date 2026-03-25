.PHONY: test test-all unit-test integration-test compat-test compat-test-hot lint format \
        docker-build docker-run docker-compare parity-report gap-analysis clean \
        start stop status smoke help test-quality validate-tests lint-project \
        test-iac test-iac-terraform test-iac-cloudformation test-iac-cdk \
        test-iac-pulumi test-iac-serverless test-iac-sam release \
        s3-semantic-audit s3-connectivity-matrix \
        coverage pre-commit-install pre-commit \
        shape-check error-check \
        catalog semantic-check audit

N := $(shell python3 -c "import os; print(min(os.cpu_count() or 4, 12))")
DEV := uv run python scripts/dev.py

# Default: parallel unit tests (fast, no server needed)
test: unit-test ## Run unit tests (default)

## ── Testing ──────────────────────────────────────────────────────────────────

unit-test: ## Run unit tests in parallel (no server needed)
	uv sync --all-extras
	uv run pytest tests/unit/ -n$(N) -q --tb=short

coverage: ## Run unit tests with coverage report
	uv sync --all-extras
	uv run pytest tests/unit/ -n$(N) -q --tb=short --cov=src/robotocore --cov-report=term-missing --cov-report=html:htmlcov

compat-test: ## Run compatibility tests (auto-starts/stops server)
	$(DEV) test-compat

compat-test-hot: ## Run compatibility tests (assumes server already running)
	uv sync --all-extras
	uv run pytest tests/compatibility/ -n$(N) --dist=loadfile -q --tb=short

integration-test: ## Run integration tests (auto-manages server)
	$(DEV) test-integration

test-all: ## Run all tests: unit + compat + integration
	$(DEV) test-all

## ── IaC tests ────────────────────────────────────────────────────────────────

test-iac: ## Run all IaC tests (requires running server + tool binaries)
	uv sync --all-extras
	uv run pytest tests/iac/ -q --tb=short

test-iac-terraform: ## Run Terraform IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/terraform/ -q --tb=short

test-iac-cloudformation: ## Run CloudFormation IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/cloudformation/ -q --tb=short

test-iac-cdk: ## Run CDK IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/cdk/ -q --tb=short

test-iac-pulumi: ## Run Pulumi IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/pulumi/ -q --tb=short

test-iac-serverless: ## Run Serverless Framework IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/serverless/ -q --tb=short

test-iac-sam: ## Run SAM IaC tests
	uv sync --all-extras
	uv run pytest tests/iac/sam/ -q --tb=short

## ── Server ───────────────────────────────────────────────────────────────────

start: ## Start dev server in background (port 4566)
	$(DEV) server-start

stop: ## Stop dev server
	$(DEV) server-stop

status: ## Show dev server status
	$(DEV) server-status

smoke: ## Run smoke tests (requires running server)
	uv run python scripts/smoke_test.py

## ── Code quality ─────────────────────────────────────────────────────────────

lint: ## Check code: ruff, mypy, bandit (matches pre-commit + CI)
	uv run ruff check src/ tests/ scripts/
	uv run ruff format --check src/ tests/ scripts/
	uv run mypy src/robotocore/ --ignore-missing-imports
	uv run bandit -r src/robotocore/ -ll -c pyproject.toml -q

format: ## Auto-format and fix code with ruff
	uv run ruff format src/ tests/ scripts/
	uv run ruff check --fix src/ tests/ scripts/

test-quality: ## Static analysis: report compat test quality (no server needed)
	uv run python scripts/validate_test_quality.py

validate-tests: ## Runtime check: verify tests actually contact server (requires make start)
	uv run python scripts/validate_tests_runtime.py --all --sample 10

lint-project: ## Structural lint: registry sync, test quality, protocol drift (no server needed)
	uv run python scripts/lint_project.py --fail

pre-commit-install: ## Install pre-commit hooks into local git repo
	uv run pre-commit install

pre-commit: ## Run pre-commit checks on all files
	uv run pre-commit run --all-files

## ── Shape & contract validation ──────────────────────────────────────────

shape-check: ## Validate response shapes against botocore (requires running server)
	uv run python scripts/validate_response_shapes.py --top 20 --no-optional

error-check: ## Validate error response contracts (requires running server)
	uv run python scripts/validate_error_contracts.py --top 20

## ── Gap analysis ─────────────────────────────────────────────────────────────

gap-analysis: ## Full gap analysis: robotocore vs LocalStack AND vs 100% botocore
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Gap 1: Robotocore vs LocalStack — tier-by-tier comparison"
	@echo "══════════════════════════════════════════════════════════════════"
	uv run python scripts/analyze_localstack.py --tier-analysis
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Gap 2: Robotocore vs LocalStack — community operation gaps"
	@echo "══════════════════════════════════════════════════════════════════"
	uv run python scripts/analyze_localstack.py --robotocore-gap
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Gap 3: Robotocore vs 100% botocore AWS operation coverage"
	@echo "══════════════════════════════════════════════════════════════════"
	uv run python scripts/generate_parity_report.py

batch-probe: ## Batch-probe all gap services against running server (requires make start)
	uv run python scripts/batch_probe_gap.py --all

parity-report: ## Generate full parity report to parity-report.json (auto-manages server)
	$(DEV) server-start
	ENDPOINT_URL=http://localhost:4566 uv run python scripts/generate_parity_report.py --output parity-report.json
	$(DEV) server-stop

s3-semantic-audit: ## Generate the S3 feature-level semantic audit report
	uv run python scripts/s3_semantic_audit.py

s3-connectivity-matrix: s3-semantic-audit ## Regenerate the S3 connectivity matrix
	@echo "Wrote docs/s3-connectivity-matrix.md"

## ── Operation catalog & semantic checks ─────────────────────────────────────

catalog: ## Build per-operation truth table (data/operation_catalog.json)
	uv run python scripts/build_operation_catalog.py --json > data/operation_catalog.json

semantic-check: ## Validate test assertions against botocore shapes (no server needed)
	uv run python scripts/validate_test_semantics.py

audit: test-quality semantic-check catalog ## Full audit: test quality + semantics + catalog
	@echo ""
	@echo "══════════════════════════════════════════════════════════════════"
	@echo "  Operation Catalog Summary"
	@echo "══════════════════════════════════════════════════════════════════"
	uv run python scripts/build_operation_catalog.py --summary

## ── Docker ───────────────────────────────────────────────────────────────────

docker-build: ## Build Docker image
	docker build -t robotocore .

docker-run: docker-build ## Build and run Docker container on port 4566
	docker run -p 4566:4566 robotocore

docker-compare: ## Run robotocore + LocalStack side-by-side for comparison
	docker compose --profile localstack up

## ── Release ─────────────────────────────────────────────────────────────────

release: ## Tag and push a CalVer release (auto-publishes Docker images via CI)
	@VERSION=$$(date -u +%Y.%-m.%-d) && \
	EXISTING=$$(git tag -l "v$$VERSION" "v$$VERSION.*" | wc -l | tr -d ' ') && \
	if [ "$$EXISTING" -gt 0 ]; then VERSION="$$VERSION.$$EXISTING"; fi && \
	echo "Releasing v$$VERSION" && \
	git tag "v$$VERSION" && \
	git push origin "v$$VERSION"

## ── Misc ─────────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts, caches, and temp files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache dist build *.egg-info parity-report.json .robotocore.pid .robotocore.log .robotocore-diag.log

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} \
	    /^## ── / { printf "\n\033[1m%s\033[0m\n", substr($$0, 4) } \
	    /^[a-zA-Z_-]+:.*## / { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' \
	    $(MAKEFILE_LIST)
	@echo ""
