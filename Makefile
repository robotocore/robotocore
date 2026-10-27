.PHONY: test compat-test compat-test-hot parity rust-test rust-lint rust-fmt \
        docker-build docker-run serve clean help start stop status \
        test-iac test-iac-terraform test-iac-cloudformation test-iac-cdk \
        test-iac-pulumi test-iac-serverless test-iac-sam help coverage-gate \
        absence-manifest

N := $(shell python3 -c "import os; print(min(os.cpu_count() or 4, 12))")

# Default: compatibility tests (they need the running rust server)
test: compat-test ## Run compatibility suites (starts the rust server)

## ── Rust gates ───────────────────────────────────────────────────────────

rust-test: ## Run all Rust unit tests
	cargo test --workspace

rust-lint: ## Clippy (CI gate)
	cargo clippy --workspace --all-targets -- -D warnings

rust-fmt: ## rustfmt check
	cargo fmt --all --check

## ── Testing ────────────────────────────────────────────────────────────

compat-test: ## Compatibility tests (auto-starts the rust server)
	bash scripts/start_parity_servers.sh
	uv run pytest tests/compatibility/ -n$(N) --dist=loadfile -q --tb=short

compat-test-hot: ## Compatibility tests (assumes server already running)
	uv run pytest tests/compatibility/ -n$(N) --dist=loadfile -q --tb=short

parity: ## Full rust-only parity run against the golden baseline
	$(MAKE) probe-freshness
	uv run python scripts/harness/parity.py --update-expectations
	$(MAKE) coverage-gate

coverage-gate: ## Regenerate coverage artifacts and fail on budget regressions
	uv run python scripts/coverage.py --gate

probe-freshness: ## Fail when a probe corpus lags its implementation merge
	uv run python scripts/probe_freshness.py

error-census: ## Live wire census: every recorded deny must answer 501 NotImplemented
	uv run python scripts/error_census.py --all

absence-manifest: ## Rebuild tooling/surface-absence.json (every deny is a recorded decision)
	uv run python scripts/build_surface_absence.py

## ── IaC tests ────────────────────────────────────────────────────────────

test-iac: ## Run all IaC tests (requires running server + tool binaries)
	uv run pytest tests/iac/ -q --tb=short

test-iac-terraform: ## Terraform IaC tests
	uv run pytest tests/iac/terraform/ -q --tb=short

test-iac-cloudformation: ## CloudFormation IaC tests
	uv run pytest tests/iac/cloudformation/ -q --tb=short

test-iac-cdk: ## CDK IaC tests
	uv run pytest tests/iac/cdk/ -q --tb=short

test-iac-pulumi: ## Pulumi IaC tests
	uv run pytest tests/iac/pulumi/ -q --tb=short

test-iac-serverless: ## Serverless Framework IaC tests
	uv run pytest tests/iac/serverless/ -q --tb=short

test-iac-sam: ## SAM IaC tests
	uv run pytest tests/iac/sam/ -q --tb=short

## ── Server ────────────────────────────────────────────────────────────

start: ## Start the rust server (rotocore native)
	bash scripts/start_parity_servers.sh

stop: ## Stop the rust server
	-kill $$(cat /tmp/parity_rust.pid 2>/dev/null) 2>/dev/null

status: ## Show server status
	curl -sf http://127.0.0.1:4566/_robotocore/health | python3 -m json.tool

## ── Docker ────────────────────────────────────────────────────────────

docker-build: ## Build the pure-Rust image
	docker build -t robotocore .

docker-run: ## Run the pure-Rust container on port 4566
	docker run -p 4566:4566 robotocore

## ── Cleanup ────────────────────────────────────────────────────────────

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache .robotocore.pid .robotocore.log
	rm -f /tmp/$${LOG_PREFIX:-parity}_rust.pid /tmp/$${LOG_PREFIX:-parity}_rust.log

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} \
	    /^## ── / { printf "\n\033[1m%s\033[0m\n", substr($$0, 4) } \
	    /^[a-zA-Z_-]+:.*## / { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' \
	    $(MAKEFILE_LIST)
	@echo ""
