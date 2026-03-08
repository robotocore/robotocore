# Skill: /lint — Run all local checks before pushing

Run the full local check suite to catch what CI would catch, before pushing.

## Steps

Run these checks in order, stopping at the first failure:

### 1. Ruff lint + format
```bash
uv run ruff check src/ tests/ scripts/
uv run ruff format --check src/ tests/ scripts/
```
If either fails, run `make format` to auto-fix, then re-check.

### 2. Structural lint
```bash
uv run python scripts/lint_project.py --fail
```
Reports registry sync, provider import, and test quality structural issues.
Exit 0 means no **errors** (warnings are informational only).

### 3. Unit tests
```bash
uv run pytest tests/unit/ -n4 -q --tb=short
```

### 4. Test quality (optional — only if compat tests were changed)
```bash
uv run python scripts/validate_test_quality.py --max-no-contact-pct 5 --max-no-assertion-pct 6
```

## Quick fix commands

| Problem | Fix |
|---------|-----|
| Ruff lint errors | `uv run ruff check --fix src/ tests/ scripts/` |
| Ruff format errors | `uv run ruff format src/ tests/ scripts/` |
| Structural errors | Fix the specific issue listed by `lint_project.py --fail` |
| Unit test failures | Investigate with `uv run pytest <failing-test> -v --tb=long` |

## What CI runs

The CI pipeline runs:
- `lint` — ruff check + format
- `test-quality` — validate_test_quality.py + lint_project.py
- `unit-gateway`, `unit-services`, `unit-infra` — sharded unit tests
- `integration-tests` — needs running server (make start first)
- `compatibility-tests` — needs running server (make start first)
- `docker-build` — Docker image build + smoke tests
- `parity-report` — operation coverage report (artifact upload)

Run `make start` before running integration/compat tests locally.
