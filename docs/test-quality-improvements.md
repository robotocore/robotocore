# Test Quality Improvement Plan

This document outlines systematic improvements to test quality tooling and processes.

## Current State (After Phase 1)

✅ **Completed:**
- Fixed coverage script accuracy (filters Python built-ins)
- Created actionable coverage report (combines coverage + probe + quality)
- Fixed weak assertions in SQS (84.9% → 98.9% quality)
- In progress: IAM, Events, Config, CloudFormation (parallel agents)

📊 **Metrics:**
- 147 services registered
- 6,383 total tests
- ~70-90% test quality for major services (after fixes)
- 16 services with coverage gaps

## Phase 2: Automation & Tooling

### 2.1 Assertion Generator (In Progress)

**Status:** Prototype created, needs refinement

**Goal:** Auto-generate assertions from botocore specs

**Current limitation:** Doesn't follow shape references

**Next steps:**
1. Fix shape reference resolution
2. Add semantic understanding (e.g., QueueUrl should contain queue name)
3. Generate contextual assertions (e.g., verify created resource appears in list)

**Example output:**
```python
# For CreateQueue
assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
assert "QueueUrl" in response
assert isinstance(response["QueueUrl"], str)
assert len(response["QueueUrl"]) > 0
assert response["QueueUrl"].startswith("http")
assert queue_name in response["QueueUrl"]  # Semantic check
```

### 2.2 CI Quality Gates

**Add to `.github/workflows/test.yml`:**

```yaml
- name: Validate test quality
  run: |
    # Fail if >5% of tests don't contact server
    uv run python scripts/validate_test_quality.py --max-no-contact-pct 5

    # Fail if >10% of tests have no assertions
    uv run python scripts/validate_test_quality.py --max-no-assertion-pct 10
```

### 2.3 Pre-commit Hook

**Add to `.pre-commit-config.yaml`:**

```yaml
- repo: local
  hooks:
    - id: test-quality
      name: Validate test quality for changed files
      entry: scripts/validate_changed_tests.sh
      language: system
      files: 'tests/compatibility/.*\.py$'
```

### 2.4 Probe Data Caching

**Problem:** Probe requires server running, which is slow

**Solution:** Cache probe results in `logs/probe-cache/`

```python
# In probe_service.py
CACHE_DIR = Path("logs/probe-cache")
cache_file = CACHE_DIR / f"{service}-{date}.json"

if cache_file.exists() and cache_file.stat().st_mtime > cutoff:
    return json.loads(cache_file.read_text())
```

**Benefits:**
- Actionable report works without server
- Faster CI/local development
- Historical probe data for tracking regressions

## Phase 3: Enhanced Quality Detection

### 3.1 Weak Assertion Categories

Extend `validate_test_quality.py` to detect:

| Category | Example | Fix |
|----------|---------|-----|
| **key_presence_only** | `assert "QueueUrl" in resp` | Add value checks |
| **truthy_only** | `assert response` | Check specific fields |
| **unchecked_error** | `except ClientError: pass` | Assert error code |
| **no_behavioral_check** | Creates queue but doesn't verify it exists | Add list/get verification |

### 3.2 Semantic Assertions

Detect when tests miss semantic checks:

```python
# Test creates queue named "test-queue"
resp = sqs.create_queue(QueueName="test-queue")
assert "QueueUrl" in resp  # ❌ Missing: name should be in URL

# Better:
assert "test-queue" in resp["QueueUrl"]  # ✅ Semantic check
```

**Implementation:**
1. Parse test to find resource names
2. Check if assertions verify those names appear in responses
3. Flag tests that create resources but don't verify names/IDs

### 3.3 Coverage Accuracy Score

Add to `actionable_coverage_report.py`:

```python
accuracy_score = (
    verified_ops / (verified_ops + false_positives + false_negatives)
) * 100
```

**Components:**
- `verified_ops`: Operations with effective tests
- `false_positives`: "Covered" but weak tests
- `false_negatives`: Implemented but marked missing

## Phase 4: Systematic Test Generation

### 4.1 Test Pattern Library

Build reusable patterns for common scenarios:

```python
# patterns/crud.py
def create_verify_delete(client, resource_type, create_op, list_op, delete_op):
    """Standard CRUD test pattern."""
    # Create
    create_resp = getattr(client, create_op)(Name=f"test-{uuid4().hex[:6]}")
    resource_id = extract_id(create_resp, resource_type)

    try:
        # Verify in list
        list_resp = getattr(client, list_op)()
        assert any(resource_id in item for item in list_resp[f"{resource_type}s"])

        # Verify behavioral assertions
        verify_resource(create_resp, resource_type)
    finally:
        # Cleanup
        getattr(client, delete_op)(**{f"{resource_type}Id": resource_id})
```

### 4.2 Overnight Workflow Enhancement

**Current:** Agents write tests one-by-one

**Better:** Batch generation with pattern library

```bash
# scripts/batch_generate_tests.py
uv run python scripts/batch_generate_tests.py \
  --service ec2 \
  --operations CreateVpc,DeleteVpc,DescribeVpcs \
  --pattern crud \
  --output tests/compatibility/test_ec2_compat.py
```

## Phase 5: Measurement & Reporting

### 5.1 Quality Dashboard

Create `scripts/quality_dashboard.py`:

**Metrics:**
- Test quality score per service
- Coverage accuracy (effective vs nominal)
- Assertion strength distribution
- Trend over time (quality improving/degrading)

**Output:** HTML dashboard at `docs/quality/index.html`

### 5.2 Regression Detection

Track quality metrics in git:

```bash
# .github/workflows/quality-tracking.yml
- name: Track quality metrics
  run: |
    uv run python scripts/generate_quality_metrics.py > metrics/$(date +%Y%m%d).json
    git add metrics/
    git commit -m "chore: update quality metrics"
```

**Benefits:**
- Catch quality regressions in PR review
- Visualize quality trends
- Identify problematic test patterns

## Phase 6: Integration with Development Workflow

### 6.1 IDE Integration

**VSCode extension:** Show test quality inline

```json
// .vscode/settings.json
{
  "python.testing.pytestArgs": [
    "--quality-report"
  ]
}
```

**Features:**
- 🟢 Green check for effective tests
- 🟡 Yellow warning for weak assertions
- 🔴 Red X for no-server-contact tests

### 6.2 PR Quality Report

Add GitHub Action comment:

```markdown
## Test Quality Report

**Changed files:**
- `test_sqs_compat.py`: 95% effective (+12% from main)
- `test_ec2_compat.py`: 68% effective (-2% from main) ⚠️

**Summary:**
- 15 new tests added
- 3 weak assertions fixed
- Overall quality: 87% → 89% ✅
```

## Implementation Priority

1. **High Impact, Low Effort:**
   - ✅ CI quality gates (prevents regression)
   - ✅ Probe data caching (speeds up workflow)
   - ✅ Pre-commit hook (catches issues early)

2. **High Impact, Medium Effort:**
   - Assertion generator refinement
   - Weak assertion categories
   - Quality dashboard

3. **Medium Impact, High Effort:**
   - Test pattern library
   - Semantic assertion detection
   - IDE integration

## Success Criteria

**After Phase 2-3:**
- No new tests with <80% quality merged
- Coverage accuracy score >90% for all services
- CI catches weak assertions before merge

**After Phase 4-5:**
- Test generation is 80% automated
- Quality trends visible in dashboard
- Regression detection prevents quality drops

**After Phase 6:**
- Developers get inline quality feedback
- PR reviews include automated quality reports
- Quality is part of development culture, not afterthought
