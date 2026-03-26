# Test Quality: Next Level Improvements

Current state: We measure *if* operations are called and verify response structure.
Next level: Measure *how well* behavior is tested and automatically generate comprehensive tests.

## 1. Semantic Assertion Analysis (Highest Impact)

### Problem
Current assertions check structure but miss business logic:

```python
# Current "strong" assertion
response = sqs.create_queue(QueueName="test-dlq")
assert "QueueUrl" in response
assert "test-dlq" in response["QueueUrl"]  # ✅ Name appears

# Missing semantic checks
# - Can we actually send messages to it?
# - Does it appear in list_queues()?
# - Can we delete it?
# - Are default attributes correct?
```

### Solution: Behavioral Coverage Tracking

**Tool:** `scripts/behavioral_coverage.py`

Analyze tests to detect behavioral patterns:

| Pattern | Example | Coverage Level |
|---------|---------|----------------|
| **Existence** | Creates resource | 20% - Basic |
| **Retrieval** | Gets resource by ID | 40% - Moderate |
| **Listing** | Resource appears in list | 60% - Good |
| **Modification** | Updates work correctly | 80% - Very Good |
| **Deletion** | Resource removed | 90% - Excellent |
| **Error Handling** | Invalid inputs rejected | 100% - Complete |

**Implementation:**

```python
def analyze_behavioral_coverage(test_function: ast.FunctionDef) -> dict:
    """Detect which behavioral patterns a test covers."""
    behaviors = {
        'creates': False,
        'retrieves': False,
        'lists': False,
        'updates': False,
        'deletes': False,
        'error_handling': False
    }

    for node in ast.walk(test_function):
        if isinstance(node, ast.Call):
            method = get_method_name(node)

            if method.startswith('Create'):
                behaviors['creates'] = True
            elif method.startswith(('Get', 'Describe')):
                behaviors['retrieves'] = True
            elif method.startswith('List'):
                behaviors['lists'] = True
            # ... etc

    # Score based on behaviors covered
    coverage_score = sum(behaviors.values()) / len(behaviors) * 100
    return {'behaviors': behaviors, 'score': coverage_score}
```

**Output:**

```
EC2 CreateVpc behavioral coverage:
  ✅ Creates VPC
  ✅ Retrieves VPC by ID
  ❌ VPC appears in DescribeVpcs (missing)
  ❌ Can modify VPC attributes (missing)
  ✅ Deletes VPC
  ❌ Rejects invalid CIDR (missing)

  Behavioral score: 50% (3/6 patterns)
  Suggestion: Add DescribeVpcs check and invalid CIDR test
```

---

## 2. Property-Based Testing Integration

### Problem
We write specific test cases but miss edge cases:

```python
def test_create_queue():
    response = sqs.create_queue(QueueName="test")  # Only tests ASCII names
    # What about: Unicode? Max length? Special chars? Empty string?
```

### Solution: Hypothesis Integration

**Tool:** `scripts/generate_property_tests.py`

```python
from hypothesis import given, strategies as st
import pytest

@given(queue_name=st.text(
    alphabet=st.characters(blacklist_characters=['\n', '\r']),
    min_size=1,
    max_size=80
))
def test_create_queue_property(sqs, queue_name):
    """Property: Any valid queue name should create successfully."""
    try:
        response = sqs.create_queue(QueueName=queue_name)
        assert "QueueUrl" in response
        assert queue_name in response["QueueUrl"]

        # Cleanup
        sqs.delete_queue(QueueUrl=response["QueueUrl"])
    except ClientError as e:
        # Should only fail on invalid characters, not crash
        assert e.response["Error"]["Code"] in [
            "InvalidParameterValue",
            "ValidationException"
        ]
```

**Benefits:**
- Automatically tests 100s of cases
- Finds edge cases humans miss
- Documents valid input ranges

---

## 3. Mutation Testing (Verify Tests Actually Work)

### Problem
A test can pass even if it doesn't catch bugs:

```python
def test_delete_queue(sqs, queue_url):
    sqs.delete_queue(QueueUrl=queue_url)
    # This test passes even if delete_queue does nothing!
```

### Solution: Mutation Testing with `mutmut`

**Workflow:**
1. Mutate the code (change `==` to `!=`, remove lines, etc.)
2. Run tests
3. Tests should FAIL on mutations
4. If tests still pass → test is weak

**Implementation:**

```yaml
# .github/workflows/mutation-testing.yml
- name: Mutation testing on changed providers
  run: |
    # Only test files changed in PR
    FILES=$(git diff --name-only origin/main | grep 'src/robotocore/services')

    for file in $FILES; do
      mutmut run --paths-to-mutate=$file
      mutmut results
    done
```

**Output:**

```
Mutation testing results for src/robotocore/services/sqs/provider.py:

✅ 45/50 mutations killed (90%)
❌ 5 mutations survived:

1. Line 123: Changed `status_code = 200` to `status_code = 201`
   → Tests still passed! No test verifies status code.

2. Line 145: Removed queue name validation
   → Tests still passed! No test verifies invalid names rejected.

Recommendation: Add tests for these scenarios.
```

---

## 4. Comparison Testing Against Real AWS

### Problem
Tests pass against our implementation, but diverge from real AWS:

```python
# Our implementation
response = sqs.receive_message(QueueUrl=url, WaitTimeSeconds=20)
# AWS max is 20, we might allow 30

# Test passes but behavior is wrong
assert "Messages" in response  # ✅ Passes against us, ❌ Wrong behavior
```

### Solution: AWS Parity Testing

**Tool:** `scripts/parity_test_runner.py`

```python
@pytest.mark.parity
def test_receive_message_wait_time_limits(sqs_local, sqs_aws):
    """Test against both local and real AWS."""

    # Test max wait time
    for wait_time in [0, 10, 20, 21]:
        try:
            local_resp = sqs_local.receive_message(
                QueueUrl=local_queue,
                WaitTimeSeconds=wait_time
            )
            aws_resp = sqs_aws.receive_message(
                QueueUrl=aws_queue,
                WaitTimeSeconds=wait_time
            )

            # Compare behavior
            assert_same_error_or_success(local_resp, aws_resp)

        except ClientError as local_err:
            # If local throws error, AWS should too
            with pytest.raises(ClientError) as aws_err:
                sqs_aws.receive_message(...)

            assert local_err.response["Error"]["Code"] == aws_err.value.response["Error"]["Code"]
```

**Configuration:**

```yaml
# pytest.ini
[parity-testing]
# Run against real AWS in CI on schedule
parity_enabled = ${AWS_PARITY_TESTING:-false}
aws_account_id = ${AWS_TEST_ACCOUNT}
aws_region = us-east-1

# Only test a sample (expensive)
parity_sample_rate = 0.1  # 10% of tests
```

---

## 5. Fully Automated Test Generation

### Current State
Assertion generator creates assertions, but tests still written manually.

### Next Level: Full Test Generation

**Tool:** `scripts/auto_generate_tests.py`

```python
def generate_comprehensive_test_suite(service: str, operation: str) -> str:
    """Generate complete test from botocore spec + behavioral patterns."""

    # 1. Analyze operation spec
    spec = get_operation_spec(service, operation)

    # 2. Detect operation type
    op_type = classify_operation(operation)  # CREATE, READ, UPDATE, DELETE, LIST

    # 3. Select pattern
    if op_type == 'CREATE':
        pattern = CRUD_PATTERN
    elif op_type == 'LIST':
        pattern = LIST_PATTERN
    # ... etc

    # 4. Generate test code
    test_code = pattern.render(
        service=service,
        operation=operation,
        required_params=spec.required_params,
        assertions=generate_assertions(spec.output_shape),
        cleanup=generate_cleanup(op_type)
    )

    return test_code
```

**Example output:**

```python
# Auto-generated from: uv run python scripts/auto_generate_tests.py --service sqs --operation CreateQueue

def test_create_queue_comprehensive(self, sqs):
    """Comprehensive test for CreateQueue operation.

    Auto-generated test covering:
    - Success case with valid inputs
    - Resource appears in list
    - Attributes match expected values
    - Cleanup (delete)
    - Error cases (invalid inputs)
    """
    queue_name = f"test-auto-{uuid4().hex[:8]}"

    # 1. Create resource
    response = sqs.create_queue(QueueName=queue_name)

    # 2. Verify response structure (auto-generated from spec)
    assert "QueueUrl" in response
    assert isinstance(response["QueueUrl"], str)
    assert len(response["QueueUrl"]) > 0
    assert response["QueueUrl"].startswith("http")
    assert queue_name in response["QueueUrl"]

    queue_url = response["QueueUrl"]

    try:
        # 3. Verify resource retrievable
        get_resp = sqs.get_queue_url(QueueName=queue_name)
        assert get_resp["QueueUrl"] == queue_url

        # 4. Verify appears in list
        list_resp = sqs.list_queues()
        assert any(queue_url in url for url in list_resp.get("QueueUrls", []))

        # 5. Verify attributes
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["All"]
        )
        assert "QueueArn" in attrs["Attributes"]
        assert queue_name in attrs["Attributes"]["QueueArn"]

    finally:
        # 6. Cleanup
        sqs.delete_queue(QueueUrl=queue_url)

        # 7. Verify deletion
        with pytest.raises(ClientError) as exc:
            sqs.get_queue_url(QueueName=queue_name)
        assert exc.value.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue"


def test_create_queue_invalid_name(self, sqs):
    """Test CreateQueue rejects invalid queue names."""
    invalid_names = [
        "",  # Empty
        "a" * 81,  # Too long
        "invalid!@#",  # Special chars
        " leading-space",  # Leading space
    ]

    for name in invalid_names:
        with pytest.raises(ClientError) as exc:
            sqs.create_queue(QueueName=name)
        assert exc.value.response["Error"]["Code"] in [
            "InvalidParameterValue",
            "ValidationException"
        ]
```

---

## 6. Test Data Factories (DRY Principle)

### Problem
Repetitive test setup code:

```python
# Repeated in 50 tests
def test_something(self, sqs):
    url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    try:
        # ... test logic ...
    finally:
        sqs.delete_queue(QueueUrl=url)
```

### Solution: Factory Pattern

**Tool:** `tests/factories/sqs.py`

```python
from contextlib import contextmanager
from uuid import uuid4

@contextmanager
def queue(sqs, **kwargs):
    """Factory for SQS queues with automatic cleanup."""
    name = kwargs.pop('QueueName', f"test-{uuid4().hex[:8]}")

    response = sqs.create_queue(QueueName=name, **kwargs)
    url = response["QueueUrl"]

    try:
        yield url
    finally:
        try:
            sqs.delete_queue(QueueUrl=url)
        except:
            pass  # Best effort cleanup


@contextmanager
def queue_with_messages(sqs, count=5, **kwargs):
    """Factory for queue pre-populated with messages."""
    with queue(sqs, **kwargs) as url:
        messages = []
        for i in range(count):
            resp = sqs.send_message(
                QueueUrl=url,
                MessageBody=f"test-message-{i}"
            )
            messages.append(resp["MessageId"])

        yield url, messages
```

**Usage:**

```python
def test_receive_message(self, sqs):
    with queue_with_messages(sqs, count=10) as (url, msg_ids):
        # Queue already has 10 messages
        response = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=5)
        assert len(response["Messages"]) == 5
    # Automatic cleanup, even if test fails
```

---

## 7. Visual Quality Dashboard

### Current
Text reports in CI

### Better
Interactive HTML dashboard

**Tool:** `scripts/generate_quality_dashboard.py`

**Features:**
1. **Service Heatmap** - Color-coded quality grid
2. **Trend Charts** - Quality over time
3. **Coverage Gaps** - Visual list of untested operations
4. **Test Execution** - Which tests take longest
5. **Behavioral Coverage** - Pattern completion percentages

**Example Dashboard Sections:**

```html
<!-- Service Quality Heatmap -->
<div class="heatmap">
  <div class="service" data-quality="100" style="background: #00ff00">
    Events
    <span class="score">100%</span>
  </div>
  <div class="service" data-quality="98" style="background: #33ff00">
    SQS
    <span class="score">98.9%</span>
  </div>
  <!-- ... -->
</div>

<!-- Behavioral Coverage Radar -->
<canvas id="behavioral-radar">
  <!-- Shows % of tests covering each pattern:
       - Create: 95%
       - Read: 88%
       - Update: 45%  <- Weak spot
       - Delete: 92%
       - Error: 67%
  -->
</canvas>

<!-- Quality Trend -->
<canvas id="quality-trend">
  <!-- Line chart showing quality over last 30 commits -->
</canvas>
```

---

## 8. Intelligent Test Prioritization

### Problem
Running all 6,383 tests takes 20+ minutes

### Solution: Smart Test Selection

**Tool:** `scripts/test_prioritizer.py`

```python
def prioritize_tests(changed_files: list[str]) -> list[str]:
    """Select which tests to run based on changes."""

    # 1. Impact analysis
    impacted_services = get_services_for_files(changed_files)

    # 2. Coverage analysis
    critical_tests = get_tests_with_low_behavioral_coverage(impacted_services)

    # 3. Historical analysis
    flaky_tests = get_recently_failed_tests(days=7)

    # 4. Mutation analysis
    weak_tests = get_tests_that_dont_kill_mutations(impacted_services)

    # Prioritize: Critical > Weak > Flaky > All others
    return sorted_by_priority([
        *critical_tests,
        *weak_tests,
        *flaky_tests,
        *get_all_tests_for_services(impacted_services)
    ])
```

**CI Integration:**

```yaml
# Run critical tests first, fail fast
- name: Run critical tests
  run: |
    TESTS=$(uv run python scripts/test_prioritizer.py --critical-only)
    uv run pytest $TESTS -x  # Fail fast

- name: Run remaining tests
  if: success()
  run: |
    TESTS=$(uv run python scripts/test_prioritizer.py --non-critical)
    uv run pytest $TESTS -n 12  # Parallel
```

---

## Implementation Priority

### Quick Wins (1 week)
1. ✅ **Behavioral coverage tracking** - Extend validate_test_quality.py
2. ✅ **Test factories** - Create common patterns for top 10 services
3. ✅ **Quality dashboard** - HTML report from existing data

### Medium Effort (2-4 weeks)
4. ⭐ **Auto test generation** - Generate full tests from specs
5. ⭐ **Property-based tests** - Add Hypothesis for top 20 services
6. ⭐ **Mutation testing** - Run on changed files in CI

### Long Term (1-3 months)
7. 🚀 **AWS parity testing** - Infrastructure for real AWS comparison
8. 🚀 **Test prioritization** - Smart test selection
9. 🚀 **Full automation** - New operations auto-get comprehensive tests

---

## Expected Impact

### After Quick Wins
- Behavioral coverage visible per service
- Common setup code eliminated (factories)
- Quality trends visible in dashboard

### After Medium Effort
- 80% of new tests auto-generated
- Edge cases automatically discovered
- Tests verified to actually catch bugs

### After Long Term
- Behavior matches AWS (verified, not assumed)
- Test suite runs 3-5x faster (smart selection)
- Quality improvements self-sustaining

---

## The Ultimate Goal

**Today:** We write tests to verify operations work

**Tomorrow:** Tests are automatically generated, verified against AWS, mutation-tested for effectiveness, and intelligently selected for fast feedback

**Measuring success:**
- 95%+ behavioral coverage (not just structural)
- 100% of tests kill mutations (actually catch bugs)
- <5 minute test suite (smart selection)
- 99% AWS parity (verified quarterly)
- Zero manual test writing (full automation)
