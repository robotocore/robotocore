# Implementation Plan: Next-Level Test Quality

## Scope

Implement 3 quick wins that deliver immediate value:

1. **Behavioral Coverage Tracking** - Detect CRUD patterns in tests
2. **Test Data Factories** - Reusable fixtures with auto-cleanup
3. **Visual Quality Dashboard** - HTML report with heatmaps and trends

## Implementation Details

### 1. Behavioral Coverage Tracking

**File:** `scripts/validate_test_quality.py` (extend existing)

**New functionality:**
- Detect which behavioral patterns each test covers
- Score tests 0-100% based on pattern completeness
- Report overall behavioral coverage per service

**Patterns to detect:**
| Pattern | Detection | Example |
|---------|-----------|---------|
| CREATE | Calls Create*/Put* operation | `create_queue()` |
| RETRIEVE | Calls Get*/Describe* with ID from create | `get_queue_url(QueueName=name)` |
| LIST | Calls List*/Describe* (plural) | `list_queues()` |
| UPDATE | Calls Update*/Modify*/Set* | `set_queue_attributes()` |
| DELETE | Calls Delete*/Remove* | `delete_queue()` |
| ERROR | Uses pytest.raises or catches ClientError | `pytest.raises(ClientError)` |

**Output format:**
```
Service: sqs
  Behavioral coverage: 67% (4/6 patterns avg)

  test_create_queue:
    ✅ CREATE  ✅ RETRIEVE  ❌ LIST  ❌ UPDATE  ✅ DELETE  ❌ ERROR
    Score: 50% (3/6)

  test_send_receive_message:
    ✅ CREATE  ✅ RETRIEVE  ❌ LIST  ❌ UPDATE  ✅ DELETE  ✅ ERROR
    Score: 67% (4/6)
```

### 2. Test Data Factories

**Directory:** `tests/factories/`

**Files to create:**
- `tests/factories/__init__.py` - Base factory utilities
- `tests/factories/sqs.py` - SQS resource factories
- `tests/factories/s3.py` - S3 resource factories
- `tests/factories/iam.py` - IAM resource factories
- `tests/factories/dynamodb.py` - DynamoDB resource factories
- `tests/factories/sns.py` - SNS resource factories

**Pattern:**
```python
@contextmanager
def resource_name(client, **kwargs):
    """Create resource with auto-cleanup."""
    response = client.create_resource(**kwargs)
    resource_id = response["ResourceId"]
    try:
        yield response  # or resource_id
    finally:
        client.delete_resource(ResourceId=resource_id)
```

**Factories to implement:**
| Service | Factory | Creates |
|---------|---------|---------|
| SQS | `queue()` | Queue with optional messages |
| SQS | `fifo_queue()` | FIFO queue |
| S3 | `bucket()` | Bucket with optional objects |
| IAM | `user()` | User with optional policies |
| IAM | `role()` | Role with trust policy |
| DynamoDB | `table()` | Table with schema |
| SNS | `topic()` | Topic with optional subscriptions |

### 3. Visual Quality Dashboard

**File:** `scripts/generate_quality_dashboard.py`

**Output:** `docs/quality/index.html`

**Sections:**
1. **Summary Cards** - Total services, avg quality, test count
2. **Service Heatmap** - Color grid by quality percentage
3. **Quality Trend** - Line chart over recent commits
4. **Behavioral Coverage** - Radar chart of pattern coverage
5. **Top Issues** - List of lowest-quality services
6. **Test Execution** - Slowest tests

**Data sources:**
- `validate_test_quality.py --json` - Quality metrics
- `compat_coverage.py --json` - Coverage metrics
- Git history - Trend data

## Execution Order

### Phase 1: Behavioral Coverage (45 min)
1. Read current validate_test_quality.py
2. Add BehavioralCoverageVisitor class
3. Add pattern detection logic
4. Add scoring function
5. Add CLI output formatting
6. Test on SQS and IAM
7. Commit

### Phase 2: Test Factories (45 min)
1. Create tests/factories/ directory
2. Implement base utilities
3. Implement SQS factories (queue, fifo_queue, queue_with_messages)
4. Implement S3 factories (bucket, bucket_with_objects)
5. Implement IAM factories (user, role)
6. Implement DynamoDB factories (table)
7. Implement SNS factories (topic)
8. Add tests for factories
9. Commit

### Phase 3: Quality Dashboard (30 min)
1. Create dashboard generator script
2. Implement data collection
3. Generate HTML with inline CSS/JS
4. Add heatmap visualization
5. Add trend chart (last 10 data points)
6. Add behavioral radar
7. Test output
8. Commit

### Phase 4: Integration (15 min)
1. Add dashboard generation to CI
2. Update documentation
3. Final commit and push

## Success Criteria

- [x] `validate_test_quality.py --behavioral` shows pattern coverage
- [x] Test factories work for 5 services (SQS, S3, IAM, DynamoDB, SNS)
- [x] Dashboard HTML renders correctly
- [x] All new code passes lint
- [x] CI workflow updated with behavioral coverage reporting
- [x] Dashboard integrated into GitHub Pages build
