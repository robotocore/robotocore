# Skill: Use robotocore in a Project's Test Suite

Use this skill when adding robotocore to an existing project so tests can run against a local AWS digital twin instead of real AWS or mocks.

## What you're setting up

robotocore runs as a Docker container on port 4566 and responds to real AWS SDK calls. Tests use real boto3 clients pointed at `http://localhost:4566`. No mocking libraries needed.

## Step 1 — Ensure robotocore is running

### Option A: start it per-session (local dev)

```bash
docker run -d -p 4566:4566 --name robotocore robotocore/robotocore:latest
```

### Option B: docker-compose (team standard)

Add to `docker-compose.yml`:

```yaml
services:
  aws:
    image: robotocore/robotocore:latest
    ports:
      - "4566:4566"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:4566/_localstack/health"]
      interval: 5s
      retries: 10
```

### Option C: GitHub Actions

```yaml
services:
  robotocore:
    image: robotocore/robotocore:latest
    ports:
      - 4566:4566
    options: >-
      --health-cmd "curl -f http://localhost:4566/_localstack/health"
      --health-interval 5s
      --health-retries 10
```

## Step 2 — Add pytest fixtures

Create `tests/conftest.py` (or add to an existing one):

```python
import uuid
import boto3
import pytest

ENDPOINT = "http://localhost:4566"
REGION   = "us-east-1"


def _session(account_id: str = "123456789012") -> dict:
    """Base kwargs for any boto3 client/resource."""
    return dict(
        endpoint_url=ENDPOINT,
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        region_name=REGION,
    )


@pytest.fixture(scope="session")
def aws():
    """Shared boto3 session kwargs. Uses a fixed account ID for the test session."""
    return _session()


@pytest.fixture
def isolated_aws():
    """Per-test boto3 kwargs with a unique account ID.

    Resources created with this fixture are invisible to other tests.
    Use this when tests must not share state.
    """
    account_id = str(uuid.uuid4().int)[:12].zfill(12)
    return _session(account_id)


@pytest.fixture
def s3(aws):
    return boto3.client("s3", **aws)


@pytest.fixture
def sqs(aws):
    return boto3.client("sqs", **aws)


@pytest.fixture
def sns(aws):
    return boto3.client("sns", **aws)


@pytest.fixture
def dynamodb(aws):
    return boto3.resource("dynamodb", **aws)


@pytest.fixture
def lambda_client(aws):
    return boto3.client("lambda", **aws)


@pytest.fixture
def ssm(aws):
    return boto3.client("ssm", **aws)


@pytest.fixture
def secrets(aws):
    return boto3.client("secretsmanager", **aws)
```

## Step 3 — Write tests using the fixtures

```python
# tests/test_myfeature.py
import uuid


def test_uploads_report_to_s3(s3):
    bucket = f"test-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)

    # call the code under test
    from myapp.reports import generate_and_upload
    generate_and_upload(bucket=bucket, s3_client=s3)

    objects = s3.list_objects_v2(Bucket=bucket)["Contents"]
    assert any(o["Key"].endswith(".csv") for o in objects)


def test_sends_notification(sns, sqs):
    queue = sqs.create_queue(QueueName=f"q-{uuid.uuid4().hex[:8]}")
    url   = queue["QueueUrl"]
    arn   = sqs.get_queue_attributes(
        QueueUrl=url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    topic = sns.create_topic(Name=f"t-{uuid.uuid4().hex[:8]}")
    sns.subscribe(TopicArn=topic["TopicArn"], Protocol="sqs", Endpoint=arn)

    from myapp.notifications import send_alert
    send_alert(topic_arn=topic["TopicArn"], sns_client=sns, message="test")

    msgs = sqs.receive_message(QueueUrl=url)
    assert len(msgs.get("Messages", [])) == 1
```

## Step 4 — Isolation strategies

**Same account for all tests** (fast, may have resource name collisions):
- Use unique resource names with `uuid.uuid4().hex[:8]` suffix
- Clean up in teardown or accept the state is ephemeral per container restart

**Per-test accounts** (fully isolated, slightly slower):
- Use `isolated_aws` fixture instead of `aws`
- Each test gets its own account ID, so bucket/queue names can be identical

```python
def test_isolated(isolated_aws):
    s3 = boto3.client("s3", **isolated_aws)
    s3.create_bucket(Bucket="assets")  # won't conflict with any other test
```

## Step 5 — Make the endpoint configurable

Let tests run against real AWS too (for CI on the real cloud):

```python
# tests/conftest.py
import os

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
```

```bash
# Against robotocore (default)
pytest tests/

# Against real AWS
AWS_ENDPOINT_URL="" pytest tests/
```

## Adding robotocore as a dev dependency

```toml
# pyproject.toml — no Python package needed, just document the Docker image
[tool.robotocore]
image = "robotocore/robotocore:latest"
port  = 4566
```

Or just document it in `README.md` or `CONTRIBUTING.md`:

```markdown
## Running tests

Start robotocore before running tests:

    docker run -d -p 4566:4566 robotocore/robotocore:latest
    pytest tests/
```
