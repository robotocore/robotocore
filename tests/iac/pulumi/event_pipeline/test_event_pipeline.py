"""IaC test: pulumi - event_pipeline.

Deploys an EventBridge rule targeting an SQS queue, plus an SNS topic with
an SQS subscription, and validates all resources exist via boto3.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def pipeline_outputs(pulumi_runner, ensure_server, tmp_path_factory):
    """Deploy the event_pipeline Pulumi program and return stack outputs."""
    import shutil

    src_dir = Path(__file__).parent
    work_dir = tmp_path_factory.mktemp("pulumi-event-pipeline")

    # Copy Pulumi program files
    for f in src_dir.iterdir():
        if f.name.startswith("test_") or f.name == "__pycache__":
            continue
        if f.is_file():
            shutil.copy2(f, work_dir / f.name)

    # Initialize stack
    init_result = pulumi_runner.run(
        ["pulumi", "stack", "init", "test"],
        work_dir,
        env={"PULUMI_CONFIG_PASSPHRASE": "", "PULUMI_BACKEND_URL": "file://~"},
    )
    if init_result.returncode != 0 and "already exists" not in init_result.stderr:
        pytest.fail(f"pulumi stack init failed:\n{init_result.stderr}")

    result = pulumi_runner.up(work_dir, stack="test")
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}\n{result.stdout}")

    outputs = pulumi_runner.stack_output(work_dir, stack="test")

    yield outputs

    # Teardown
    pulumi_runner.destroy(work_dir, stack="test")


@pytest.fixture(scope="module")
def sqs_client():
    return make_client("sqs")


@pytest.fixture(scope="module")
def sns_client():
    return make_client("sns")


@pytest.fixture(scope="module")
def events_client():
    return make_client("events")


class TestEventPipeline:
    """Validate event pipeline resources created by Pulumi."""

    def test_queue_created(self, pipeline_outputs, sqs_client):
        """Verify the SQS queue exists."""
        queue_url = pipeline_outputs["queue_url"]
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        assert "Attributes" in attrs
        assert attrs["Attributes"]["QueueArn"] == pipeline_outputs["queue_arn"]

    def test_topic_created(self, pipeline_outputs, sns_client):
        """Verify the SNS topic exists."""
        topic_arn = pipeline_outputs["topic_arn"]
        resp = sns_client.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn

    def test_rule_created(self, pipeline_outputs, events_client):
        """Verify the EventBridge rule exists with correct event pattern."""
        rule_name = pipeline_outputs["rule_name"]
        resp = events_client.describe_rule(Name=rule_name)
        assert resp["Name"] == rule_name
        assert resp["State"] == "ENABLED"
        assert "my.app" in resp["EventPattern"]
