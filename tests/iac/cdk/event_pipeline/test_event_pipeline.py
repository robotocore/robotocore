"""IaC test: CDK Event Pipeline with EventBridge, SQS, and SNS."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent
STACK_NAME = "CdkEventPipelineStack"


class TestEventPipeline:
    """Deploy a CDK event pipeline stack and validate all resources."""

    @pytest.fixture(scope="class")
    def deployed(self, cdk_runner):
        """Deploy the CDK stack and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, STACK_NAME)
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, STACK_NAME)

    def test_queue_created(self, deployed):
        """Verify the SQS queue exists."""
        client = make_client("sqs")
        queues = client.list_queues()
        queue_urls = queues.get("QueueUrls", [])
        assert any(
            "CdkEventPipelineStack" in url or "queue" in url.lower() for url in queue_urls
        ), f"Expected SQS queue not found. Queues: {queue_urls}"

    def test_topic_created(self, deployed):
        """Verify the SNS topic exists."""
        client = make_client("sns")
        topics = client.list_topics()
        topic_arns = [t["TopicArn"] for t in topics["Topics"]]
        assert any(
            "CdkEventPipelineStack" in arn or "topic" in arn.lower() for arn in topic_arns
        ), f"Expected SNS topic not found. Topics: {topic_arns}"

    def test_rule_created(self, deployed):
        """Verify the EventBridge rule exists."""
        client = make_client("events")
        rules = client.list_rules()
        rule_names = [r["Name"] for r in rules["Rules"]]
        assert any(
            "CdkEventPipelineStack" in name or "rule" in name.lower() for name in rule_names
        ), f"Expected EventBridge rule not found. Rules: {rule_names}"
