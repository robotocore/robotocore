"""Terraform IaC test: Event pipeline (SQS -> Lambda -> DynamoDB)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_dynamodb_table_exists,
    assert_iam_role_exists,
    assert_lambda_function_exists,
    assert_sqs_queue_exists,
)

pytestmark = [
    pytest.mark.iac,
    pytest.mark.terraform,
]


@pytest.fixture(scope="module")
def deployed(terraform_dir, tf_runner):
    """Apply the event pipeline Terraform scenario and return outputs."""
    result = tf_runner.apply(terraform_dir)
    if result.returncode != 0:
        pytest.fail(f"terraform apply failed:\n{result.stderr}\n{result.stdout}")
    return tf_runner.output(terraform_dir)


class TestEventPipeline:
    """Validate that Terraform-provisioned event pipeline resources exist."""

    def test_sqs_queue_exists(self, deployed):
        """SQS queue created by Terraform is visible via the AWS API."""
        client = make_client("sqs")
        queue_url = assert_sqs_queue_exists(client, "rc-evpipe-inbox")
        assert "rc-evpipe-inbox" in queue_url

    def test_sqs_queue_attributes(self, deployed):
        """SQS queue has the expected visibility timeout."""
        client = make_client("sqs")
        queue_url = deployed["queue_url"]["value"]
        attrs = client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["VisibilityTimeout"],
        )["Attributes"]
        assert attrs["VisibilityTimeout"] == "60"

    def test_dynamodb_table_exists(self, deployed):
        """DynamoDB table created by Terraform is visible and ACTIVE."""
        table_name = deployed["table_name"]["value"]
        client = make_client("dynamodb")
        table = assert_dynamodb_table_exists(client, table_name)
        key_schema = table["KeySchema"]
        hash_keys = [k["AttributeName"] for k in key_schema if k["KeyType"] == "HASH"]
        assert "message_id" in hash_keys

    def test_lambda_function_exists(self, deployed):
        """Lambda function created by Terraform is visible via the AWS API."""
        function_name = deployed["function_name"]["value"]
        client = make_client("lambda")
        config = assert_lambda_function_exists(client, function_name)
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "handler.handler"
        assert config["Timeout"] == 30

    def test_lambda_environment_has_table_name(self, deployed):
        """Lambda function has TABLE_NAME environment variable set."""
        function_name = deployed["function_name"]["value"]
        table_name = deployed["table_name"]["value"]
        client = make_client("lambda")
        resp = client.get_function_configuration(FunctionName=function_name)
        env_vars = resp.get("Environment", {}).get("Variables", {})
        assert env_vars.get("TABLE_NAME") == table_name

    def test_iam_role_exists(self, deployed):
        """IAM execution role for the Lambda is visible via the AWS API."""
        client = make_client("iam")
        assert_iam_role_exists(client, "rc-evpipe-lambda-role")

    def test_event_source_mapping_exists(self, deployed):
        """Event source mapping from SQS to Lambda exists."""
        function_name = deployed["function_name"]["value"]
        client = make_client("lambda")
        mappings = client.list_event_source_mappings(FunctionName=function_name)[
            "EventSourceMappings"
        ]
        assert len(mappings) >= 1, "No event source mappings found"
        esm = mappings[0]
        assert "sqs" in esm["EventSourceArn"].lower() or "rc-evpipe-inbox" in esm["EventSourceArn"]
        assert esm["BatchSize"] == 10
