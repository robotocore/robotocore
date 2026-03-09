"""IaC test: CloudFormation event pipeline with SQS, Lambda, and DynamoDB."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import (
    assert_dynamodb_table_exists,
    assert_iam_role_exists,
    assert_lambda_function_exists,
    assert_sqs_queue_exists,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


class TestEventPipeline:
    """Deploy an event pipeline stack and validate all resources."""

    def test_deploy_and_validate(self, deploy_stack):
        stack = deploy_stack("event-pipeline", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs into a dict
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}

        sqs_client = make_client("sqs")
        lambda_client = make_client("lambda")
        dynamodb_client = make_client("dynamodb")
        iam_client = make_client("iam")

        # Validate SQS queue
        queue_name = outputs["QueueName"]
        queue_url = assert_sqs_queue_exists(sqs_client, queue_name)
        assert queue_url  # non-empty URL

        # Validate Lambda function
        function_name = outputs["FunctionName"]
        config = assert_lambda_function_exists(lambda_client, function_name)
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"

        # Validate DynamoDB table
        table_name = outputs["TableName"]
        table = assert_dynamodb_table_exists(dynamodb_client, table_name)
        key_schema = {ks["AttributeName"]: ks["KeyType"] for ks in table["KeySchema"]}
        assert key_schema == {"message_id": "HASH"}

        # Validate IAM role
        role_name = outputs["RoleName"]
        assert_iam_role_exists(iam_client, role_name)

        # Validate event source mapping links SQS queue to Lambda
        queue_arn = outputs["QueueArn"]
        function_arn = outputs["FunctionArn"]
        esm_resp = lambda_client.list_event_source_mappings(
            EventSourceArn=queue_arn,
            FunctionName=function_name,
        )
        mappings = esm_resp.get("EventSourceMappings", [])
        assert len(mappings) >= 1, (
            f"Expected at least 1 event source mapping for {queue_arn} -> {function_name}"
        )
        mapping = mappings[0]
        assert mapping["EventSourceArn"] == queue_arn
        assert function_arn in mapping["FunctionArn"] or function_name in mapping["FunctionArn"]
