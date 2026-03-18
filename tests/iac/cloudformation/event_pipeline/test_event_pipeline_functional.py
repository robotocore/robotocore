"""Functional test: deploy event pipeline and exercise SQS + DynamoDB data flows."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.functional_validator import (
    put_and_get_dynamodb_item,
    send_and_receive_sqs,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestEventPipelineFunctional:
    """Deploy event pipeline and verify SQS message flow and DynamoDB storage."""

    def test_sqs_message_roundtrip(self, deploy_stack):
        """Send a message to SQS and receive it back."""
        stack = deploy_stack("evt-pipe-func", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        queue_url = outputs["QueueUrl"]
        sqs = make_client("sqs")

        msg = send_and_receive_sqs(sqs, queue_url, '{"event": "order_placed", "id": "123"}')
        assert msg["Body"] == '{"event": "order_placed", "id": "123"}'
        assert "MessageId" in msg

    def test_dynamodb_item_roundtrip(self, deploy_stack):
        """Put and get an item in the messages DynamoDB table."""
        stack = deploy_stack("evt-pipe-func-ddb", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        table_name = outputs["TableName"]
        dynamodb = make_client("dynamodb")

        item = {
            "message_id": {"S": "msg-001"},
            "status": {"S": "processed"},
            "payload": {"S": '{"event": "order_placed"}'},
        }
        key = {"message_id": {"S": "msg-001"}}

        returned = put_and_get_dynamodb_item(dynamodb, table_name, item, key)
        assert returned["message_id"] == {"S": "msg-001"}
        assert returned["status"] == {"S": "processed"}
        assert returned["payload"] == {"S": '{"event": "order_placed"}'}

    def test_sqs_multiple_messages(self, deploy_stack):
        """Send multiple messages and verify each is received.

        ESM is disabled to prevent the Lambda poller from racing with the
        test's receive_message calls (the poller runs every 1 second and
        would consume messages before the test can).
        """
        stack = deploy_stack("evt-pipe-func-multi", TEMPLATE, {"EsmEnabled": "false"})
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        queue_url = outputs["QueueUrl"]
        sqs = make_client("sqs")

        for i in range(3):
            body = f'{{"seq": {i}}}'
            msg = send_and_receive_sqs(sqs, queue_url, body)
            assert msg["Body"] == body
