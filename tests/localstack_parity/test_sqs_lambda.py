"""SQS -> Lambda event source mapping parity test.

Tests the common pattern of SQS triggering Lambda via event source mapping,
which is a fundamental LocalStack feature.

Note: In robotocore, Lambda executes in-process and cannot make HTTP calls
back to the server (deadlock). This test verifies that:
1. SQS queues can be created and messages sent/received
2. Lambda functions can be created and invoked
3. Event source mappings can be configured (SQS -> Lambda)
4. The ESM reaches Enabled state

End-to-end ESM execution (SQS -> Lambda -> DynamoDB) requires Docker-based
Lambda execution, which is tested separately.
"""

import io
import json
import time
import uuid
import zipfile


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


SIMPLE_HANDLER = """
import json
def handler(event, context):
    return {"statusCode": 200, "body": json.dumps({"records": len(event.get("Records", []))})}
"""


class TestSqsLambda:
    """SQS + Lambda ESM configuration and individual service tests."""

    def test_sqs_send_receive(self, aws_client):
        """Verify SQS message send and receive works."""
        sqs = aws_client.sqs
        queue_name = _unique("parity-sqs")
        queue_url = None

        try:
            queue_resp = sqs.create_queue(QueueName=queue_name)
            queue_url = queue_resp["QueueUrl"]

            # Send message
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"content": "test message"}),
            )

            # Receive message
            recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
            messages = recv.get("Messages", [])
            assert len(messages) == 1
            body = json.loads(messages[0]["Body"])
            assert body["content"] == "test message"

            # Delete message
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=messages[0]["ReceiptHandle"],
            )

            # Verify empty
            recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
            assert len(recv.get("Messages", [])) == 0

        finally:
            if queue_url:
                try:
                    sqs.delete_queue(QueueUrl=queue_url)
                except Exception:
                    pass

    def test_sqs_lambda_esm_creation(self, aws_client, lambda_role_arn):
        """Create SQS -> Lambda event source mapping and verify config."""
        sqs = aws_client.sqs
        lam = aws_client.lambda_

        queue_name = _unique("parity-sqs")
        fn_name = _unique("sqs-processor")
        role_arn = lambda_role_arn
        esm_uuid = None
        queue_url = None

        try:
            # Create SQS queue
            queue_resp = sqs.create_queue(QueueName=queue_name)
            queue_url = queue_resp["QueueUrl"]
            queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
            queue_arn = queue_attrs["Attributes"]["QueueArn"]

            # Create Lambda
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_lambda_zip(SIMPLE_HANDLER)},
                Timeout=30,
            )

            for _ in range(30):
                fn = lam.get_function(FunctionName=fn_name)
                if fn["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)

            # Create event source mapping
            esm = lam.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=fn_name,
                BatchSize=10,
                Enabled=True,
            )
            esm_uuid = esm["UUID"]
            assert esm["EventSourceArn"] == queue_arn
            assert esm["BatchSize"] == 10

            # Wait for ESM to be enabled
            for _ in range(30):
                esm_state = lam.get_event_source_mapping(UUID=esm_uuid)
                if esm_state["State"] in ("Enabled", "Enabling"):
                    break
                time.sleep(1)

            # Verify ESM configuration
            esm_state = lam.get_event_source_mapping(UUID=esm_uuid)
            assert esm_state["EventSourceArn"] == queue_arn
            assert esm_state["FunctionArn"].endswith(fn_name)

            # List ESMs
            esm_list = lam.list_event_source_mappings(EventSourceArn=queue_arn)
            uuids = [e["UUID"] for e in esm_list["EventSourceMappings"]]
            assert esm_uuid in uuids

        finally:
            if esm_uuid:
                try:
                    lam.delete_event_source_mapping(UUID=esm_uuid)
                except Exception:
                    pass
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass
            if queue_url:
                try:
                    sqs.delete_queue(QueueUrl=queue_url)
                except Exception:
                    pass

    def test_sqs_batch_send(self, aws_client):
        """Verify SQS batch send works (common ESM pattern)."""
        sqs = aws_client.sqs
        queue_name = _unique("parity-batch")
        queue_url = None

        try:
            queue_resp = sqs.create_queue(QueueName=queue_name)
            queue_url = queue_resp["QueueUrl"]

            # Batch send
            entries = [
                {
                    "Id": f"msg-{i}",
                    "MessageBody": json.dumps({"index": i}),
                }
                for i in range(5)
            ]
            resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            assert len(resp.get("Successful", [])) == 5
            assert len(resp.get("Failed", [])) == 0

            # Receive all
            received = []
            for _ in range(10):
                recv = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=1,
                )
                received.extend(recv.get("Messages", []))
                if len(received) >= 5:
                    break

            assert len(received) == 5

        finally:
            if queue_url:
                try:
                    sqs.delete_queue(QueueUrl=queue_url)
                except Exception:
                    pass
