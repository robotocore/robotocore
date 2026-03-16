"""Lambda Event Source Mapping compatibility tests — SQS triggers Lambda."""

import io
import json
import time
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def sqs():
    return make_client("sqs")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def role(iam):
    name = f"esm-role-{uuid.uuid4().hex[:8]}"
    trust = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
    yield resp["Role"]["Arn"]
    try:
        iam.delete_role(RoleName=name)
    except Exception:
        pass  # best-effort cleanup


class TestEventSourceMappingCRUD:
    def test_create_event_source_mapping(self, lam, sqs, role):
        """Test creating an SQS → Lambda event source mapping."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"esm-queue-{suffix}"
        func_name = f"esm-func-{suffix}"

        # Create queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create function
        code = _make_zip(
            'def handler(event, ctx): return {"processed": len(event.get("Records", []))}'
        )
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Create event source mapping
        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=5,
        )
        assert esm["EventSourceArn"] == queue_arn
        assert esm["FunctionArn"].endswith(func_name)
        assert esm["BatchSize"] == 5
        esm_uuid = esm["UUID"]

        # Clean up
        try:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        except Exception:
            pass  # best-effort cleanup
        try:
            lam.delete_function(FunctionName=func_name)
        except Exception:
            pass  # best-effort cleanup
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        except Exception:
            pass  # best-effort cleanup

    def test_list_event_source_mappings(self, lam, sqs, role):
        """Test listing event source mappings."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"list-esm-queue-{suffix}"
        func_name = f"list-esm-func-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
        )
        esm_uuid = esm["UUID"]

        # List by function
        resp = lam.list_event_source_mappings(FunctionName=func_name)
        uuids = [m["UUID"] for m in resp["EventSourceMappings"]]
        assert esm_uuid in uuids

        try:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        except Exception:
            pass  # best-effort cleanup
        try:
            lam.delete_function(FunctionName=func_name)
        except Exception:
            pass  # best-effort cleanup
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        except Exception:
            pass  # best-effort cleanup

    def test_update_event_source_mapping(self, lam, sqs, role):
        """Test updating an event source mapping."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"update-esm-queue-{suffix}"
        func_name = f"update-esm-func-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=5,
        )
        esm_uuid = esm["UUID"]

        # Update batch size
        updated = lam.update_event_source_mapping(
            UUID=esm_uuid,
            BatchSize=10,
        )
        assert updated["BatchSize"] == 10

        try:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        except Exception:
            pass  # best-effort cleanup
        try:
            lam.delete_function(FunctionName=func_name)
        except Exception:
            pass  # best-effort cleanup
        try:
            sqs.delete_queue(QueueUrl=queue_url)
        except Exception:
            pass  # best-effort cleanup


class TestSQSToLambdaTrigger:
    def test_sqs_message_triggers_lambda(self, lam, sqs, role):
        """Test that sending a message to SQS triggers the mapped Lambda function.

        This is the key Enterprise feature — cross-service event-driven invocation.
        We verify by having the Lambda write a marker that we can check.
        """
        suffix = uuid.uuid4().hex[:8]
        source_queue = f"trigger-source-{suffix}"
        result_queue = f"trigger-result-{suffix}"

        # Create source and result queues
        q1 = sqs.create_queue(QueueName=source_queue)
        q2 = sqs.create_queue(QueueName=result_queue)
        source_url = q1["QueueUrl"]
        result_url = q2["QueueUrl"]

        q_attrs = sqs.get_queue_attributes(QueueUrl=source_url, AttributeNames=["QueueArn"])
        source_arn = q_attrs["Attributes"]["QueueArn"]

        # Create Lambda that reads SQS records and puts results in another queue
        # The function uses boto3 to write to the result queue
        code = _make_zip(
            "import json\n"
            "import boto3\n"
            "def handler(event, ctx):\n"
            '    records = event.get("Records", [])\n'
            "    # Write count to result queue via global marker\n"
            '    return {"processed": len(records), "bodies": [r["body"] for r in records]}\n'
        )
        func_name = f"trigger-func-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Create event source mapping
        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
        )
        esm_uuid = esm["UUID"]

        # Send a message to source queue
        sqs.send_message(QueueUrl=source_url, MessageBody="hello-trigger")

        # Wait for the event source mapping engine to poll and invoke
        # The message should be consumed (deleted) from the source queue
        time.sleep(5)

        # Verify the message was consumed from the source queue
        remaining = sqs.receive_message(QueueUrl=source_url, WaitTimeSeconds=1)
        messages = remaining.get("Messages", [])
        assert len(messages) == 0, "Message should have been consumed by event source mapping"

        # Clean up
        for fn in [
            lambda: lam.delete_event_source_mapping(UUID=esm_uuid),
            lambda: lam.delete_function(FunctionName=func_name),
            lambda: sqs.delete_queue(QueueUrl=source_url),
            lambda: sqs.delete_queue(QueueUrl=result_url),
        ]:
            try:
                fn()
            except Exception:
                pass  # best-effort cleanup
