"""Lambda Dead Letter Queue (DLQ) and Destinations compatibility tests.

Tests async invocation flows:
- DLQ: failed async invocations route error records to SQS queues or SNS topics
- Destinations: OnSuccess/OnFailure routing to SQS, SNS, and Lambda targets
"""

import io
import json
import time
import uuid
import zipfile

import pytest

from tests.compatibility.conftest import make_client

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _poll_sqs(sqs, queue_url: str, timeout: int = 8) -> list[dict]:
    """Poll SQS queue until messages arrive or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        msgs = resp.get("Messages", [])
        if msgs:
            return msgs
        time.sleep(0.5)
    return []


@pytest.fixture
def lam():
    return make_client("lambda")


@pytest.fixture
def sqs():
    return make_client("sqs")


@pytest.fixture
def sns():
    return make_client("sns")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def role(iam):
    name = _unique("dlq-dest-role")
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
    iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)
    arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{name}"
    yield arn
    iam.delete_role(RoleName=name)


class TestLambdaDLQ:
    """Dead Letter Queue tests — failed async invocations route to DLQ."""

    def test_dlq_sqs_on_async_failure(self, lam, sqs, role):
        """Failed async invocation sends error record to SQS DLQ."""
        fn_name = _unique("dlq-sqs-fn")
        queue_name = _unique("dlq-sqs-q")

        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}"

        code = _make_zip('def handler(event, ctx): raise ValueError("intentional error")')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            DeadLetterConfig={"TargetArn": queue_arn},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"trigger": "dlq-test"}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url)
            assert len(msgs) >= 1, "Expected at least one DLQ message"

            body = json.loads(msgs[0]["Body"])
            assert body["requestContext"]["condition"] == "RetriesExhausted"
            assert "functionArn" in body["requestContext"]
            assert body["requestPayload"] == {"trigger": "dlq-test"}
            assert "errorMessage" in body
        finally:
            lam.delete_function(FunctionName=fn_name)
            sqs.delete_queue(QueueUrl=queue_url)

    def test_dlq_sns_on_async_failure(self, lam, sqs, sns, role):
        """Failed async invocation sends error record to SNS DLQ topic."""
        fn_name = _unique("dlq-sns-fn")
        topic_name = _unique("dlq-sns-topic")
        sub_queue_name = _unique("dlq-sns-sub-q")

        # Create SNS topic + SQS subscription to observe the message
        topic = sns.create_topic(Name=topic_name)
        topic_arn = topic["TopicArn"]

        q = sqs.create_queue(QueueName=sub_queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{sub_queue_name}"
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        code = _make_zip('def handler(event, ctx): raise RuntimeError("boom")')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            DeadLetterConfig={"TargetArn": topic_arn},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"x": 42}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url)
            assert len(msgs) >= 1, "Expected SNS->SQS DLQ message"

            # SNS wraps the message in an envelope
            envelope = json.loads(msgs[0]["Body"])
            assert envelope["Type"] == "Notification"
            assert envelope["Subject"] == "Lambda DLQ"

            inner = json.loads(envelope["Message"])
            assert inner["requestContext"]["condition"] == "RetriesExhausted"
            assert inner["requestPayload"] == {"x": 42}
        finally:
            lam.delete_function(FunctionName=fn_name)
            sqs.delete_queue(QueueUrl=queue_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_async_failure_without_dlq_is_silent(self, lam, role):
        """Async invocation failure without DLQ configured does not crash."""
        fn_name = _unique("no-dlq-fn")

        code = _make_zip('def handler(event, ctx): raise Exception("no dlq")')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"silent": True}),
            )
            # Should still return 202 — failure is swallowed
            assert resp["StatusCode"] == 202

            # Wait a moment for async processing to complete without crash
            time.sleep(2)

            # Verify the function still exists and is callable (server didn't crash)
            config = lam.get_function(FunctionName=fn_name)
            assert config["Configuration"]["FunctionName"] == fn_name
        finally:
            lam.delete_function(FunctionName=fn_name)


class TestLambdaDestinations:
    """Invocation destination tests — OnSuccess/OnFailure routing."""

    def test_onsuccess_destination_sqs(self, lam, sqs, role):
        """Successful async invocation routes to OnSuccess SQS destination."""
        fn_name = _unique("dest-ok-fn")
        queue_name = _unique("dest-ok-q")

        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}"

        code = _make_zip('def handler(event, ctx): return {"result": "ok"}')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.put_function_event_invoke_config(
            FunctionName=fn_name,
            DestinationConfig={"OnSuccess": {"Destination": queue_arn}},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"input": "hello"}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url)
            assert len(msgs) >= 1, "Expected OnSuccess destination message"

            body = json.loads(msgs[0]["Body"])
            assert body["version"] == "1.0"
            assert body["requestContext"]["condition"] == "Success"
            assert body["requestPayload"] == {"input": "hello"}
            assert body["responsePayload"] == {"result": "ok"}
            assert body["responseContext"]["statusCode"] == 200
        finally:
            lam.delete_function(FunctionName=fn_name)
            sqs.delete_queue(QueueUrl=queue_url)

    def test_onfailure_destination_sqs(self, lam, sqs, role):
        """Failed async invocation routes to OnFailure SQS destination."""
        fn_name = _unique("dest-fail-fn")
        queue_name = _unique("dest-fail-q")

        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}"

        code = _make_zip('def handler(event, ctx): raise ValueError("dest failure")')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.put_function_event_invoke_config(
            FunctionName=fn_name,
            DestinationConfig={"OnFailure": {"Destination": queue_arn}},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"fail": True}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url)
            assert len(msgs) >= 1, "Expected OnFailure destination message"

            body = json.loads(msgs[0]["Body"])
            assert body["requestContext"]["condition"] == "RetriesExhausted"
            assert body["requestPayload"] == {"fail": True}
            assert "functionError" in body["responseContext"]
        finally:
            lam.delete_function(FunctionName=fn_name)
            sqs.delete_queue(QueueUrl=queue_url)

    def test_onsuccess_destination_sns(self, lam, sqs, sns, role):
        """Successful async invocation routes to OnSuccess SNS destination."""
        fn_name = _unique("dest-sns-fn")
        topic_name = _unique("dest-sns-topic")
        sub_queue_name = _unique("dest-sns-sub-q")

        topic = sns.create_topic(Name=topic_name)
        topic_arn = topic["TopicArn"]
        q = sqs.create_queue(QueueName=sub_queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{sub_queue_name}"
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        code = _make_zip('def handler(event, ctx): return {"status": "success"}')
        lam.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        lam.put_function_event_invoke_config(
            FunctionName=fn_name,
            DestinationConfig={"OnSuccess": {"Destination": topic_arn}},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_name,
                InvocationType="Event",
                Payload=json.dumps({"data": "sns-dest"}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url)
            assert len(msgs) >= 1, "Expected SNS destination message via subscription"

            envelope = json.loads(msgs[0]["Body"])
            assert envelope["Type"] == "Notification"
            inner = json.loads(envelope["Message"])
            assert inner["requestContext"]["condition"] == "Success"
            assert inner["responsePayload"] == {"status": "success"}
        finally:
            lam.delete_function(FunctionName=fn_name)
            sqs.delete_queue(QueueUrl=queue_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_onsuccess_destination_lambda(self, lam, sqs, role):
        """Successful async invocation of function A triggers function B."""
        fn_a_name = _unique("dest-src-fn")
        fn_b_name = _unique("dest-tgt-fn")
        proof_queue_name = _unique("dest-l2l-proof")

        q = sqs.create_queue(QueueName=proof_queue_name)
        queue_url = q["QueueUrl"]

        # Target function B: writes a proof record to SQS (using internal store)
        code_b = (
            "import json, hashlib, uuid\n"
            "def handler(event, ctx):\n"
            "    from robotocore.services.sqs.provider import _get_store\n"
            "    from robotocore.services.sqs.models import SqsMessage\n"
            '    store = _get_store("us-east-1", "123456789012")\n'
            f'    queue = store.get_queue("{proof_queue_name}")\n'
            "    if queue:\n"
            "        cond = event.get('requestContext', {}).get('condition')\n"
            '        body = json.dumps({"from_b": True, "condition": cond})\n'
            "        md5 = hashlib.md5(body.encode()).hexdigest()\n"
            "        msg = SqsMessage(\n"
            "            message_id=str(uuid.uuid4()),\n"
            "            body=body, md5_of_body=md5)\n"
            "        queue.put(msg)\n"
            '    return {"processed": True}\n'
        )
        fn_b_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn_b_name}"

        lam.create_function(
            FunctionName=fn_b_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": _make_zip(code_b)},
        )

        # Source function A: succeeds
        code_a = 'def handler(event, ctx): return {"from_a": True}'
        lam.create_function(
            FunctionName=fn_a_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": _make_zip(code_a)},
        )

        lam.put_function_event_invoke_config(
            FunctionName=fn_a_name,
            DestinationConfig={"OnSuccess": {"Destination": fn_b_arn}},
        )

        try:
            resp = lam.invoke(
                FunctionName=fn_a_name,
                InvocationType="Event",
                Payload=json.dumps({"trigger": "l2l"}),
            )
            assert resp["StatusCode"] == 202

            msgs = _poll_sqs(sqs, queue_url, timeout=10)
            assert len(msgs) >= 1, "Expected proof message from target Lambda B"

            body = json.loads(msgs[0]["Body"])
            assert body["from_b"] is True
            assert body["condition"] == "Success"
        finally:
            lam.delete_function(FunctionName=fn_a_name)
            lam.delete_function(FunctionName=fn_b_name)
            sqs.delete_queue(QueueUrl=queue_url)
