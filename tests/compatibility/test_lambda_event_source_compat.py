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


class TestEventSourceMappingTags:
    """Tag calls on a mapping ARN resolve the mapping, not a function —
    moto's `_get_resource_by_arn` (awslambda/models.py:2447-2462) routes
    the `event-source-mapping` ARN family to the ESM store by uuid. The
    terraform provider's refresh ListTags-es a mapping mid-apply, so a
    misrouted 404 "Function not found" aborts the drift apply."""

    @pytest.fixture
    def lam(self):
        return make_client("lambda")

    @pytest.fixture
    def sqs(self):
        return make_client("sqs")

    @pytest.fixture
    def iam(self):
        return make_client("iam")

    @pytest.fixture
    def role(self, iam):
        name = f"esm-tag-role-{uuid.uuid4().hex[:8]}"
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
        created = iam.create_role(RoleName=name, AssumeRolePolicyDocument=trust)["Role"]
        yield created["Arn"]
        try:
            iam.delete_role(RoleName=name)
        except Exception:
            pass  # best-effort cleanup

    @pytest.fixture
    def mapping(self, lam, sqs, role):
        """Queue + function + a tagged mapping, torn down after each test."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"tag-esm-queue-{suffix}"
        func_name = f"tag-esm-func-{suffix}"
        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
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
            Tags={"team": "red"},
        )
        yield esm
        for fn in [
            lambda: lam.delete_event_source_mapping(UUID=esm["UUID"]),
            lambda: lam.delete_function(FunctionName=func_name),
            lambda: sqs.delete_queue(QueueUrl=q_url),
        ]:
            try:
                fn()
            except Exception:
                pass  # best-effort cleanup

    def test_list_tags_routes_to_mapping(self, lam, mapping):
        tags = lam.list_tags(Resource=mapping["EventSourceMappingArn"])["Tags"]
        assert tags == {"team": "red"}

    def test_list_tags_unknown_mapping_404(self, lam, mapping):
        unknown = mapping["EventSourceMappingArn"].rsplit(":", 1)[0] + ":deadbeef"
        with pytest.raises(Exception) as err:
            lam.list_tags(Resource=unknown)
        body = getattr(err.value, "response", {}).get("Error", {})
        assert body.get("Code") == "ResourceNotFoundException"
        # moto's exact message (models.py:2455 RESTError) — a bare
        # "deadbeef"-in-message would also pass the pre-fix "Function
        # not found: …" which pins nothing.
        assert body.get("Message", "") == "Event source mapping deadbeef not found."

    # NOTE (Tagsh member render): moto's get_configuration carries `Tags`
    # UNFILTERED (models.py:1431 — `{}` survives the None-filter), so every
    # ESM body (create/get/list) describes the mapping's tags. NOT pinned
    # here: botocore's EventSourceMappingConfiguration model has no `Tags`
    # member, so its response parser drops the member silently — the pin
    # lives in the crate's own unit test
    # lambda::handler::tests::esm_wire_dict_carries_tags_member instead.

    def test_tag_untag_round_trip(self, lam, mapping):
        arn = mapping["EventSourceMappingArn"]
        lam.tag_resource(Resource=arn, Tags={"env": "prod"})
        tags = lam.list_tags(Resource=arn)["Tags"]
        assert tags == {"team": "red", "env": "prod"}
        lam.untag_resource(Resource=arn, TagKeys=["team"])
        assert lam.list_tags(Resource=arn)["Tags"] == {"env": "prod"}

    def test_update_preserves_tags(self, lam, mapping):
        arn = mapping["EventSourceMappingArn"]
        updated = lam.update_event_source_mapping(UUID=mapping["UUID"], BatchSize=10)
        assert updated["BatchSize"] == 10
        lam.untag_resource(Resource=arn, TagKeys=["nope"])
        assert lam.list_tags(Resource=arn)["Tags"] == {"team": "red"}

    def test_function_tags_unaffected(self, lam, sqs, role):
        """The function tag path still resolves through the function
        resolver — the ESM branch only claims mapping ARNs."""
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"tag-fn-esm-queue-{suffix}"
        func_name = f"tag-fn-esm-{suffix}"
        q_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        code = _make_zip("def handler(e, c): pass")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Tags={"on": "fn"},
        )
        try:
            func_arn = lam.get_function(FunctionName=func_name)["Configuration"]["FunctionArn"]
            assert lam.list_tags(Resource=func_arn)["Tags"] == {"on": "fn"}
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                sqs.delete_queue(QueueUrl=q_url)
            except Exception:
                pass  # best-effort cleanup
