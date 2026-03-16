"""Concurrent request stress tests -- verify robotocore handles parallel requests without
race conditions, data corruption, or crashes."""

import io
import json
import tempfile
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uid(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _lambda_role_arn() -> str:
    """Create a minimal IAM role for Lambda and return its ARN."""
    iam = make_client("iam")
    role_name = _uid("concurrent-role-")
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
    resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
    return resp["Role"]["Arn"], role_name


# ---------------------------------------------------------------------------
# 1. Concurrent S3 puts to same bucket
# ---------------------------------------------------------------------------


class TestConcurrentS3Puts:
    def test_concurrent_puts_all_objects_present(self):
        """10 threads put different objects to the same bucket; all must be readable after."""
        s3 = make_client("s3")
        bucket = _uid("conc-s3-")
        s3.create_bucket(Bucket=bucket)
        num_objects = 10

        def put_object(i: int) -> str:
            key = f"obj-{i}"
            s3.put_object(Bucket=bucket, Key=key, Body=f"data-{i}")
            return key

        try:
            with ThreadPoolExecutor(max_workers=num_objects) as pool:
                futures = [pool.submit(put_object, i) for i in range(num_objects)]
                keys = [f.result() for f in as_completed(futures)]

            assert len(keys) == num_objects

            # Verify all objects exist and have correct content
            listed = s3.list_objects_v2(Bucket=bucket)
            listed_keys = {obj["Key"] for obj in listed.get("Contents", [])}
            for i in range(num_objects):
                assert f"obj-{i}" in listed_keys, f"obj-{i} missing after concurrent puts"

            # Spot-check content integrity
            body = s3.get_object(Bucket=bucket, Key="obj-0")["Body"].read().decode()
            assert body == "data-0"
        finally:
            # Cleanup
            for i in range(num_objects):
                try:
                    s3.delete_object(Bucket=bucket, Key=f"obj-{i}")
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)


# ---------------------------------------------------------------------------
# 2. Concurrent SQS sends + receives
# ---------------------------------------------------------------------------


class TestConcurrentSQSSendReceive:
    def test_concurrent_send_and_receive(self):
        """5 threads send messages, 5 threads receive from same queue.
        Total received (after draining) must equal total sent."""
        sqs = make_client("sqs")
        queue_name = _uid("conc-sqs-")
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        num_senders = 5
        msgs_per_sender = 4  # 20 total messages
        total_expected = num_senders * msgs_per_sender

        sent_ids: list[str] = []

        def send_batch(sender_id: int) -> list[str]:
            ids = []
            for j in range(msgs_per_sender):
                resp = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({"sender": sender_id, "seq": j}),
                )
                ids.append(resp["MessageId"])
            return ids

        try:
            # Phase 1: concurrent sends
            with ThreadPoolExecutor(max_workers=num_senders) as pool:
                futures = [pool.submit(send_batch, i) for i in range(num_senders)]
                for f in as_completed(futures):
                    sent_ids.extend(f.result())

            assert len(sent_ids) == total_expected

            # Phase 2: concurrent receives -- drain the queue
            received_bodies: list[str] = []

            def receive_batch() -> list[str]:
                bodies = []
                for _ in range(10):  # multiple attempts to drain
                    resp = sqs.receive_message(
                        QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
                    )
                    msgs = resp.get("Messages", [])
                    if not msgs:
                        break
                    for m in msgs:
                        bodies.append(m["Body"])
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                return bodies

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(receive_batch) for _ in range(5)]
                for f in as_completed(futures):
                    received_bodies.extend(f.result())

            # All messages should have been received (possibly by different threads)
            assert len(received_bodies) == total_expected, (
                f"Expected {total_expected} messages, got {len(received_bodies)}"
            )

            # Verify each message body is valid JSON with sender/seq
            for body in received_bodies:
                parsed = json.loads(body)
                assert "sender" in parsed
                assert "seq" in parsed
        finally:
            sqs.delete_queue(QueueUrl=queue_url)


# ---------------------------------------------------------------------------
# 3. Concurrent DynamoDB writes
# ---------------------------------------------------------------------------


class TestConcurrentDynamoDBWrites:
    def test_concurrent_put_items_all_present(self):
        """10 threads write different items to same table; all must be present after."""
        ddb = make_client("dynamodb")
        table_name = _uid("conc-ddb-")
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        num_items = 10

        def put_item(i: int) -> str:
            pk = f"item-{i}"
            ddb.put_item(
                TableName=table_name,
                Item={"pk": {"S": pk}, "value": {"N": str(i)}},
            )
            return pk

        try:
            with ThreadPoolExecutor(max_workers=num_items) as pool:
                futures = [pool.submit(put_item, i) for i in range(num_items)]
                pks = [f.result() for f in as_completed(futures)]

            assert len(pks) == num_items

            # Scan to verify all items
            scan = ddb.scan(TableName=table_name)
            found_pks = {item["pk"]["S"] for item in scan["Items"]}
            for i in range(num_items):
                assert f"item-{i}" in found_pks, f"item-{i} missing after concurrent writes"

            assert scan["Count"] == num_items
        finally:
            ddb.delete_table(TableName=table_name)


# ---------------------------------------------------------------------------
# 4. Concurrent Lambda invocations
# ---------------------------------------------------------------------------


class TestConcurrentLambdaInvocations:
    def test_concurrent_invocations_all_succeed(self):
        """5 simultaneous invocations of the same function; all must return correctly."""
        lam = make_client("lambda")
        role_arn, role_name = _lambda_role_arn()
        func_name = _uid("conc-lambda-")

        code = _make_zip(
            "def handler(event, ctx):\n"
            "    return {'statusCode': 200, 'body': f'hello {event.get(\"id\", \"?\")}'}\n"
        )

        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        num_invocations = 5

        def invoke(i: int) -> dict:
            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({"id": i}),
            )
            payload = json.loads(resp["Payload"].read())
            return payload

        try:
            with ThreadPoolExecutor(max_workers=num_invocations) as pool:
                futures = {pool.submit(invoke, i): i for i in range(num_invocations)}
                results = {}
                for f in as_completed(futures):
                    i = futures[f]
                    results[i] = f.result()

            assert len(results) == num_invocations
            for i in range(num_invocations):
                assert results[i]["statusCode"] == 200
                assert str(i) in results[i]["body"]
        finally:
            lam.delete_function(FunctionName=func_name)
            iam = make_client("iam")
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# 5. Cross-service concurrent requests
# ---------------------------------------------------------------------------


class TestCrossServiceConcurrent:
    def test_s3_dynamodb_sqs_simultaneously(self):
        """Thread A creates S3 objects, Thread B writes DynamoDB items,
        Thread C sends SQS messages -- all at the same time. No crashes
        or data corruption."""
        s3 = make_client("s3")
        ddb = make_client("dynamodb")
        sqs = make_client("sqs")

        bucket = _uid("cross-s3-")
        table_name = _uid("cross-ddb-")
        queue_name = _uid("cross-sqs-")

        # Setup
        s3.create_bucket(Bucket=bucket)
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]

        n = 5  # operations per service

        errors: list[str] = []

        def s3_work():
            try:
                for i in range(n):
                    s3.put_object(Bucket=bucket, Key=f"cross-{i}", Body=f"s3-{i}")
            except Exception as e:
                errors.append(f"S3 error: {e}")

        def ddb_work():
            try:
                for i in range(n):
                    ddb.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"cross-{i}"}, "v": {"S": f"ddb-{i}"}},
                    )
            except Exception as e:
                errors.append(f"DynamoDB error: {e}")

        def sqs_work():
            try:
                for i in range(n):
                    sqs.send_message(QueueUrl=queue_url, MessageBody=f"sqs-{i}")
            except Exception as e:
                errors.append(f"SQS error: {e}")

        try:
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [
                    pool.submit(s3_work),
                    pool.submit(ddb_work),
                    pool.submit(sqs_work),
                ]
                for f in as_completed(futures):
                    f.result()  # re-raise any exception

            assert not errors, f"Cross-service errors: {errors}"

            # Verify S3
            listed = s3.list_objects_v2(Bucket=bucket)
            assert listed["KeyCount"] == n

            # Verify DynamoDB
            scan = ddb.scan(TableName=table_name)
            assert scan["Count"] == n

            # Verify SQS (count via attributes)
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            approx = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
            assert approx == n, f"Expected {n} SQS messages, got {approx}"
        finally:
            for i in range(n):
                try:
                    s3.delete_object(Bucket=bucket, Key=f"cross-{i}")
                except Exception:
                    pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
            try:
                ddb.delete_table(TableName=table_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                sqs.delete_queue(QueueUrl=queue_url)
            except Exception:
                pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# 6. Rapid create/delete cycle
# ---------------------------------------------------------------------------


class TestRapidCreateDeleteCycle:
    def test_s3_bucket_create_delete_cycle(self):
        """Create and immediately delete the same bucket name 10 times.
        Final state must be clean (bucket does not exist)."""
        s3 = make_client("s3")
        bucket = _uid("cycle-")
        cycles = 10

        for i in range(cycles):
            s3.create_bucket(Bucket=bucket)
            s3.delete_bucket(Bucket=bucket)

        # Verify bucket is gone
        buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        assert bucket not in buckets

    def test_sqs_queue_create_delete_cycle(self):
        """Create and immediately delete a queue 10 times.
        Each cycle uses a unique name to avoid QueueDeletedRecently."""
        sqs = make_client("sqs")
        cycles = 10

        for i in range(cycles):
            name = _uid(f"cycle-{i}-")
            url = sqs.create_queue(QueueName=name)["QueueUrl"]
            sqs.delete_queue(QueueUrl=url)

        # Verify no leftover queues with our prefix pattern
        resp = sqs.list_queues()
        urls = resp.get("QueueUrls", [])
        cycle_urls = [u for u in urls if "cycle-" in u]
        assert len(cycle_urls) == 0, f"Leftover queues: {cycle_urls}"

    def test_dynamodb_table_create_delete_cycle(self):
        """Create and delete same table name 5 times."""
        ddb = make_client("dynamodb")
        table_name = _uid("cycle-tbl-")
        cycles = 5

        for _ in range(cycles):
            ddb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            ddb.delete_table(TableName=table_name)

        tables = ddb.list_tables()["TableNames"]
        assert table_name not in tables


# ---------------------------------------------------------------------------
# 7. Concurrent state save
# ---------------------------------------------------------------------------


class TestConcurrentStateSave:
    def test_two_concurrent_saves_no_crash(self):
        """Two threads save state simultaneously. Both should succeed
        (or one wins gracefully) -- no crash or 500."""
        state_dir = tempfile.mkdtemp(prefix="robotocore-conc-state-")

        def save_state(name: str) -> int:
            resp = requests.post(
                f"{ENDPOINT_URL}/_robotocore/state/save",
                json={"name": name, "path": state_dir},
                timeout=10,
            )
            return resp.status_code

        name_a = _uid("snap-a-")
        name_b = _uid("snap-b-")

        with ThreadPoolExecutor(max_workers=2) as pool:
            fa = pool.submit(save_state, name_a)
            fb = pool.submit(save_state, name_b)
            status_a = fa.result()
            status_b = fb.result()

        # Both should succeed (200) -- no 500 errors
        assert status_a == 200, f"State save A returned {status_a}"
        assert status_b == 200, f"State save B returned {status_b}"


# ---------------------------------------------------------------------------
# 8. Concurrent API Gateway requests
# ---------------------------------------------------------------------------


class TestConcurrentAPIGatewayRequests:
    def test_multiple_simultaneous_api_calls(self):
        """Create an API Gateway REST API with a Lambda backend, then
        hit it with 5 concurrent HTTP requests."""
        apigw = make_client("apigateway")
        lam = make_client("lambda")
        role_arn, role_name = _lambda_role_arn()

        func_name = _uid("apigw-func-")
        api_name = _uid("apigw-api-")

        # Create Lambda
        code = _make_zip("def handler(event, ctx):\n    return {'statusCode': 200, 'body': 'ok'}\n")
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        func_arn = func_resp["FunctionArn"]

        # Create API Gateway
        api = apigw.create_rest_api(name=api_name, description="concurrent test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = resources["items"][0]["id"]

        # Create /test resource
        resource = apigw.create_resource(restApiId=api_id, parentId=root_id, pathPart="test")
        resource_id = resource["id"]

        # PUT method + integration
        apigw.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=f"arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/{func_arn}/invocations",
        )

        # Deploy
        apigw.create_deployment(restApiId=api_id, stageName="test")

        invoke_url = f"{ENDPOINT_URL}/restapis/{api_id}/test/_user_request_/test"
        num_requests = 5

        def hit_api(i: int) -> int:
            resp = requests.get(invoke_url, timeout=10)
            return resp.status_code

        try:
            with ThreadPoolExecutor(max_workers=num_requests) as pool:
                futures = [pool.submit(hit_api, i) for i in range(num_requests)]
                statuses = [f.result() for f in as_completed(futures)]

            assert len(statuses) == num_requests
            # All should be 200 (success) -- no 500s from race conditions
            for status in statuses:
                assert status == 200, f"API Gateway returned {status}, expected 200"
        finally:
            try:
                apigw.delete_rest_api(restApiId=api_id)
            except Exception:
                pass  # best-effort cleanup
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass  # best-effort cleanup
            iam = make_client("iam")
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass  # best-effort cleanup
