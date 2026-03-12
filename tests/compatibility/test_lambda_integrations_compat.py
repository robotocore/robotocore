"""Lambda cross-service integration compatibility tests.

Tests Lambda functions that call back to the Robotocore emulator to interact
with other AWS services (S3, DynamoDB, SQS, SNS, Secrets Manager, Lambda).
Each test creates a Lambda function whose code uses boto3 to call another
service via endpoint_url="http://localhost:4566", invokes the function, and
verifies the side effects.
"""

import io
import json
import time
import uuid
import zipfile

import requests

from tests.compatibility.conftest import ENDPOINT_URL, make_client


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _uid(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:8]}"


def _create_role():
    """Create IAM role for Lambda and return (iam_client, role_name, role_arn)."""
    iam = make_client("iam")
    role_name = f"li-role-{uuid.uuid4().hex[:8]}"
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
    return iam, role_name, resp["Role"]["Arn"]


# ---------------------------------------------------------------------------
# 1. Lambda -> S3: function creates bucket and puts object
# ---------------------------------------------------------------------------


class TestLambdaToS3:
    """Lambda function writes an object to S3, verify it exists after invocation."""

    def test_lambda_puts_object_to_s3(self):
        lam = make_client("lambda")
        s3 = make_client("s3")
        iam, role_name, role_arn = _create_role()

        bucket = _uid("li-s3-bkt-")
        func_name = _uid("li-s3-fn-")
        key = "lambda-output.json"

        handler_code = f"""
import json, os, boto3
def handler(event, context):
    s3 = boto3.client("s3",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    s3.put_object(
        Bucket="{bucket}",
        Key="{key}",
        Body=json.dumps({{"source": "lambda", "data": event}}).encode())
    return {{"statusCode": 200, "bucket": "{bucket}", "key": "{key}"}}
"""
        try:
            s3.create_bucket(Bucket=bucket)
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({"test_key": "test_value"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["bucket"] == bucket

            # Verify S3 object
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = json.loads(obj["Body"].read())
            assert body["source"] == "lambda"
            assert body["data"]["test_key"] == "test_value"
        finally:
            try:
                s3.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 2. Lambda -> DynamoDB: function writes item to table
# ---------------------------------------------------------------------------


class TestLambdaToDynamoDB:
    """Lambda function writes an item to DynamoDB, verify it exists."""

    def test_lambda_puts_item_to_dynamodb(self):
        lam = make_client("lambda")
        ddb = make_client("dynamodb")
        iam, role_name, role_arn = _create_role()

        table_name = _uid("li-ddb-tbl-")
        func_name = _uid("li-ddb-fn-")

        handler_code = f"""
import json, os, boto3
def handler(event, context):
    ddb = boto3.client("dynamodb",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    ddb.put_item(
        TableName="{table_name}",
        Item={{
            "pk": {{"S": event["id"]}},
            "data": {{"S": json.dumps(event)}},
        }})
    return {{"statusCode": 200, "id": event["id"]}}
"""
        try:
            ddb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            test_id = _uid("item-")
            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({"id": test_id, "msg": "hello"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["id"] == test_id

            # Verify DynamoDB item
            item_resp = ddb.get_item(TableName=table_name, Key={"pk": {"S": test_id}})
            assert "Item" in item_resp
            assert item_resp["Item"]["pk"]["S"] == test_id
            data = json.loads(item_resp["Item"]["data"]["S"])
            assert data["msg"] == "hello"
        finally:
            try:
                ddb.delete_table(TableName=table_name)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 3. Lambda -> SQS: function sends message to queue
# ---------------------------------------------------------------------------


class TestLambdaToSQS:
    """Lambda function sends a message to SQS, verify it's receivable."""

    def test_lambda_sends_message_to_sqs(self):
        lam = make_client("lambda")
        sqs = make_client("sqs")
        iam, role_name, role_arn = _create_role()

        queue_name = _uid("li-sqs-q-")
        func_name = _uid("li-sqs-fn-")

        handler_code = """\
import json, os, boto3
def handler(event, context):
    sqs = boto3.client("sqs",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    queue_url = os.environ["QUEUE_URL"]
    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({"from_lambda": True, "data": event}))
    return {"statusCode": 200, "queue": queue_url}
"""
        queue_url = None
        try:
            q = sqs.create_queue(QueueName=queue_name)
            queue_url = q["QueueUrl"]

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={
                    "Variables": {
                        "AWS_ENDPOINT_URL": ENDPOINT_URL,
                        "QUEUE_URL": queue_url,
                    }
                },
            )

            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({"action": "enqueue", "item": 42}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200

            # Verify SQS message
            recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
            msgs = recv.get("Messages", [])
            assert len(msgs) >= 1, "Lambda should have sent a message to SQS"
            body = json.loads(msgs[0]["Body"])
            assert body["from_lambda"] is True
            assert body["data"]["item"] == 42
        finally:
            try:
                sqs.delete_queue(QueueUrl=queue_url)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 4. Lambda -> Lambda: function invokes another Lambda
# ---------------------------------------------------------------------------


class TestLambdaToLambda:
    """Lambda function invokes another Lambda, returns combined result."""

    def test_lambda_invokes_lambda(self):
        lam = make_client("lambda")
        iam, role_name, role_arn = _create_role()

        inner_name = _uid("li-inner-fn-")
        outer_name = _uid("li-outer-fn-")

        inner_code = """
import json
def handler(event, context):
    return {"inner_result": event.get("x", 0) * 2}
"""

        outer_code = f"""
import json, os, boto3
def handler(event, context):
    lam = boto3.client("lambda",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    resp = lam.invoke(
        FunctionName="{inner_name}",
        Payload=json.dumps({{"x": event.get("value", 5)}}))
    inner = json.loads(resp["Payload"].read())
    return {{"outer": True, "inner": inner}}
"""
        try:
            lam.create_function(
                FunctionName=inner_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(inner_code)},
            )
            lam.create_function(
                FunctionName=outer_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(outer_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            resp = lam.invoke(
                FunctionName=outer_name,
                Payload=json.dumps({"value": 7}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["outer"] is True
            assert payload["inner"]["inner_result"] == 14
        finally:
            try:
                lam.delete_function(FunctionName=inner_name)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=outer_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 5. Lambda -> SNS: function publishes to topic
# ---------------------------------------------------------------------------


class TestLambdaToSNS:
    """Lambda function publishes a message to SNS topic."""

    def test_lambda_publishes_to_sns(self):
        lam = make_client("lambda")
        sns = make_client("sns")
        sqs = make_client("sqs")
        iam, role_name, role_arn = _create_role()

        topic_name = _uid("li-sns-topic-")
        queue_name = _uid("li-sns-q-")
        func_name = _uid("li-sns-fn-")

        queue_url = None
        topic_arn = None
        try:
            # Create SNS topic and SQS subscriber so we can verify delivery
            topic = sns.create_topic(Name=topic_name)
            topic_arn = topic["TopicArn"]

            q = sqs.create_queue(QueueName=queue_name)
            queue_url = q["QueueUrl"]
            q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
            queue_arn = q_attrs["Attributes"]["QueueArn"]

            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

            handler_code = f"""
import json, os, boto3
def handler(event, context):
    sns = boto3.client("sns",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    sns.publish(
        TopicArn="{topic_arn}",
        Message=json.dumps({{"from_lambda": True, "data": event}}),
        Subject="Lambda Test")
    return {{"statusCode": 200, "published": True}}
"""

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({"msg": "sns-test"}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["published"] is True

            # Verify message arrived in SQS via SNS
            time.sleep(1)
            recv = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
            msgs = recv.get("Messages", [])
            assert len(msgs) >= 1, "SNS message published by Lambda should reach SQS subscriber"
            envelope = json.loads(msgs[0]["Body"])
            assert "Message" in envelope
            inner = json.loads(envelope["Message"])
            assert inner["from_lambda"] is True
        finally:
            try:
                sns.delete_topic(TopicArn=topic_arn)
            except Exception:
                pass
            try:
                sqs.delete_queue(QueueUrl=queue_url)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 6. Lambda -> Secrets Manager: function reads a secret
# ---------------------------------------------------------------------------


class TestLambdaToSecretsManager:
    """Lambda function reads a secret from Secrets Manager."""

    def test_lambda_reads_secret(self):
        lam = make_client("lambda")
        sm = make_client("secretsmanager")
        iam, role_name, role_arn = _create_role()

        secret_name = _uid("li-secret-")
        func_name = _uid("li-sm-fn-")
        secret_value = json.dumps({"db_host": "localhost", "db_pass": "s3cret"})

        try:
            sm.create_secret(Name=secret_name, SecretString=secret_value)

            handler_code = f"""
import json, os, boto3
def handler(event, context):
    sm = boto3.client("secretsmanager",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    resp = sm.get_secret_value(SecretId="{secret_name}")
    secret = json.loads(resp["SecretString"])
    return {{"statusCode": 200, "db_host": secret["db_host"]}}
"""

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["db_host"] == "localhost"
        finally:
            try:
                sm.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 7. API Gateway -> Lambda: REST API with Lambda proxy integration
# ---------------------------------------------------------------------------


class TestAPIGatewayToLambda:
    """Create REST API with Lambda proxy integration, invoke via HTTP."""

    def test_apigateway_lambda_proxy_integration(self):
        apigw = make_client("apigateway")
        lam = make_client("lambda")
        iam, role_name, role_arn = _create_role()

        func_name = _uid("li-apigw-fn-")
        api_name = _uid("li-api-")
        rest_api_id = None

        handler_code = """
import json
def handler(event, context):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"message": "Hello from Lambda", "path": event.get("path", "/")}),
    }
"""
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
            )
            func_arn = lam.get_function(FunctionName=func_name)["Configuration"]["FunctionArn"]

            # Create REST API
            api = apigw.create_rest_api(name=api_name, description="Lambda integration test")
            rest_api_id = api["id"]

            resources = apigw.get_resources(restApiId=rest_api_id)
            root_id = resources["items"][0]["id"]

            resource = apigw.create_resource(
                restApiId=rest_api_id, parentId=root_id, pathPart="hello"
            )
            resource_id = resource["id"]

            apigw.put_method(
                restApiId=rest_api_id,
                resourceId=resource_id,
                httpMethod="GET",
                authorizationType="NONE",
            )

            apigw.put_integration(
                restApiId=rest_api_id,
                resourceId=resource_id,
                httpMethod="GET",
                type="AWS_PROXY",
                integrationHttpMethod="POST",
                uri=(
                    f"arn:aws:apigateway:us-east-1:lambda:path"
                    f"/2015-03-31/functions/{func_arn}/invocations"
                ),
            )

            apigw.create_deployment(restApiId=rest_api_id, stageName="test")

            # Invoke via HTTP through API Gateway
            url = f"{ENDPOINT_URL}/restapis/{rest_api_id}/test/_user_request_/hello"
            http_resp = requests.get(url, timeout=10)
            # API Gateway proxy should return Lambda's response
            assert http_resp.status_code == 200
            body = http_resp.json()
            assert body["message"] == "Hello from Lambda"
        finally:
            try:
                if rest_api_id:
                    apigw.delete_rest_api(restApiId=rest_api_id)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 8. Lambda versioning: publish version, create alias, invoke via alias
# ---------------------------------------------------------------------------


class TestLambdaVersioningIntegration:
    """Publish version, create alias, invoke via alias, verify CRUD works."""

    def test_publish_version_create_alias_invoke(self):
        lam = make_client("lambda")
        iam, role_name, role_arn = _create_role()

        func_name = _uid("li-ver-fn-")

        code = """
import json
def handler(event, context):
    return {"invoked": True, "input": event}
"""
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(code)},
            )

            # Publish version 1
            v1 = lam.publish_version(FunctionName=func_name, Description="version 1")
            assert v1["Version"] == "1"
            assert v1["Description"] == "version 1"

            # Update code to get a new version
            code_v2 = """
import json
def handler(event, context):
    return {"invoked": True, "v": 2, "input": event}
"""
            lam.update_function_code(FunctionName=func_name, ZipFile=_make_zip(code_v2))

            # Publish version 2
            v2 = lam.publish_version(FunctionName=func_name, Description="version 2")
            assert v2["Version"] == "2"

            # Create alias pointing to v1
            alias = lam.create_alias(FunctionName=func_name, Name="stable", FunctionVersion="1")
            assert alias["Name"] == "stable"
            assert alias["FunctionVersion"] == "1"

            # Invoke via alias -- should succeed
            resp = lam.invoke(
                FunctionName=func_name,
                Qualifier="stable",
                Payload=json.dumps({"test": True}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["invoked"] is True
            assert payload["input"]["test"] is True

            # Update alias to v2
            updated = lam.update_alias(FunctionName=func_name, Name="stable", FunctionVersion="2")
            assert updated["FunctionVersion"] == "2"

            # Get alias to verify update
            got = lam.get_alias(FunctionName=func_name, Name="stable")
            assert got["FunctionVersion"] == "2"

            # List versions
            versions = lam.list_versions_by_function(FunctionName=func_name)
            version_nums = [v["Version"] for v in versions["Versions"]]
            assert "1" in version_nums
            assert "2" in version_nums

            # List aliases
            aliases = lam.list_aliases(FunctionName=func_name)
            alias_names = [a["Name"] for a in aliases["Aliases"]]
            assert "stable" in alias_names
        finally:
            try:
                lam.delete_alias(FunctionName=func_name, Name="stable")
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 9. Lambda concurrency: set reserved concurrency
# ---------------------------------------------------------------------------


class TestLambdaConcurrencyIntegration:
    """Set reserved concurrency, verify it persists and can be read back."""

    def test_put_get_delete_concurrency(self):
        lam = make_client("lambda")
        iam, role_name, role_arn = _create_role()

        func_name = _uid("li-conc-fn-")
        code = "def handler(e, c): return 'ok'"

        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(code)},
            )

            # Set reserved concurrency
            put_resp = lam.put_function_concurrency(
                FunctionName=func_name, ReservedConcurrentExecutions=5
            )
            assert put_resp["ReservedConcurrentExecutions"] == 5

            # Get concurrency
            get_resp = lam.get_function_concurrency(FunctionName=func_name)
            assert get_resp["ReservedConcurrentExecutions"] == 5

            # Update concurrency
            put_resp2 = lam.put_function_concurrency(
                FunctionName=func_name, ReservedConcurrentExecutions=10
            )
            assert put_resp2["ReservedConcurrentExecutions"] == 10

            # Delete concurrency
            lam.delete_function_concurrency(FunctionName=func_name)
            get_resp2 = lam.get_function_concurrency(FunctionName=func_name)
            # After deletion, should be empty or zero
            assert (
                get_resp2.get("ReservedConcurrentExecutions", 0) == 0
                or "ReservedConcurrentExecutions" not in get_resp2
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 10. Lambda with VPC config (basic create/get)
# ---------------------------------------------------------------------------


class TestLambdaVPCConfig:
    """Create Lambda with VPC config, verify it shows in GetFunction."""

    def test_create_function_with_vpc_config(self):
        lam = make_client("lambda")
        ec2 = make_client("ec2")
        iam, role_name, role_arn = _create_role()

        func_name = _uid("li-vpc-fn-")
        code = "def handler(e, c): return 'ok'"

        try:
            # Create VPC and subnet
            vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
            vpc_id = vpc["Vpc"]["VpcId"]
            subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
            subnet_id = subnet["Subnet"]["SubnetId"]
            sg = ec2.create_security_group(
                GroupName=_uid("li-sg-"), Description="test", VpcId=vpc_id
            )
            sg_id = sg["GroupId"]

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(code)},
                VpcConfig={
                    "SubnetIds": [subnet_id],
                    "SecurityGroupIds": [sg_id],
                },
            )

            fn_resp = lam.get_function(FunctionName=func_name)
            vpc_cfg = fn_resp["Configuration"].get("VpcConfig", {})
            assert subnet_id in vpc_cfg.get("SubnetIds", [])
            assert sg_id in vpc_cfg.get("SecurityGroupIds", [])
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                ec2.delete_security_group(GroupId=sg_id)
            except Exception:
                pass
            try:
                ec2.delete_subnet(SubnetId=subnet_id)
            except Exception:
                pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 11. Lambda tags: tag function, list tags
# ---------------------------------------------------------------------------


class TestLambdaTagsIntegration:
    """Tag a Lambda function, verify tags via list_tags and tag_resource."""

    def test_tag_and_list_tags(self):
        lam = make_client("lambda")
        iam, role_name, role_arn = _create_role()

        func_name = _uid("li-tag-fn-")
        code = "def handler(e, c): return 'ok'"

        try:
            resp = lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(code)},
                Tags={"env": "test", "project": "robotocore"},
            )
            func_arn = resp["FunctionArn"]

            # List initial tags
            tags_resp = lam.list_tags(Resource=func_arn)
            assert tags_resp["Tags"]["env"] == "test"
            assert tags_resp["Tags"]["project"] == "robotocore"

            # Add more tags
            lam.tag_resource(Resource=func_arn, Tags={"version": "1.0"})

            tags_resp2 = lam.list_tags(Resource=func_arn)
            assert tags_resp2["Tags"]["version"] == "1.0"
            assert tags_resp2["Tags"]["env"] == "test"

            # Remove a tag
            lam.untag_resource(Resource=func_arn, TagKeys=["env"])

            tags_resp3 = lam.list_tags(Resource=func_arn)
            assert "env" not in tags_resp3["Tags"]
            assert tags_resp3["Tags"]["project"] == "robotocore"
            assert tags_resp3["Tags"]["version"] == "1.0"
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# 12. Lambda -> SSM Parameter Store: function reads a parameter
# ---------------------------------------------------------------------------


class TestLambdaToSSM:
    """Lambda function reads an SSM parameter."""

    def test_lambda_reads_ssm_parameter(self):
        lam = make_client("lambda")
        ssm = make_client("ssm")
        iam, role_name, role_arn = _create_role()

        param_name = f"/li/test/{_uid()}"
        func_name = _uid("li-ssm-fn-")

        try:
            ssm.put_parameter(Name=param_name, Value="config-value-123", Type="String")

            handler_code = f"""
import json, os, boto3
def handler(event, context):
    ssm = boto3.client("ssm",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing")
    resp = ssm.get_parameter(Name="{param_name}")
    return {{"statusCode": 200, "value": resp["Parameter"]["Value"]}}
"""

            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_zip(handler_code)},
                Timeout=30,
                Environment={"Variables": {"AWS_ENDPOINT_URL": ENDPOINT_URL}},
            )

            resp = lam.invoke(
                FunctionName=func_name,
                Payload=json.dumps({}),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            assert payload["value"] == "config-value-123"
        finally:
            try:
                ssm.delete_parameter(Name=param_name)
            except Exception:
                pass
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass
            try:
                iam.delete_role(RoleName=role_name)
            except Exception:
                pass
