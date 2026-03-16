"""End-to-end CloudFormation stack operation tests.

These tests verify that CFN stack operations create REAL resources
in the target AWS services -- not just that CFN API calls succeed.
Every test verifies the end state via the target service's own client.
"""

import json
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:4566"


def _client(service_name: str):
    config_kwargs = {}
    if service_name == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}
    return boto3.client(
        service_name,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(**config_kwargs),
    )


@pytest.fixture
def cfn():
    return _client("cloudformation")


@pytest.fixture
def s3():
    return _client("s3")


@pytest.fixture
def dynamodb():
    return _client("dynamodb")


@pytest.fixture
def sqs():
    return _client("sqs")


@pytest.fixture
def lam():
    return _client("lambda")


@pytest.fixture
def iam():
    return _client("iam")


@pytest.fixture
def logs():
    return _client("logs")


def _uid():
    return uuid.uuid4().hex[:8]


class TestCfnE2eS3:
    """Create stack with S3 bucket, verify bucket exists via S3 client."""

    def test_create_s3_bucket_and_verify(self, cfn, s3):
        uid = _uid()
        bucket_name = f"cfn-e2e-bucket-{uid}"
        stack_name = f"e2e-s3-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Bucket": {
                        "Type": "AWS::S3::Bucket",
                        "Properties": {"BucketName": bucket_name},
                    },
                },
                "Outputs": {
                    "BucketName": {"Value": {"Ref": "Bucket"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            # Verify stack succeeded
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify bucket exists via S3 client
            buckets = s3.list_buckets()
            bucket_names = [b["Name"] for b in buckets["Buckets"]]
            assert bucket_name in bucket_names

            # Verify we can interact with the bucket
            s3.put_object(Bucket=bucket_name, Key="test.txt", Body=b"hello")
            obj = s3.get_object(Bucket=bucket_name, Key="test.txt")
            assert obj["Body"].read() == b"hello"

            # Verify CFN output matches
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            assert outputs.get("BucketName") == bucket_name
        finally:
            # Clean up object before deleting stack
            try:
                s3.delete_object(Bucket=bucket_name, Key="test.txt")
            except Exception:
                pass  # best-effort cleanup
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eDynamoDB:
    """Create stack with DynamoDB table, verify table via DynamoDB client."""

    def test_create_dynamodb_table_and_verify(self, cfn, dynamodb):
        uid = _uid()
        table_name = f"cfn-e2e-table-{uid}"
        stack_name = f"e2e-ddb-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Table": {
                        "Type": "AWS::DynamoDB::Table",
                        "Properties": {
                            "TableName": table_name,
                            "AttributeDefinitions": [
                                {"AttributeName": "pk", "AttributeType": "S"},
                            ],
                            "KeySchema": [
                                {"AttributeName": "pk", "KeyType": "HASH"},
                            ],
                            "BillingMode": "PAY_PER_REQUEST",
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify table via DynamoDB client
            desc = dynamodb.describe_table(TableName=table_name)
            assert desc["Table"]["TableName"] == table_name
            assert desc["Table"]["KeySchema"][0]["AttributeName"] == "pk"

            # Verify we can write and read data
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "test-item"}, "data": {"S": "hello"}},
            )
            item = dynamodb.get_item(
                TableName=table_name,
                Key={"pk": {"S": "test-item"}},
            )
            assert item["Item"]["data"]["S"] == "hello"
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eSQS:
    """Create stack with SQS queue, verify queue via SQS client."""

    def test_create_sqs_queue_and_verify(self, cfn, sqs):
        uid = _uid()
        queue_name = f"cfn-e2e-queue-{uid}"
        stack_name = f"e2e-sqs-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": queue_name,
                            "VisibilityTimeout": 45,
                        },
                    },
                },
                "Outputs": {
                    "QueueUrl": {"Value": {"Ref": "Queue"}},
                    "QueueArn": {"Value": {"Fn::GetAtt": ["Queue", "Arn"]}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify queue via SQS client
            q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            assert queue_name in q_url

            # Verify queue attributes
            attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
            assert attrs["Attributes"]["VisibilityTimeout"] == "45"

            # Verify we can send/receive messages
            sqs.send_message(QueueUrl=q_url, MessageBody="e2e-test")
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert recv["Messages"][0]["Body"] == "e2e-test"
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eLambda:
    """Create stack with Lambda function + IAM role, verify via Lambda client."""

    def test_create_lambda_and_verify(self, cfn, lam):
        uid = _uid()
        stack_name = f"e2e-lam-{uid}"
        fn_name = f"cfn-e2e-fn-{uid}"
        role_name = f"cfn-e2e-role-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Role": {
                        "Type": "AWS::IAM::Role",
                        "Properties": {
                            "RoleName": role_name,
                            "AssumeRolePolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Principal": {"Service": "lambda.amazonaws.com"},
                                        "Action": "sts:AssumeRole",
                                    }
                                ],
                            },
                        },
                    },
                    "Fn": {
                        "Type": "AWS::Lambda::Function",
                        "Properties": {
                            "FunctionName": fn_name,
                            "Runtime": "python3.12",
                            "Handler": "index.handler",
                            "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                            "Code": {
                                "ZipFile": "def handler(event, context): return {'ok': True}",
                            },
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify Lambda function exists via Lambda client
            fn = lam.get_function(FunctionName=fn_name)
            assert fn["Configuration"]["FunctionName"] == fn_name
            assert fn["Configuration"]["Runtime"] == "python3.12"
            assert fn["Configuration"]["Handler"] == "index.handler"
            assert role_name in fn["Configuration"]["Role"]
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eUpdateStack:
    """Update stack and verify changes in target service."""

    def test_update_sqs_visibility_timeout(self, cfn, sqs):
        uid = _uid()
        queue_name = f"cfn-e2e-upd-{uid}"
        stack_name = f"e2e-upd-{uid}"

        def _template(vt: int) -> str:
            return json.dumps(
                {
                    "AWSTemplateFormatVersion": "2010-09-09",
                    "Resources": {
                        "Q": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {
                                "QueueName": queue_name,
                                "VisibilityTimeout": vt,
                            },
                        },
                    },
                }
            )

        # Create with VT=30
        cfn.create_stack(StackName=stack_name, TemplateBody=_template(30))
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "30"

        # Update to VT=60
        cfn.update_stack(StackName=stack_name, TemplateBody=_template(60))
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"

        # Verify updated attribute via SQS client
        q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "60"

        cfn.delete_stack(StackName=stack_name)


class TestCfnE2eDeleteCleanup:
    """Delete stack and verify resources are cleaned up in target services."""

    def test_delete_cleans_up_sqs(self, cfn, sqs):
        uid = _uid()
        queue_name = f"cfn-e2e-del-q-{uid}"
        stack_name = f"e2e-del-sqs-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify queue exists
        sqs.get_queue_url(QueueName=queue_name)

        # Delete stack
        cfn.delete_stack(StackName=stack_name)

        # Verify queue is gone
        with pytest.raises(ClientError) as exc_info:
            sqs.get_queue_url(QueueName=queue_name)
        assert (
            "NonExistentQueue" in str(exc_info.value)
            or "does not exist" in str(exc_info.value).lower()
        )

    def test_delete_cleans_up_dynamodb(self, cfn, dynamodb):
        uid = _uid()
        table_name = f"cfn-e2e-del-t-{uid}"
        stack_name = f"e2e-del-ddb-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Table": {
                        "Type": "AWS::DynamoDB::Table",
                        "Properties": {
                            "TableName": table_name,
                            "AttributeDefinitions": [
                                {"AttributeName": "pk", "AttributeType": "S"},
                            ],
                            "KeySchema": [
                                {"AttributeName": "pk", "KeyType": "HASH"},
                            ],
                            "BillingMode": "PAY_PER_REQUEST",
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify table exists
        dynamodb.describe_table(TableName=table_name)

        # Delete stack
        cfn.delete_stack(StackName=stack_name)

        # Verify table is gone
        with pytest.raises(ClientError) as exc_info:
            dynamodb.describe_table(TableName=table_name)
        assert (
            "ResourceNotFoundException" in str(exc_info.value)
            or "not found" in str(exc_info.value).lower()
        )


class TestCfnE2eStackOutputs:
    """Verify stack outputs with Fn::GetAtt and Ref."""

    def test_outputs_with_getatt_and_ref(self, cfn, sqs):
        uid = _uid()
        queue_name = f"cfn-e2e-out-{uid}"
        stack_name = f"e2e-out-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
                "Outputs": {
                    "QueueUrl": {
                        "Description": "URL of the queue",
                        "Value": {"Ref": "Queue"},
                    },
                    "QueueArn": {
                        "Description": "ARN of the queue",
                        "Value": {"Fn::GetAtt": ["Queue", "Arn"]},
                    },
                    "QueueName": {
                        "Description": "Name of the queue",
                        "Value": {"Fn::GetAtt": ["Queue", "QueueName"]},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            stack = resp["Stacks"][0]
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            outputs = {o["OutputKey"]: o for o in stack.get("Outputs", [])}

            # QueueUrl output should be a valid URL
            assert queue_name in outputs["QueueUrl"]["OutputValue"]
            assert outputs["QueueUrl"]["Description"] == "URL of the queue"

            # QueueArn should contain the queue name
            assert queue_name in outputs["QueueArn"]["OutputValue"]
            assert "arn:aws:sqs:" in outputs["QueueArn"]["OutputValue"]

            # QueueName output should be the queue name
            assert outputs["QueueName"]["OutputValue"] == queue_name

            # Verify the URL from outputs actually works
            q_url = outputs["QueueUrl"]["OutputValue"]
            sqs.send_message(QueueUrl=q_url, MessageBody="output-test")
            recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
            assert recv["Messages"][0]["Body"] == "output-test"
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eParameters:
    """Verify stack parameters are resolved into resources."""

    def test_parameter_used_in_resource(self, cfn, sqs):
        uid = _uid()
        stack_name = f"e2e-param-{uid}"
        queue_name = f"cfn-e2e-param-q-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "QName": {
                        "Type": "String",
                        "Description": "Queue name",
                    },
                    "VTimeout": {
                        "Type": "Number",
                        "Default": "30",
                        "Description": "Visibility timeout",
                    },
                },
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {"Ref": "QName"},
                            "VisibilityTimeout": {"Ref": "VTimeout"},
                        },
                    },
                },
                "Outputs": {
                    "QueueUrl": {"Value": {"Ref": "Queue"}},
                },
            }
        )
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Parameters=[
                {"ParameterKey": "QName", "ParameterValue": queue_name},
                {"ParameterKey": "VTimeout", "ParameterValue": "90"},
            ],
        )
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify parameter values are in describe_stacks
            params = {
                p["ParameterKey"]: p["ParameterValue"]
                for p in resp["Stacks"][0].get("Parameters", [])
            }
            assert params.get("QName") == queue_name
            assert params.get("VTimeout") == "90"

            # Verify queue was created with parameterized name
            q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            assert queue_name in q_url

            # Verify the visibility timeout was set from parameter
            attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
            assert attrs["Attributes"]["VisibilityTimeout"] == "90"
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eNestedReferences:
    """Verify Ref and Fn::GetAtt resolve correctly between resources."""

    def test_iam_role_referenced_by_lambda(self, cfn, lam, iam):
        uid = _uid()
        stack_name = f"e2e-ref-{uid}"
        role_name = f"cfn-e2e-ref-role-{uid}"
        fn_name = f"cfn-e2e-ref-fn-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Role": {
                        "Type": "AWS::IAM::Role",
                        "Properties": {
                            "RoleName": role_name,
                            "AssumeRolePolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Effect": "Allow",
                                        "Principal": {"Service": "lambda.amazonaws.com"},
                                        "Action": "sts:AssumeRole",
                                    }
                                ],
                            },
                        },
                    },
                    "Fn": {
                        "Type": "AWS::Lambda::Function",
                        "Properties": {
                            "FunctionName": fn_name,
                            "Runtime": "python3.12",
                            "Handler": "index.handler",
                            "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                            "Code": {
                                "ZipFile": "def handler(e, c): return 'ok'",
                            },
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify IAM role exists
            role_resp = iam.get_role(RoleName=role_name)
            role_arn = role_resp["Role"]["Arn"]
            assert role_name in role_arn

            # Verify Lambda references the correct role ARN
            fn_resp = lam.get_function(FunctionName=fn_name)
            assert fn_resp["Configuration"]["Role"] == role_arn
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eStackEvents:
    """Verify stack events are recorded during create."""

    def test_create_events_recorded(self, cfn):
        uid = _uid()
        stack_name = f"e2e-events-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-e2e-ev-{uid}"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_events(StackName=stack_name)
            events = resp["StackEvents"]

            # Should have at least: stack CREATE_IN_PROGRESS, resource events, stack CREATE_COMPLETE
            assert len(events) >= 2

            # All events should reference this stack
            for event in events:
                assert event["StackName"] == stack_name

            # Should have events with timestamps
            for event in events:
                assert "Timestamp" in event

            # Should have the stack-level CREATE_COMPLETE
            statuses = [e.get("ResourceStatus", "") for e in events]
            assert "CREATE_COMPLETE" in statuses

            # Should have resource-level events with resource types
            resource_events = [e for e in events if e.get("ResourceType") == "AWS::SQS::Queue"]
            assert len(resource_events) >= 1
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCfnE2eMultiService:
    """Stack with multiple resource types, verify all in their target services."""

    def test_sqs_and_dynamodb_together(self, cfn, sqs, dynamodb):
        uid = _uid()
        stack_name = f"e2e-multi-{uid}"
        queue_name = f"cfn-e2e-multi-q-{uid}"
        table_name = f"cfn-e2e-multi-t-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                    "Table": {
                        "Type": "AWS::DynamoDB::Table",
                        "Properties": {
                            "TableName": table_name,
                            "AttributeDefinitions": [
                                {"AttributeName": "id", "AttributeType": "S"},
                            ],
                            "KeySchema": [
                                {"AttributeName": "id", "KeyType": "HASH"},
                            ],
                            "BillingMode": "PAY_PER_REQUEST",
                        },
                    },
                },
                "Outputs": {
                    "QueueArn": {"Value": {"Fn::GetAtt": ["Queue", "Arn"]}},
                    "TableName": {"Value": {"Ref": "Table"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify SQS queue
            q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
            assert queue_name in q_url

            # Verify DynamoDB table
            desc = dynamodb.describe_table(TableName=table_name)
            assert desc["Table"]["TableName"] == table_name

            # Verify outputs
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            assert queue_name in outputs.get("QueueArn", "")
            assert outputs.get("TableName") == table_name
        finally:
            cfn.delete_stack(StackName=stack_name)
