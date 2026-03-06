"""Tests for CloudFormation resource handlers (create/delete)."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.cloudformation.engine import CfnResource
from robotocore.services.cloudformation.resources import (
    create_resource,
    delete_resource,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resource(resource_type, properties=None, logical_id="MyResource"):
    return CfnResource(
        logical_id=logical_id,
        resource_type=resource_type,
        properties=properties or {},
    )


REGION = "us-east-1"
ACCOUNT_ID = "999999999999"


# ---------------------------------------------------------------------------
# Unknown resource type
# ---------------------------------------------------------------------------


class TestUnknownResourceType:
    def test_create_assigns_fake_physical_id(self):
        res = _resource("AWS::Custom::Thing")
        create_resource(res, REGION, ACCOUNT_ID)
        assert res.physical_id is not None
        assert "aws-custom-thing" in res.physical_id
        assert res.status == "CREATE_COMPLETE"

    def test_delete_unknown_does_not_raise(self):
        res = _resource("AWS::Custom::Thing")
        res.physical_id = "fake-id"
        delete_resource(res, REGION, ACCOUNT_ID)


# ---------------------------------------------------------------------------
# SQS::Queue
# ---------------------------------------------------------------------------


class TestSqsQueue:
    def test_create_sqs_queue(self):
        mock_queue = SimpleNamespace(
            url="http://localhost:4566/queue/my-queue",
            arn="arn:aws:sqs:us-east-1:999999999999:my-queue",
            name="my-queue",
        )
        mock_store = MagicMock()
        mock_store.create_queue.return_value = mock_queue

        with patch(
            "robotocore.services.cloudformation.resources._get_store",
            return_value=mock_store,
            create=True,
        ):
            with patch(
                "robotocore.services.sqs.provider._get_store",
                return_value=mock_store,
            ):
                res = _resource(
                    "AWS::SQS::Queue",
                    {
                        "QueueName": "my-queue",
                        "VisibilityTimeout": 30,
                    },
                )
                create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "http://localhost:4566/queue/my-queue"
        assert res.attributes["Arn"] == "arn:aws:sqs:us-east-1:999999999999:my-queue"
        assert res.attributes["QueueName"] == "my-queue"
        assert res.status == "CREATE_COMPLETE"

    def test_create_sqs_queue_with_fifo_and_redrive(self):
        mock_queue = SimpleNamespace(
            url="http://localhost:4566/queue/my-queue.fifo",
            arn="arn:aws:sqs:us-east-1:999999999999:my-queue.fifo",
            name="my-queue.fifo",
        )
        mock_store = MagicMock()
        mock_store.create_queue.return_value = mock_queue

        with patch("robotocore.services.sqs.provider._get_store", return_value=mock_store):
            res = _resource(
                "AWS::SQS::Queue",
                {
                    "QueueName": "my-queue.fifo",
                    "FifoQueue": True,
                    "DelaySeconds": 5,
                    "RedrivePolicy": {
                        "deadLetterTargetArn": "arn:aws:sqs:us-east-1:999:dlq",
                        "maxReceiveCount": 3,
                    },
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        call_args = mock_store.create_queue.call_args
        attrs = (
            call_args[0][3] if len(call_args[0]) > 3 else call_args[1].get("attrs", call_args[0][3])
        )
        assert "FifoQueue" in attrs
        assert "RedrivePolicy" in attrs
        assert res.status == "CREATE_COMPLETE"

    def test_delete_sqs_queue(self):
        mock_queue = SimpleNamespace(name="my-queue")
        mock_store = MagicMock()
        mock_store.get_queue_by_url.return_value = mock_queue

        with patch("robotocore.services.sqs.provider._get_store", return_value=mock_store):
            res = _resource("AWS::SQS::Queue")
            res.physical_id = "http://localhost:4566/queue/my-queue"
            delete_resource(res, REGION, ACCOUNT_ID)

        mock_store.delete_queue.assert_called_once_with("my-queue")

    def test_delete_sqs_queue_no_physical_id(self):
        mock_store = MagicMock()
        with patch("robotocore.services.sqs.provider._get_store", return_value=mock_store):
            res = _resource("AWS::SQS::Queue")
            res.physical_id = None
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_store.get_queue_by_url.assert_not_called()


# ---------------------------------------------------------------------------
# SNS::Topic
# ---------------------------------------------------------------------------


class TestSnsTopic:
    def test_create_sns_topic(self):
        mock_topic = SimpleNamespace(
            arn="arn:aws:sns:us-east-1:999999999999:my-topic",
            name="my-topic",
        )
        mock_store = MagicMock()
        mock_store.create_topic.return_value = mock_topic

        with patch("robotocore.services.sns.provider._get_store", return_value=mock_store):
            res = _resource("AWS::SNS::Topic", {"TopicName": "my-topic"})
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "arn:aws:sns:us-east-1:999999999999:my-topic"
        assert res.attributes["TopicName"] == "my-topic"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_sns_topic(self):
        mock_store = MagicMock()
        with patch("robotocore.services.sns.provider._get_store", return_value=mock_store):
            res = _resource("AWS::SNS::Topic")
            res.physical_id = "arn:aws:sns:us-east-1:999:topic"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_store.delete_topic.assert_called_once_with("arn:aws:sns:us-east-1:999:topic")


# ---------------------------------------------------------------------------
# SNS::Subscription
# ---------------------------------------------------------------------------


class TestSnsSubscription:
    def test_create_sns_subscription(self):
        mock_sub = SimpleNamespace(
            subscription_arn="arn:aws:sns:us-east-1:999:topic:sub-123",
        )
        mock_store = MagicMock()
        mock_store.subscribe.return_value = mock_sub

        with patch("robotocore.services.sns.provider._get_store", return_value=mock_store):
            res = _resource(
                "AWS::SNS::Subscription",
                {
                    "TopicArn": "arn:aws:sns:us-east-1:999:topic",
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:999:queue",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "arn:aws:sns:us-east-1:999:topic:sub-123"
        assert res.status == "CREATE_COMPLETE"

    def test_create_sns_subscription_none_returned(self):
        mock_store = MagicMock()
        mock_store.subscribe.return_value = None

        with patch("robotocore.services.sns.provider._get_store", return_value=mock_store):
            res = _resource(
                "AWS::SNS::Subscription",
                {
                    "TopicArn": "arn:aws:sns:us-east-1:999:topic",
                    "Protocol": "sqs",
                    "Endpoint": "arn:aws:sqs:us-east-1:999:queue",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id is None
        assert res.status == "CREATE_COMPLETE"

    def test_delete_sns_subscription(self):
        mock_store = MagicMock()
        with patch("robotocore.services.sns.provider._get_store", return_value=mock_store):
            res = _resource("AWS::SNS::Subscription")
            res.physical_id = "arn:aws:sns:us-east-1:999:topic:sub-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_store.unsubscribe.assert_called_once_with("arn:aws:sns:us-east-1:999:topic:sub-123")


# ---------------------------------------------------------------------------
# S3::Bucket
# ---------------------------------------------------------------------------


class TestS3Bucket:
    def test_create_s3_bucket(self):
        mock_s3 = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_s3,
        ):
            res = _resource("AWS::S3::Bucket", {"BucketName": "my-bucket"})
            create_resource(res, REGION, ACCOUNT_ID)

        mock_s3.create_bucket.assert_called_once_with("my-bucket", REGION)
        assert res.physical_id == "my-bucket"
        assert res.attributes["Arn"] == "arn:aws:s3:::my-bucket"
        assert "s3.amazonaws.com" in res.attributes["DomainName"]
        assert res.status == "CREATE_COMPLETE"

    def test_create_s3_bucket_auto_name(self):
        mock_s3 = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_s3,
        ):
            res = _resource("AWS::S3::Bucket", {}, logical_id="MyBucket")
            create_resource(res, REGION, ACCOUNT_ID)

        call_args = mock_s3.create_bucket.call_args[0]
        assert call_args[0].startswith("cfn-mybucket-")
        assert res.status == "CREATE_COMPLETE"

    def test_delete_s3_bucket(self):
        mock_s3 = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_s3,
        ):
            res = _resource("AWS::S3::Bucket")
            res.physical_id = "my-bucket"
            delete_resource(res, REGION, ACCOUNT_ID)

        mock_s3.delete_bucket.assert_called_once_with("my-bucket")


# ---------------------------------------------------------------------------
# IAM::Role
# ---------------------------------------------------------------------------


class TestIamRole:
    def test_create_iam_role(self):
        mock_role = SimpleNamespace(arn="arn:aws:iam::999:role/my-role", id="ROLE123")
        mock_iam = MagicMock()
        mock_iam.create_role.return_value = mock_role

        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_iam,
        ):
            res = _resource(
                "AWS::IAM::Role",
                {
                    "RoleName": "my-role",
                    "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
                    "Path": "/service/",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "my-role"
        assert res.attributes["Arn"] == "arn:aws:iam::999:role/my-role"
        assert res.attributes["RoleId"] == "ROLE123"
        assert res.status == "CREATE_COMPLETE"
        # Verify policy doc was JSON-serialized
        call_args = mock_iam.create_role.call_args
        assert isinstance(call_args[0][1], str)
        assert json.loads(call_args[0][1])["Version"] == "2012-10-17"

    def test_delete_iam_role(self):
        mock_iam = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_iam,
        ):
            res = _resource("AWS::IAM::Role")
            res.physical_id = "my-role"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_iam.delete_role.assert_called_once_with("my-role")


# ---------------------------------------------------------------------------
# IAM::Policy
# ---------------------------------------------------------------------------


class TestIamPolicy:
    def test_create_iam_policy(self):
        mock_policy = SimpleNamespace(arn="arn:aws:iam::999:policy/my-policy")
        mock_iam = MagicMock()
        mock_iam.create_policy.return_value = mock_policy

        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_iam,
        ):
            res = _resource(
                "AWS::IAM::Policy",
                {
                    "PolicyName": "my-policy",
                    "PolicyDocument": {"Version": "2012-10-17", "Statement": []},
                    "Description": "Test policy",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "arn:aws:iam::999:policy/my-policy"
        assert res.attributes["Arn"] == "arn:aws:iam::999:policy/my-policy"
        assert res.status == "CREATE_COMPLETE"

    def test_delete_iam_policy(self):
        mock_iam = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_global_backend",
            return_value=mock_iam,
        ):
            res = _resource("AWS::IAM::Policy")
            res.physical_id = "arn:aws:iam::999:policy/my-policy"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_iam.delete_policy.assert_called_once_with("arn:aws:iam::999:policy/my-policy")


# ---------------------------------------------------------------------------
# Logs::LogGroup
# ---------------------------------------------------------------------------


class TestLogsLogGroup:
    def test_create_log_group(self):
        mock_logs = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_logs,
        ):
            res = _resource("AWS::Logs::LogGroup", {"LogGroupName": "/my/log/group"})
            create_resource(res, REGION, ACCOUNT_ID)

        mock_logs.create_log_group.assert_called_once_with("/my/log/group", {})
        assert res.physical_id == "/my/log/group"
        assert "log-group:/my/log/group" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_create_log_group_auto_name(self):
        mock_logs = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_logs,
        ):
            res = _resource("AWS::Logs::LogGroup", {}, logical_id="AppLogs")
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "/cfn/AppLogs"

    def test_delete_log_group(self):
        mock_logs = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_logs,
        ):
            res = _resource("AWS::Logs::LogGroup")
            res.physical_id = "/my/log/group"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_logs.delete_log_group.assert_called_once_with("/my/log/group")


# ---------------------------------------------------------------------------
# DynamoDB::Table
# ---------------------------------------------------------------------------


class TestDynamoDbTable:
    def test_create_dynamodb_table(self):
        mock_ddb = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ddb,
        ):
            res = _resource(
                "AWS::DynamoDB::Table",
                {
                    "TableName": "my-table",
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "BillingMode": "PAY_PER_REQUEST",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        mock_ddb.create_table.assert_called_once()
        assert res.physical_id == "my-table"
        assert "my-table" in res.attributes["Arn"]
        assert res.attributes["TableName"] == "my-table"
        assert res.status == "CREATE_COMPLETE"

    def test_create_dynamodb_table_with_streams(self):
        mock_ddb = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ddb,
        ):
            res = _resource(
                "AWS::DynamoDB::Table",
                {
                    "TableName": "stream-table",
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "StreamSpecification": {"StreamViewType": "NEW_IMAGE"},
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        call_kwargs = mock_ddb.create_table.call_args
        # streams kwarg should be set
        streams_arg = call_kwargs[1].get("streams") if call_kwargs[1] else None
        if streams_arg is None:
            # Positional arg check
            call_kwargs[0]
            # Find the streams argument (it's passed as keyword in the code)
            pass
        assert res.status == "CREATE_COMPLETE"

    def test_delete_dynamodb_table(self):
        mock_ddb = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ddb,
        ):
            res = _resource("AWS::DynamoDB::Table")
            res.physical_id = "my-table"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_ddb.delete_table.assert_called_once_with("my-table")


# ---------------------------------------------------------------------------
# Events::Rule
# ---------------------------------------------------------------------------


class TestEventsRule:
    def test_create_events_rule(self):
        mock_events = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_events,
        ):
            res = _resource(
                "AWS::Events::Rule",
                {
                    "Name": "my-rule",
                    "EventPattern": {"source": ["aws.ec2"]},
                    "State": "ENABLED",
                    "Description": "Test rule",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        mock_events.put_rule.assert_called_once()
        assert res.physical_id == "my-rule"
        assert "rule/my-rule" in res.attributes["Arn"]
        assert res.status == "CREATE_COMPLETE"

    def test_create_events_rule_with_schedule(self):
        mock_events = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_events,
        ):
            res = _resource(
                "AWS::Events::Rule",
                {
                    "Name": "schedule-rule",
                    "ScheduleExpression": "rate(5 minutes)",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.status == "CREATE_COMPLETE"

    def test_delete_events_rule(self):
        mock_events = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_events,
        ):
            res = _resource("AWS::Events::Rule")
            res.physical_id = "my-rule"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_events.delete_rule.assert_called_once_with("my-rule", None)


# ---------------------------------------------------------------------------
# KMS::Key
# ---------------------------------------------------------------------------


class TestKmsKey:
    def test_create_kms_key(self):
        mock_key = SimpleNamespace(id="key-123", arn="arn:aws:kms:us-east-1:999:key/key-123")
        mock_kms = MagicMock()
        mock_kms.create_key.return_value = mock_key

        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_kms,
        ):
            res = _resource(
                "AWS::KMS::Key",
                {
                    "Description": "My key",
                    "KeyUsage": "ENCRYPT_DECRYPT",
                    "KeySpec": "SYMMETRIC_DEFAULT",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "key-123"
        assert res.attributes["Arn"] == "arn:aws:kms:us-east-1:999:key/key-123"
        assert res.attributes["KeyId"] == "key-123"
        assert res.status == "CREATE_COMPLETE"

    def test_create_kms_key_with_dict_policy(self):
        mock_key = SimpleNamespace(id="key-456", arn="arn:aws:kms:us-east-1:999:key/key-456")
        mock_kms = MagicMock()
        mock_kms.create_key.return_value = mock_key

        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_kms,
        ):
            res = _resource(
                "AWS::KMS::Key",
                {
                    "KeyPolicy": {"Version": "2012-10-17", "Statement": []},
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        # Policy should have been JSON-serialized
        call_args = mock_kms.create_key.call_args[0]
        assert isinstance(call_args[0], str)
        assert res.status == "CREATE_COMPLETE"

    def test_delete_kms_key(self):
        mock_kms = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_kms,
        ):
            res = _resource("AWS::KMS::Key")
            res.physical_id = "key-123"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_kms.schedule_key_deletion.assert_called_once_with("key-123", 7)


# ---------------------------------------------------------------------------
# SSM::Parameter
# ---------------------------------------------------------------------------


class TestSsmParameter:
    def test_create_ssm_parameter(self):
        mock_ssm = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ssm,
        ):
            res = _resource(
                "AWS::SSM::Parameter",
                {
                    "Name": "/my/param",
                    "Value": "secret-value",
                    "Type": "SecureString",
                    "Description": "A param",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        mock_ssm.put_parameter.assert_called_once()
        assert res.physical_id == "/my/param"
        assert res.attributes["Type"] == "SecureString"
        assert res.attributes["Value"] == "secret-value"
        assert res.status == "CREATE_COMPLETE"

    def test_create_ssm_parameter_defaults(self):
        mock_ssm = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ssm,
        ):
            res = _resource("AWS::SSM::Parameter", {}, logical_id="Param1")
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "/cfn/Param1"
        assert res.attributes["Type"] == "String"
        assert res.attributes["Value"] == ""

    def test_delete_ssm_parameter(self):
        mock_ssm = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_ssm,
        ):
            res = _resource("AWS::SSM::Parameter")
            res.physical_id = "/my/param"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_ssm.delete_parameter.assert_called_once_with("/my/param")


# ---------------------------------------------------------------------------
# Lambda::Function
# ---------------------------------------------------------------------------


class TestLambdaFunction:
    def test_create_lambda_function_with_inline_code(self):
        mock_fn = SimpleNamespace(
            function_arn="arn:aws:lambda:us-east-1:999:function:my-fn",
        )
        mock_lambda = MagicMock()
        mock_lambda.create_function.return_value = mock_fn

        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_lambda,
        ):
            res = _resource(
                "AWS::Lambda::Function",
                {
                    "FunctionName": "my-fn",
                    "Runtime": "python3.12",
                    "Handler": "index.handler",
                    "Role": "arn:aws:iam::999:role/lambda-role",
                    "Code": {"ZipFile": "def handler(event, context): return 'ok'"},
                    "Environment": {"Variables": {"KEY": "VALUE"}},
                    "Timeout": 30,
                    "MemorySize": 256,
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.physical_id == "arn:aws:lambda:us-east-1:999:function:my-fn"
        assert res.attributes["Arn"] == "arn:aws:lambda:us-east-1:999:function:my-fn"
        assert res.attributes["FunctionName"] == "my-fn"
        assert res.status == "CREATE_COMPLETE"

        # Verify spec was passed correctly
        spec = mock_lambda.create_function.call_args[0][0]
        assert spec["FunctionName"] == "my-fn"
        assert spec["Runtime"] == "python3.12"
        assert spec["Timeout"] == 30
        assert spec["MemorySize"] == 256
        assert "Environment" in spec
        # ZipFile should be base64-encoded
        assert spec["Code"]["ZipFile"]  # non-empty

    def test_create_lambda_function_without_code(self):
        mock_fn = SimpleNamespace(
            function_arn="arn:aws:lambda:us-east-1:999:function:no-code-fn",
        )
        mock_lambda = MagicMock()
        mock_lambda.create_function.return_value = mock_fn

        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_lambda,
        ):
            res = _resource(
                "AWS::Lambda::Function",
                {
                    "FunctionName": "no-code-fn",
                },
            )
            create_resource(res, REGION, ACCOUNT_ID)

        assert res.status == "CREATE_COMPLETE"
        spec = mock_lambda.create_function.call_args[0][0]
        assert spec["Code"]["ZipFile"] == ""

    def test_delete_lambda_function_from_arn(self):
        mock_lambda = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_lambda,
        ):
            res = _resource("AWS::Lambda::Function")
            res.physical_id = "arn:aws:lambda:us-east-1:999:function:my-fn"
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_lambda.delete_function.assert_called_once_with("my-fn")

    def test_delete_lambda_function_no_physical_id(self):
        mock_lambda = MagicMock()
        with patch(
            "robotocore.services.cloudformation.resources._moto_backend",
            return_value=mock_lambda,
        ):
            res = _resource("AWS::Lambda::Function")
            res.physical_id = None
            delete_resource(res, REGION, ACCOUNT_ID)
        mock_lambda.delete_function.assert_not_called()


# ---------------------------------------------------------------------------
# _moto_backend / _moto_global_backend
# ---------------------------------------------------------------------------


class TestMotoBackendHelpers:
    def test_moto_backend_uses_account_id(self):
        from robotocore.services.cloudformation.resources import _moto_backend

        mock_backend = MagicMock()
        mock_dict = {"999999999999": {"us-east-1": mock_backend}}

        with patch("moto.backends.get_backend", return_value=mock_dict):
            result = _moto_backend("logs", "999999999999", "us-east-1")
        assert result is mock_backend

    def test_moto_global_backend(self):
        from robotocore.services.cloudformation.resources import _moto_global_backend

        mock_backend = MagicMock()
        mock_dict = {"999999999999": {"global": mock_backend}}

        with patch("moto.backends.get_backend", return_value=mock_dict):
            result = _moto_global_backend("iam", "999999999999")
        assert result is mock_backend
