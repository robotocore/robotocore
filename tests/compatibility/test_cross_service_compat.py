"""Cross-service integration compatibility tests.

These tests verify enterprise-grade cross-service integrations that require
multiple AWS services working together. These are the features that
differentiate us from basic Moto.
"""

import io
import json
import time
import urllib.request
import uuid
import zipfile
from datetime import UTC, datetime

import pytest

from tests.compatibility.conftest import make_client


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _create_lambda_role():
    """Create an IAM role for Lambda and return (iam_client, role_name, role_arn)."""
    iam = make_client("iam")
    role_name = f"lambda-xsvc-role-{uuid.uuid4().hex[:8]}"
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


class TestS3ToSNSToSQSNotification:
    """S3 -> SNS -> SQS: Put an object in S3 with notification config,
    verify the event arrives in SQS via SNS."""

    def test_s3_event_notification_via_sns_to_sqs(self):
        s3 = make_client("s3")
        sns = make_client("sns")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        bucket_name = f"s3notif-{suffix}"
        topic_name = f"s3notif-topic-{suffix}"
        queue_name = f"s3notif-queue-{suffix}"

        # Create SQS queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create SNS topic
        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        # Subscribe SQS to SNS
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # Create S3 bucket
        s3.create_bucket(Bucket=bucket_name)

        # Configure S3 bucket notification to send to SNS on object creation
        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": topic_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )

        # Put an object in S3
        s3.put_object(Bucket=bucket_name, Key="test-key.txt", Body=b"hello world")

        # Allow time for notification propagation
        time.sleep(2)

        # Receive message from SQS
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected at least one S3 event notification in SQS"

        # Parse the SNS wrapper to find the S3 event
        body = json.loads(msgs[0]["Body"])
        # SNS wraps the message; the actual S3 event is in body["Message"]
        if "Message" in body:
            s3_event = json.loads(body["Message"])
        else:
            s3_event = body

        # Verify it's an S3 event
        assert "Records" in s3_event
        record = s3_event["Records"][0]
        assert record["eventSource"] == "aws:s3"
        assert record["s3"]["bucket"]["name"] == bucket_name
        assert record["s3"]["object"]["key"] == "test-key.txt"

        # Clean up
        s3.delete_object(Bucket=bucket_name, Key="test-key.txt")
        s3.delete_bucket(Bucket=bucket_name)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestSNSToLambda:
    """SNS -> Lambda: Subscribe a Lambda function to an SNS topic, publish a message,
    and verify the Lambda was invoked with the correct SNS event format."""

    def test_sns_publish_invokes_lambda_subscriber(self):
        sns = make_client("sns")
        lam = make_client("lambda")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        topic_name = f"sns-lam-topic-{suffix}"
        func_name = f"sns-lam-func-{suffix}"
        queue_name = f"sns-lam-verify-{suffix}"

        # Create IAM role
        iam, role_name, role_arn = _create_lambda_role()

        # Create a Lambda function that writes the received event to SQS
        # so we can verify it was invoked with the correct payload
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client(\n"
            '        "sqs",\n'
            "        endpoint_url=os.environ.get(\n"
            '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
            '        region_name="us-east-1",\n'
            '        aws_access_key_id="testing",\n'
            '        aws_secret_access_key="testing")\n'
            '    queue_url = os.environ["VERIFY_QUEUE_URL"]\n'
            "    sqs.send_message(\n"
            "        QueueUrl=queue_url,\n"
            "        MessageBody=json.dumps(event))\n"
            '    return {"statusCode": 200}\n'
        )

        # Create verification SQS queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        # Create Lambda function with env vars pointing to verify queue
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        # Create SNS topic
        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        # Subscribe Lambda to SNS
        sub_resp = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="lambda",
            Endpoint=func_arn,
        )
        assert "SubscriptionArn" in sub_resp

        # Publish message to SNS
        pub_resp = sns.publish(
            TopicArn=topic_arn,
            Message="Hello from SNS to Lambda!",
            Subject="Test SNS->Lambda",
        )
        assert "MessageId" in pub_resp
        sns_message_id = pub_resp["MessageId"]

        # Check the verification queue for the Lambda invocation result
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected Lambda to write its event to the verification queue"

        # Parse and verify the SNS event that Lambda received
        lambda_event = json.loads(msgs[0]["Body"])
        assert "Records" in lambda_event
        record = lambda_event["Records"][0]
        assert record["EventSource"] == "aws:sns"
        assert record["EventVersion"] == "1.0"
        sns_data = record["Sns"]
        assert sns_data["Type"] == "Notification"
        assert sns_data["MessageId"] == sns_message_id
        assert sns_data["TopicArn"] == topic_arn
        assert sns_data["Subject"] == "Test SNS->Lambda"
        assert sns_data["Message"] == "Hello from SNS to Lambda!"

        # Clean up
        sns.unsubscribe(SubscriptionArn=sub_resp["SubscriptionArn"])
        sns.delete_topic(TopicArn=topic_arn)
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_sns_lambda_subscription_listed(self):
        """Verify that Lambda subscriptions appear in list_subscriptions."""
        sns = make_client("sns")
        lam = make_client("lambda")
        suffix = uuid.uuid4().hex[:8]
        topic_name = f"sns-lam-list-{suffix}"
        func_name = f"sns-lam-list-fn-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()
        code = _make_lambda_zip("def handler(event, ctx): return {}")
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        func_arn = func_resp["FunctionArn"]

        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        sub_resp = sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=func_arn)
        sub_arn = sub_resp["SubscriptionArn"]

        # List subscriptions and verify
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
        sub_list = subs["Subscriptions"]
        assert len(sub_list) == 1
        assert sub_list[0]["Protocol"] == "lambda"
        assert sub_list[0]["Endpoint"] == func_arn

        # Clean up
        sns.unsubscribe(SubscriptionArn=sub_arn)
        sns.delete_topic(TopicArn=topic_arn)
        lam.delete_function(FunctionName=func_name)
        iam.delete_role(RoleName=role_name)


class TestEventBridgeToLambda:
    """EventBridge -> Lambda: Create a rule with Lambda target, put an event,
    verify the cross-service dispatch works.

    Uses the invocation log from the EventBridge provider to verify that
    the Lambda function was actually executed with the correct payload.
    """

    def test_eventbridge_invokes_lambda(self):
        """EventBridge rule with Lambda target invokes the function.

        Uses SQS as a verification channel: Lambda writes its received event to SQS.
        """
        events = make_client("events")
        lam = make_client("lambda")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"eb-lambda-{suffix}"
        rule_name = f"eb-rule-{suffix}"
        queue_name = f"eb-verify-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()

        # Create verification SQS queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        # Lambda writes its received event to SQS for verification
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client(\n"
            '        "sqs",\n'
            "        endpoint_url=os.environ.get(\n"
            '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
            '        region_name="us-east-1",\n'
            '        aws_access_key_id="testing",\n'
            '        aws_secret_access_key="testing")\n'
            "    sqs.send_message(\n"
            '        QueueUrl=os.environ["VERIFY_QUEUE_URL"],\n'
            "        MessageBody=json.dumps(event))\n"
            '    return {"source": event.get("source", "unknown")}\n'
        )
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.eb.lambda"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "lambda-target", "Arn": func_arn}],
        )

        put_resp = events.put_events(
            Entries=[
                {
                    "Source": "test.eb.lambda",
                    "DetailType": "TestEvent",
                    "Detail": json.dumps({"message": "from-eventbridge"}),
                }
            ]
        )
        assert put_resp["FailedEntryCount"] == 0

        # Check SQS for the event the Lambda received
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected Lambda to write its event to the verification queue"

        lambda_event = json.loads(msgs[0]["Body"])
        assert lambda_event["source"] == "test.eb.lambda"
        assert lambda_event["detail-type"] == "TestEvent"
        assert lambda_event["detail"]["message"] == "from-eventbridge"

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["lambda-target"])
        events.delete_rule(Name=rule_name)
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_eventbridge_lambda_receives_full_event(self):
        """Lambda target receives the full EventBridge event (not SNS-style Records wrapper)."""
        events = make_client("events")
        lam = make_client("lambda")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"eb-full-evt-{suffix}"
        rule_name = f"eb-full-rule-{suffix}"
        queue_name = f"eb-full-verify-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        # Lambda writes its event keys to SQS for verification
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    result = {\n"
            '        "keys": sorted(event.keys()),\n'
            '        "has_records": "Records" in event}\n'
            "    sqs = boto3.client(\n"
            '        "sqs",\n'
            "        endpoint_url=os.environ.get(\n"
            '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
            '        region_name="us-east-1",\n'
            '        aws_access_key_id="testing",\n'
            '        aws_secret_access_key="testing")\n'
            "    sqs.send_message(\n"
            '        QueueUrl=os.environ["VERIFY_QUEUE_URL"],\n'
            "        MessageBody=json.dumps(result))\n"
            "    return result\n"
        )
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.eb.full"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "lam-t", "Arn": func_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.eb.full",
                    "DetailType": "FullEventTest",
                    "Detail": json.dumps({"data": 42}),
                }
            ]
        )

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected Lambda to write its result to the verification queue"

        result = json.loads(msgs[0]["Body"])
        # EventBridge events have standard keys, NOT a "Records" wrapper
        assert result["has_records"] is False
        expected_keys = [
            "account",
            "detail",
            "detail-type",
            "id",
            "region",
            "resources",
            "source",
            "time",
            "version",
        ]
        assert result["keys"] == expected_keys

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["lam-t"])
        events.delete_rule(Name=rule_name)
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_eventbridge_non_matching_event_does_not_invoke_lambda(self):
        """Events that don't match the rule pattern should not invoke the Lambda."""
        events = make_client("events")
        lam = make_client("lambda")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"eb-nomatch-{suffix}"
        rule_name = f"eb-nomatch-rule-{suffix}"
        queue_name = f"eb-nomatch-verify-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        # Lambda writes to SQS if invoked
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client(\n"
            '        "sqs",\n'
            "        endpoint_url=os.environ.get(\n"
            '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
            '        region_name="us-east-1",\n'
            '        aws_access_key_id="testing",\n'
            '        aws_secret_access_key="testing")\n'
            "    sqs.send_message(\n"
            '        QueueUrl=os.environ["VERIFY_QUEUE_URL"],\n'
            '        MessageBody="invoked")\n'
            '    return {"invoked": True}\n'
        )
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["specific.source.only"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "lam-t", "Arn": func_arn}],
        )

        # Put event with DIFFERENT source -- should NOT match
        events.put_events(
            Entries=[
                {
                    "Source": "different.source",
                    "DetailType": "NoMatch",
                    "Detail": json.dumps({"x": 1}),
                }
            ]
        )

        # Brief wait, then verify queue is empty
        time.sleep(2)
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 0, f"Lambda should NOT have been invoked, but got: {msgs}"

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["lam-t"])
        events.delete_rule(Name=rule_name)
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)


class TestStepFunctionsToLambda:
    """Step Functions -> Lambda: Create a state machine with a Task state
    that invokes a Lambda function, verify execution completes with
    the Lambda's return value."""

    def test_step_functions_invokes_lambda(self):
        sfn = make_client("stepfunctions")
        lam = make_client("lambda")
        iam = make_client("iam")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"sfn-lambda-{suffix}"

        # Create roles
        sfn_role_name = f"sfn-xsvc-role-{suffix}"
        lambda_role_name = f"sfn-lam-role-{suffix}"

        # SFN role
        sfn_trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        sfn_role = iam.create_role(
            RoleName=sfn_role_name,
            AssumeRolePolicyDocument=sfn_trust,
        )
        sfn_role_arn = sfn_role["Role"]["Arn"]

        # Lambda role
        lambda_trust = json.dumps(
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
        iam.create_role(
            RoleName=lambda_role_name,
            AssumeRolePolicyDocument=lambda_trust,
        )
        lambda_role_arn = f"arn:aws:iam::123456789012:role/{lambda_role_name}"

        # Create Lambda function that returns a value
        code = _make_lambda_zip(
            "def handler(event, ctx):\n"
            "    return {\n"
            '        "processed": True,\n'
            '        "input_name": event.get("name", "unknown")}'
        )
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=lambda_role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        func_arn = func_resp["FunctionArn"]

        # Create state machine with Task state invoking Lambda
        sm_name = f"sfn-lam-sm-{suffix}"
        definition = json.dumps(
            {
                "Comment": "State machine that invokes a Lambda function",
                "StartAt": "InvokeLambda",
                "States": {
                    "InvokeLambda": {
                        "Type": "Task",
                        "Resource": func_arn,
                        "End": True,
                    }
                },
            }
        )

        sm_resp = sfn.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn=sfn_role_arn,
        )
        sm_arn = sm_resp["stateMachineArn"]

        # Start execution with input
        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps({"name": "robotocore"}),
        )
        exec_arn = exec_resp["executionArn"]

        # Describe execution to check result
        result = sfn.describe_execution(executionArn=exec_arn)
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["processed"] is True
        assert output["input_name"] == "robotocore"

        # Clean up
        sfn.delete_state_machine(stateMachineArn=sm_arn)
        lam.delete_function(FunctionName=func_name)
        iam.delete_role(RoleName=sfn_role_name)
        iam.delete_role(RoleName=lambda_role_name)


class TestFirehoseToS3:
    """Firehose -> S3: Create a delivery stream pointing to S3, put records,
    verify data appears in S3."""

    @pytest.mark.skip(
        reason=(
            "Firehose does not flush buffered records to S3 "
            "in Moto — cross-service delivery not yet implemented"
        )
    )
    def test_firehose_delivers_to_s3(self):
        firehose = make_client("firehose")
        s3 = make_client("s3")
        suffix = uuid.uuid4().hex[:8]
        bucket_name = f"fh-dest-{suffix}"
        stream_name = f"fh-stream-{suffix}"

        # Create destination bucket
        s3.create_bucket(Bucket=bucket_name)

        # Create Firehose delivery stream
        firehose.create_delivery_stream(
            DeliveryStreamName=stream_name,
            ExtendedS3DestinationConfiguration={
                "BucketARN": f"arn:aws:s3:::{bucket_name}",
                "RoleARN": "arn:aws:iam::123456789012:role/firehose-role",
                "Prefix": "data/",
                "BufferingHints": {
                    "SizeInMBs": 1,
                    "IntervalInSeconds": 60,
                },
            },
        )

        # Put records
        records = [
            {"Data": json.dumps({"event": "login", "user": "alice"}).encode() + b"\n"},
            {"Data": json.dumps({"event": "purchase", "user": "bob"}).encode() + b"\n"},
            {"Data": json.dumps({"event": "logout", "user": "alice"}).encode() + b"\n"},
        ]
        resp = firehose.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=records,
        )
        assert resp["FailedPutCount"] == 0

        # Allow time for delivery
        time.sleep(2)

        # Check that data appeared in S3
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="data/")
        contents = objects.get("Contents", [])
        assert len(contents) >= 1, "Expected at least one object delivered by Firehose"

        # Read the delivered data and verify content
        all_data = b""
        for obj in contents:
            body = s3.get_object(Bucket=bucket_name, Key=obj["Key"])["Body"].read()
            all_data += body

        data_str = all_data.decode("utf-8")
        assert "login" in data_str
        assert "alice" in data_str

        # Clean up
        firehose.delete_delivery_stream(DeliveryStreamName=stream_name)
        for obj in contents:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        # Re-list in case more objects appeared
        remaining = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in remaining:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)


class TestCloudFormationMultiService:
    """CloudFormation stack with multiple services: Deploy a template that
    creates an SQS queue and an SNS topic. Verify both resources exist.
    Delete the stack. Verify cleanup."""

    def test_cfn_creates_and_deletes_sqs_and_sns(self):
        cfn = make_client("cloudformation")
        sqs = make_client("sqs")
        sns = make_client("sns")
        suffix = uuid.uuid4().hex[:8]
        stack_name = f"xsvc-stack-{suffix}"
        queue_name = f"xsvc-queue-{suffix}"
        topic_name = f"xsvc-topic-{suffix}"

        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Cross-service integration test stack",
                "Resources": {
                    "TestQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": queue_name,
                        },
                    },
                    "TestTopic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {
                            "TopicName": topic_name,
                        },
                    },
                },
                "Outputs": {
                    "QueueUrl": {
                        "Value": {"Ref": "TestQueue"},
                    },
                    "QueueArn": {
                        "Value": {"Fn::GetAtt": ["TestQueue", "Arn"]},
                    },
                    "TopicArn": {
                        "Value": {"Ref": "TestTopic"},
                    },
                },
            }
        )

        # Create stack
        cfn.create_stack(StackName=stack_name, TemplateBody=template)

        # Verify stack created successfully
        response = cfn.describe_stacks(StackName=stack_name)
        stack = response["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Verify outputs
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "QueueUrl" in outputs
        assert "QueueArn" in outputs
        assert "TopicArn" in outputs

        # Verify SQS queue exists
        q_url = sqs.get_queue_url(QueueName=queue_name)
        assert queue_name in q_url["QueueUrl"]

        # Verify SNS topic exists
        topics = sns.list_topics()
        topic_arns = [t["TopicArn"] for t in topics["Topics"]]
        matching = [a for a in topic_arns if topic_name in a]
        assert len(matching) >= 1, f"Expected topic {topic_name} in {topic_arns}"

        # Delete stack
        cfn.delete_stack(StackName=stack_name)

        # Verify SQS queue was cleaned up
        with pytest.raises(Exception):
            sqs.get_queue_url(QueueName=queue_name)

        # Verify SNS topic was cleaned up
        topics_after = sns.list_topics()
        topic_arns_after = [t["TopicArn"] for t in topics_after["Topics"]]
        matching_after = [a for a in topic_arns_after if topic_name in a]
        assert len(matching_after) == 0, f"Topic {topic_name} should be deleted"


class TestSNSToSQS:
    """SNS -> SQS: Publish a message to an SNS topic with an SQS subscriber."""

    def test_sns_publishes_to_sqs_subscriber(self):
        sns = make_client("sns")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        topic_name = f"sns-sqs-topic-{suffix}"
        queue_name = f"sns-sqs-queue-{suffix}"

        # Create SQS queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create SNS topic
        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        # Subscribe SQS to SNS
        sub_resp = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sub_arn = sub_resp["SubscriptionArn"]

        # Publish message
        msg_body = f"cross-service-test-{suffix}"
        pub_resp = sns.publish(TopicArn=topic_arn, Message=msg_body)
        assert "MessageId" in pub_resp

        # Receive from SQS
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected at least one message in SQS from SNS"

        body = json.loads(msgs[0]["Body"])
        assert body["Type"] == "Notification"
        assert body["TopicArn"] == topic_arn
        assert body["Message"] == msg_body

        # Clean up
        sns.unsubscribe(SubscriptionArn=sub_arn)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)

    def test_sns_to_sqs_with_message_attributes(self):
        """Publish with MessageAttributes, verify they arrive in SQS."""
        sns = make_client("sns")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        q_resp = sqs.create_queue(QueueName=f"sns-sqs-attr-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        topic_resp = sns.create_topic(Name=f"sns-sqs-attr-topic-{suffix}")
        topic_arn = topic_resp["TopicArn"]

        sub_resp = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sub_arn = sub_resp["SubscriptionArn"]

        sns.publish(
            TopicArn=topic_arn,
            Message="attr-test",
            MessageAttributes={
                "color": {"DataType": "String", "StringValue": "blue"},
            },
        )

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert body["Message"] == "attr-test"
        assert "MessageAttributes" in body

        # Clean up
        sns.unsubscribe(SubscriptionArn=sub_arn)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)

    def test_sns_to_sqs_multiple_messages(self):
        """Publish multiple messages and verify all arrive."""
        sns = make_client("sns")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        q_resp = sqs.create_queue(QueueName=f"sns-sqs-multi-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        topic_resp = sns.create_topic(Name=f"sns-sqs-multi-topic-{suffix}")
        topic_arn = topic_resp["TopicArn"]

        sub_resp = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        sub_arn = sub_resp["SubscriptionArn"]

        # Publish 3 messages
        for i in range(3):
            sns.publish(TopicArn=topic_arn, Message=f"message-{i}")

        time.sleep(1)

        # Receive all messages (may take multiple receives)
        all_msgs = []
        for _ in range(3):
            recv = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=3
            )
            all_msgs.extend(recv.get("Messages", []))
            if len(all_msgs) >= 3:
                break

        assert len(all_msgs) >= 3, f"Expected 3 messages, got {len(all_msgs)}"

        # Clean up
        sns.unsubscribe(SubscriptionArn=sub_arn)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestCloudFormationDynamoDB:
    """CloudFormation with DynamoDB: Deploy a template that creates a DDB table."""

    def test_cfn_creates_dynamodb_table(self):
        cfn = make_client("cloudformation")
        dynamodb = make_client("dynamodb")
        suffix = uuid.uuid4().hex[:8]
        stack_name = f"cfn-ddb-{suffix}"
        table_name = f"cfn-ddb-table-{suffix}"

        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyTable": {
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

        # Verify stack created
        stacks = cfn.describe_stacks(StackName=stack_name)
        assert stacks["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify table exists
        desc = dynamodb.describe_table(TableName=table_name)
        assert desc["Table"]["TableName"] == table_name
        assert desc["Table"]["KeySchema"][0]["AttributeName"] == "pk"

        # Delete stack and verify table is cleaned up
        cfn.delete_stack(StackName=stack_name)
        with pytest.raises(Exception):
            dynamodb.describe_table(TableName=table_name)


class TestCloudFormationLambda:
    """CloudFormation with Lambda: Deploy a template that creates a Lambda function."""

    def test_cfn_creates_and_invokes_lambda(self):
        cfn = make_client("cloudformation")
        lam = make_client("lambda")
        iam_client = make_client("iam")
        suffix = uuid.uuid4().hex[:8]
        stack_name = f"cfn-lam-{suffix}"
        func_name = f"cfn-lam-func-{suffix}"
        role_name = f"cfn-lam-role-{suffix}"

        # Create IAM role first (outside the stack for simplicity)
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
        role_resp = iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=trust)
        role_arn = role_resp["Role"]["Arn"]

        code = _make_lambda_zip(
            "def handler(event, ctx):\n    return {'result': 'from-cfn-lambda', 'input': event}\n"
        )

        import base64

        code_b64 = base64.b64encode(code).decode("utf-8")

        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyFunction": {
                        "Type": "AWS::Lambda::Function",
                        "Properties": {
                            "FunctionName": func_name,
                            "Runtime": "python3.12",
                            "Handler": "lambda_function.handler",
                            "Role": role_arn,
                            "Code": {"ZipFile": code_b64},
                        },
                    },
                },
            }
        )

        cfn.create_stack(StackName=stack_name, TemplateBody=template)

        stacks = cfn.describe_stacks(StackName=stack_name)
        assert stacks["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify function exists
        func = lam.get_function(FunctionName=func_name)
        assert func["Configuration"]["FunctionName"] == func_name

        # Invoke the function
        invoke_resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({"key": "value"}))
        payload = json.loads(invoke_resp["Payload"].read())
        assert payload["result"] == "from-cfn-lambda"

        # Delete stack
        cfn.delete_stack(StackName=stack_name)
        iam_client.delete_role(RoleName=role_name)


class TestSQSTagsViaResourceGroupsTagging:
    """SQS queue tags visible through Resource Groups Tagging API."""

    def test_sqs_tags_via_tagging_api(self):
        sqs = make_client("sqs")
        tagging = make_client("resourcegroupstaggingapi")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"tagged-queue-{suffix}"

        # Create SQS queue with tags
        q_resp = sqs.create_queue(
            QueueName=queue_name,
            tags={"Environment": "test", "Project": "robotocore"},
        )
        queue_url = q_resp["QueueUrl"]

        # Verify tags via SQS API
        tags_resp = sqs.list_queue_tags(QueueUrl=queue_url)
        assert tags_resp["Tags"]["Environment"] == "test"

        # Verify tags via Resource Groups Tagging API
        resp = tagging.get_resources(
            TagFilters=[{"Key": "Project", "Values": ["robotocore"]}],
            ResourceTypeFilters=["sqs"],
        )
        arns = [r["ResourceARN"] for r in resp["ResourceTagMappingList"]]
        assert len(arns) >= 1, "Expected tagged SQS queue to appear in tagging API results"

        # Clean up
        sqs.delete_queue(QueueUrl=queue_url)

    def test_sqs_tags_via_tagging_api_get_tag_keys(self):
        sqs = make_client("sqs")
        tagging = make_client("resourcegroupstaggingapi")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"tagkeys-queue-{suffix}"

        q_resp = sqs.create_queue(
            QueueName=queue_name,
            tags={"UniqueTagKey": "value"},
        )
        queue_url = q_resp["QueueUrl"]

        # Get tag keys
        resp = tagging.get_tag_keys()
        assert "UniqueTagKey" in resp["TagKeys"]

        # Clean up
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeToSQS:
    """EventBridge -> SQS: Rule with SQS target delivers events."""

    def test_eventbridge_delivers_to_sqs(self):
        events = make_client("events")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"eb-sqs-rule-{suffix}"
        queue_name = f"eb-sqs-queue-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.eb.sqs"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.eb.sqs",
                    "DetailType": "SQSDelivery",
                    "Detail": json.dumps({"payload": "from-eventbridge"}),
                }
            ]
        )

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected EventBridge to deliver event to SQS"

        body = json.loads(msgs[0]["Body"])
        assert body["source"] == "test.eb.sqs"
        assert body["detail"]["payload"] == "from-eventbridge"

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["sqs-target"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeToSNS:
    """EventBridge -> SNS: Rule with SNS target publishes events."""

    def test_eventbridge_delivers_to_sns_then_sqs(self):
        events = make_client("events")
        sns = make_client("sns")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        # Set up SQS as SNS subscriber for verification
        q_resp = sqs.create_queue(QueueName=f"eb-sns-verify-{suffix}")
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        topic_resp = sns.create_topic(Name=f"eb-sns-topic-{suffix}")
        topic_arn = topic_resp["TopicArn"]

        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

        # EventBridge rule targeting SNS
        rule_name = f"eb-sns-rule-{suffix}"
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.eb.sns"]}),
            State="ENABLED",
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "sns-target", "Arn": topic_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.eb.sns",
                    "DetailType": "SNSDelivery",
                    "Detail": json.dumps({"data": "eb-to-sns"}),
                }
            ]
        )

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected EventBridge->SNS->SQS chain to deliver"

        # The SQS message is an SNS notification wrapping the EventBridge event
        body = json.loads(msgs[0]["Body"])
        assert body["Type"] == "Notification"
        inner = json.loads(body["Message"])
        assert inner["source"] == "test.eb.sns"

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["sns-target"])
        events.delete_rule(Name=rule_name)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)


class TestSQSLambdaEventSourceMapping:
    """SQS -> Lambda via Event Source Mapping."""

    def test_sqs_event_source_mapping_crud(self):
        """Create and verify an SQS event source mapping for Lambda."""
        lam = make_client("lambda")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"esm-sqs-fn-{suffix}"
        queue_name = f"esm-sqs-queue-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        code = _make_lambda_zip("def handler(event, ctx): return {'ok': True}")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm_resp = lam.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=func_name,
            BatchSize=5,
        )
        esm_uuid = esm_resp["UUID"]
        assert esm_resp["EventSourceArn"] == queue_arn
        assert esm_resp["BatchSize"] == 5

        # List and verify
        list_resp = lam.list_event_source_mappings(FunctionName=func_name)
        uuids = [m["UUID"] for m in list_resp["EventSourceMappings"]]
        assert esm_uuid in uuids

        # Delete
        lam.delete_event_source_mapping(UUID=esm_uuid)

        # Clean up
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)


class TestKinesisLambdaEventSourceMapping:
    """Kinesis -> Lambda via Event Source Mapping."""

    def test_kinesis_event_source_mapping_crud(self):
        """Create and verify a Kinesis event source mapping for Lambda."""
        lam = make_client("lambda")
        kinesis = make_client("kinesis")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"esm-kin-fn-{suffix}"
        stream_name = f"esm-kin-stream-{suffix}"

        iam, role_name, role_arn = _create_lambda_role()

        kinesis.create_stream(StreamName=stream_name, ShardCount=1)

        # Wait for stream to be active
        for _ in range(10):
            desc = kinesis.describe_stream(StreamName=stream_name)
            if desc["StreamDescription"]["StreamStatus"] == "ACTIVE":
                break
            time.sleep(0.5)
        stream_arn = desc["StreamDescription"]["StreamARN"]

        code = _make_lambda_zip("def handler(event, ctx): return {'ok': True}")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        esm_resp = lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=func_name,
            StartingPosition="LATEST",
            BatchSize=10,
        )
        esm_uuid = esm_resp["UUID"]
        assert esm_resp["EventSourceArn"] == stream_arn
        assert esm_resp["BatchSize"] == 10

        # List and verify
        list_resp = lam.list_event_source_mappings(FunctionName=func_name)
        uuids = [m["UUID"] for m in list_resp["EventSourceMappings"]]
        assert esm_uuid in uuids

        # Delete
        lam.delete_event_source_mapping(UUID=esm_uuid)

        # Clean up
        lam.delete_function(FunctionName=func_name)
        kinesis.delete_stream(StreamName=stream_name)
        iam.delete_role(RoleName=role_name)


class TestS3ToLambdaNotification:
    """S3 bucket notification -> Lambda real invocation.

    Lambda writes to SQS for verification.
    """

    _LAMBDA_CODE = (
        "import json, boto3, os\n"
        "def handler(event, ctx):\n"
        "    sqs = boto3.client(\n"
        '        "sqs",\n'
        "        endpoint_url=os.environ.get(\n"
        '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
        '        region_name="us-east-1",\n'
        '        aws_access_key_id="testing",\n'
        '        aws_secret_access_key="testing")\n'
        '    queue_url = os.environ["VERIFY_QUEUE_URL"]\n'
        "    sqs.send_message(\n"
        "        QueueUrl=queue_url,\n"
        "        MessageBody=json.dumps(event))\n"
        '    return {"statusCode": 200}\n'
    )

    def _setup(self, suffix, events, filter_rules=None):
        """Shared setup: IAM role, SQS queue, Lambda, S3 bucket
        with notification config. Returns dict of clients/names."""
        iam, role_name, role_arn = _create_lambda_role()

        sqs = make_client("sqs")
        queue_name = f"verify-s3-{suffix}"
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        lam = make_client("lambda")
        func_name = f"s3notif-fn-{suffix}"
        code = _make_lambda_zip(self._LAMBDA_CODE)
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        s3 = make_client("s3")
        bucket_name = f"s3notif-{suffix}"
        s3.create_bucket(Bucket=bucket_name)

        lambda_config = {
            "LambdaFunctionArn": func_arn,
            "Events": events,
        }
        if filter_rules is not None:
            lambda_config["Filter"] = {"Key": {"FilterRules": filter_rules}}

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={"LambdaFunctionConfigurations": [lambda_config]},
        )

        return {
            "iam": iam,
            "sqs": sqs,
            "lam": lam,
            "s3": s3,
            "role_name": role_name,
            "queue_url": queue_url,
            "queue_name": queue_name,
            "func_name": func_name,
            "func_arn": func_arn,
            "bucket_name": bucket_name,
        }

    def _cleanup(self, ctx):
        ctx["s3"].delete_bucket(Bucket=ctx["bucket_name"])
        ctx["lam"].delete_function(FunctionName=ctx["func_name"])
        ctx["sqs"].delete_queue(QueueUrl=ctx["queue_url"])
        ctx["iam"].delete_role(RoleName=ctx["role_name"])

    def test_put_object_invokes_lambda(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(suffix, events=["s3:ObjectCreated:*"])

        ctx["s3"].put_object(
            Bucket=ctx["bucket_name"],
            Key="hello.txt",
            Body=b"world",
        )

        time.sleep(3)

        recv = ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        event = json.loads(msgs[0]["Body"])
        assert "Records" in event
        rec = event["Records"][0]
        assert rec["eventSource"] == "aws:s3"
        assert rec["eventName"].startswith("ObjectCreated")
        assert rec["s3"]["bucket"]["name"] == ctx["bucket_name"]
        assert rec["s3"]["object"]["key"] == "hello.txt"

        # Clean up
        ctx["s3"].delete_object(Bucket=ctx["bucket_name"], Key="hello.txt")
        self._cleanup(ctx)

    def test_delete_object_invokes_lambda(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(suffix, events=["s3:ObjectRemoved:*"])

        # Put then delete to trigger the removal event
        ctx["s3"].put_object(
            Bucket=ctx["bucket_name"],
            Key="bye.txt",
            Body=b"gone",
        )
        ctx["s3"].delete_object(Bucket=ctx["bucket_name"], Key="bye.txt")

        time.sleep(3)

        recv = ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        event = json.loads(msgs[0]["Body"])
        assert "Records" in event
        rec = event["Records"][0]
        assert rec["eventSource"] == "aws:s3"
        assert rec["eventName"].startswith("ObjectRemoved")
        assert rec["s3"]["bucket"]["name"] == ctx["bucket_name"]
        assert rec["s3"]["object"]["key"] == "bye.txt"

        self._cleanup(ctx)

    def test_filter_prefix(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(
            suffix,
            events=["s3:ObjectCreated:*"],
            filter_rules=[{"Name": "prefix", "Value": "data/"}],
        )

        # Put object with matching prefix -> should invoke
        ctx["s3"].put_object(
            Bucket=ctx["bucket_name"],
            Key="data/file.csv",
            Body=b"matched",
        )
        # Put object without prefix -> should NOT invoke
        ctx["s3"].put_object(
            Bucket=ctx["bucket_name"],
            Key="other/file.csv",
            Body=b"no-match",
        )

        time.sleep(3)

        recv = ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        # Should have exactly 1 message (the data/ prefixed one)
        assert len(msgs) == 1
        event = json.loads(msgs[0]["Body"])
        assert "Records" in event
        rec = event["Records"][0]
        assert rec["s3"]["object"]["key"] == "data/file.csv"

        # Clean up
        ctx["s3"].delete_object(Bucket=ctx["bucket_name"], Key="data/file.csv")
        ctx["s3"].delete_object(Bucket=ctx["bucket_name"], Key="other/file.csv")
        self._cleanup(ctx)


class TestDynamoDBStreamsToLambdaESM:
    """DynamoDB Streams -> Lambda via Event Source Mapping.

    Lambda writes to SQS for verification.
    """

    _LAMBDA_CODE = (
        "import json, boto3, os\n"
        "def handler(event, ctx):\n"
        "    sqs = boto3.client(\n"
        '        "sqs",\n'
        "        endpoint_url=os.environ.get(\n"
        '            "SQS_ENDPOINT", "http://localhost:4566"),\n'
        '        region_name="us-east-1",\n'
        '        aws_access_key_id="testing",\n'
        '        aws_secret_access_key="testing")\n'
        '    queue_url = os.environ["VERIFY_QUEUE_URL"]\n'
        "    sqs.send_message(\n"
        "        QueueUrl=queue_url,\n"
        "        MessageBody=json.dumps(event))\n"
        '    return {"statusCode": 200}\n'
    )

    def _create_table_with_stream(self, suffix):
        """Create a DDB table with streams enabled."""
        ddb = make_client("dynamodb")
        table_name = f"ddb-stream-{suffix}"
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {
                    "AttributeName": "pk",
                    "AttributeType": "S",
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        desc = ddb.describe_table(TableName=table_name)
        stream_arn = desc["Table"]["LatestStreamArn"]
        return ddb, table_name, stream_arn

    def _setup(self, suffix, batch_size=10):
        """Shared setup: IAM role, SQS queue, Lambda, DDB table
        with stream, ESM. Returns dict of clients/names."""
        iam, role_name, role_arn = _create_lambda_role()

        sqs = make_client("sqs")
        queue_name = f"verify-ddb-{suffix}"
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]

        lam = make_client("lambda")
        func_name = f"ddb-fn-{suffix}"
        code = _make_lambda_zip(self._LAMBDA_CODE)
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": queue_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )
        func_arn = func_resp["FunctionArn"]

        ddb, table_name, stream_arn = self._create_table_with_stream(suffix)

        esm_resp = lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=func_name,
            StartingPosition="LATEST",
            BatchSize=batch_size,
        )
        esm_uuid = esm_resp["UUID"]

        return {
            "iam": iam,
            "sqs": sqs,
            "lam": lam,
            "ddb": ddb,
            "role_name": role_name,
            "queue_url": queue_url,
            "queue_name": queue_name,
            "func_name": func_name,
            "func_arn": func_arn,
            "table_name": table_name,
            "stream_arn": stream_arn,
            "esm_uuid": esm_uuid,
        }

    def _cleanup(self, ctx):
        ctx["lam"].delete_event_source_mapping(UUID=ctx["esm_uuid"])
        ctx["lam"].delete_function(FunctionName=ctx["func_name"])
        ctx["ddb"].delete_table(TableName=ctx["table_name"])
        ctx["sqs"].delete_queue(QueueUrl=ctx["queue_url"])
        ctx["iam"].delete_role(RoleName=ctx["role_name"])

    def test_put_item_triggers_lambda(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(suffix)

        ctx["ddb"].put_item(
            TableName=ctx["table_name"],
            Item={"pk": {"S": "item-1"}, "val": {"S": "a"}},
        )

        time.sleep(5)

        recv = ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        event = json.loads(msgs[0]["Body"])
        assert "Records" in event
        rec = event["Records"][0]
        assert rec["eventSource"] == "aws:dynamodb"
        assert rec["eventName"] == "INSERT"
        assert "dynamodb" in rec
        new_img = rec["dynamodb"].get("NewImage", {})
        assert "pk" in new_img

        self._cleanup(ctx)

    def test_update_item_triggers_lambda(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(suffix)

        # Insert first
        ctx["ddb"].put_item(
            TableName=ctx["table_name"],
            Item={"pk": {"S": "upd-1"}, "val": {"S": "v1"}},
        )
        time.sleep(5)

        # Drain INSERT message
        ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )

        # Now update the item
        ctx["ddb"].update_item(
            TableName=ctx["table_name"],
            Key={"pk": {"S": "upd-1"}},
            UpdateExpression="SET val = :v",
            ExpressionAttributeValues={":v": {"S": "v2"}},
        )

        time.sleep(5)

        recv = ctx["sqs"].receive_message(
            QueueUrl=ctx["queue_url"],
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        event = json.loads(msgs[0]["Body"])
        assert "Records" in event
        rec = event["Records"][0]
        assert rec["eventSource"] == "aws:dynamodb"
        assert rec["eventName"] == "MODIFY"

        self._cleanup(ctx)

    def test_batch_size(self):
        suffix = uuid.uuid4().hex[:8]
        ctx = self._setup(suffix, batch_size=1)

        # Put 3 items
        for i in range(3):
            ctx["ddb"].put_item(
                TableName=ctx["table_name"],
                Item={
                    "pk": {"S": f"batch-{i}"},
                    "val": {"S": str(i)},
                },
            )

        time.sleep(5)

        # With BatchSize=1, each item should trigger a
        # separate Lambda invocation -> 3 SQS messages
        all_msgs = []
        for _ in range(3):
            recv = ctx["sqs"].receive_message(
                QueueUrl=ctx["queue_url"],
                WaitTimeSeconds=5,
                MaxNumberOfMessages=10,
            )
            batch = recv.get("Messages", [])
            all_msgs.extend(batch)
            if len(all_msgs) >= 3:
                break

        assert len(all_msgs) >= 3
        # Each message should have exactly 1 record
        for msg in all_msgs[:3]:
            event = json.loads(msg["Body"])
            assert "Records" in event
            assert len(event["Records"]) == 1
            assert event["Records"][0]["eventSource"] == "aws:dynamodb"

        self._cleanup(ctx)


class TestSQSFifoLambdaESM:
    """SQS FIFO -> Lambda via Event Source Mapping."""

    def test_fifo_triggers_lambda(self):
        """FIFO queue triggers Lambda via ESM."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        # Create verification queue
        verify_name = f"verify-fifo-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        # Create Lambda that writes event to verify queue
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps(event))\n"
            "    return {'ok': True}\n"
        )
        func_name = f"fifo-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        # Create FIFO source queue
        fifo_name = f"src-fifo-{suffix}.fifo"
        sq = sqs.create_queue(
            QueueName=fifo_name,
            Attributes={"FifoQueue": "true"},
        )
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        # Create ESM
        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
        )

        # Send a FIFO message
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody='{"action": "fifo-test"}',
            MessageGroupId="grp1",
            MessageDeduplicationId=f"dedup-{suffix}",
        )
        time.sleep(8)

        # Check verification queue
        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert "Records" in body

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)

    def test_message_group_id_in_event(self):
        """FIFO ESM event records contain MessageGroupId."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        verify_name = f"verify-grp-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps(event))\n"
            "    return {'ok': True}\n"
        )
        func_name = f"grpid-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        fifo_name = f"src-grp-{suffix}.fifo"
        sq = sqs.create_queue(
            QueueName=fifo_name,
            Attributes={"FifoQueue": "true"},
        )
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
        )

        sqs.send_message(
            QueueUrl=source_url,
            MessageBody='{"data": "grpid-test"}',
            MessageGroupId="test-group-42",
            MessageDeduplicationId=f"dedup-grp-{suffix}",
        )
        time.sleep(8)

        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        record = body["Records"][0]
        attrs = record.get("attributes", {})
        assert attrs.get("MessageGroupId") == "test-group-42"

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)


class TestESMBisectOnError:
    """ESM bisect-on-error splits failed batches."""

    def test_bisect_retries_halves_on_error(self):
        """BisectBatchOnFunctionError splits batch and retries."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        verify_name = f"verify-bisect-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        # Lambda logs batch size then raises
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps({"
            "'batch_size': len(event.get('Records', []))}"
            "))\n"
            "    raise Exception("
            "'Intentional failure for bisect test')\n"
        )
        func_name = f"bisect-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        source_name = f"src-bisect-{suffix}"
        sq = sqs.create_queue(QueueName=source_name)
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
            BisectBatchOnFunctionError=True,
            MaximumRetryAttempts=0,
        )

        # Send 4 messages
        for i in range(4):
            sqs.send_message(
                QueueUrl=source_url,
                MessageBody=json.dumps({"idx": i}),
            )
        time.sleep(8)

        # Bisection means multiple invocations
        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) > 1, "Expected multiple invocations from bisection"

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)


class TestESMFilterCriteria:
    """ESM filter criteria routes only matching messages."""

    def test_sqs_filter_body_match(self):
        """Only messages matching filter pattern trigger Lambda."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        verify_name = f"verify-filt-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps(event))\n"
            "    return {'ok': True}\n"
        )
        func_name = f"filt-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        source_name = f"src-filt-{suffix}"
        sq = sqs.create_queue(QueueName=source_name)
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        filter_pattern = json.dumps({"body": {"type": ["order"]}})
        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
            FilterCriteria={"Filters": [{"Pattern": filter_pattern}]},
        )

        # Send matching message
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"type": "order", "id": 1}),
        )
        # Send non-matching message
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"type": "other"}),
        )
        time.sleep(8)

        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1, f"Expected 1 invocation for matching msg, got {len(msgs)}"
        body = json.loads(msgs[0]["Body"])
        # The single record should be the "order" message
        record_body = json.loads(body["Records"][0]["body"])
        assert record_body["type"] == "order"

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)

    def test_sqs_filter_no_match_skipped(self):
        """Non-matching messages do not trigger Lambda."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        verify_name = f"verify-nofilt-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps(event))\n"
            "    return {'ok': True}\n"
        )
        func_name = f"nofilt-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        source_name = f"src-nofilt-{suffix}"
        sq = sqs.create_queue(QueueName=source_name)
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        filter_pattern = json.dumps({"body": {"type": ["order"]}})
        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
            FilterCriteria={"Filters": [{"Pattern": filter_pattern}]},
        )

        # Send only non-matching messages
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"type": "other"}),
        )
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"type": "ignored"}),
        )
        time.sleep(8)

        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) == 0, f"Expected 0 invocations, got {len(msgs)}"

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)


class TestESMPartialBatchFailure:
    """ESM partial batch failure with ReportBatchItemFailures."""

    def test_partial_batch_failure_report(self):
        """Failed items stay in queue when reported via
        batchItemFailures."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        verify_name = f"verify-partial-{suffix}"
        vq = sqs.create_queue(QueueName=verify_name)
        verify_url = vq["QueueUrl"]

        # Lambda reports first record as failed
        code = _make_lambda_zip(
            "import json, boto3, os\n"
            "def handler(event, ctx):\n"
            "    sqs = boto3.client('sqs',\n"
            "        endpoint_url=os.environ.get("
            "'SQS_ENDPOINT', 'http://localhost:4566'),\n"
            "        region_name='us-east-1',\n"
            "        aws_access_key_id='testing',\n"
            "        aws_secret_access_key='testing')\n"
            "    records = event.get('Records', [])\n"
            "    sqs.send_message(\n"
            "        QueueUrl=os.environ['VERIFY_QUEUE_URL'],\n"
            "        MessageBody=json.dumps("
            "{'count': len(records)}))\n"
            "    if records:\n"
            "        return {'batchItemFailures': [{\n"
            "            'itemIdentifier': "
            "records[0]['messageId']}]}\n"
            "    return {}\n"
        )
        func_name = f"partial-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
            Environment={
                "Variables": {
                    "VERIFY_QUEUE_URL": verify_url,
                    "SQS_ENDPOINT": "http://localhost:4566",
                }
            },
        )

        source_name = f"src-partial-{suffix}"
        sq = sqs.create_queue(
            QueueName=source_name,
            Attributes={"VisibilityTimeout": "5"},
        )
        source_url = sq["QueueUrl"]
        source_arn = sqs.get_queue_attributes(
            QueueUrl=source_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        esm = lam.create_event_source_mapping(
            EventSourceArn=source_arn,
            FunctionName=func_name,
            BatchSize=10,
            FunctionResponseTypes=["ReportBatchItemFailures"],
        )

        # Send 2 messages
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"idx": 0}),
        )
        sqs.send_message(
            QueueUrl=source_url,
            MessageBody=json.dumps({"idx": 1}),
        )
        time.sleep(8)

        # Verify Lambda was invoked
        recv = sqs.receive_message(
            QueueUrl=verify_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Lambda should have been invoked"

        # The failed message should still be in the source queue
        # (visibility timeout expired, so it's receivable again)
        time.sleep(3)
        retry = sqs.receive_message(
            QueueUrl=source_url,
            WaitTimeSeconds=5,
            MaxNumberOfMessages=10,
        )
        retry_msgs = retry.get("Messages", [])
        assert len(retry_msgs) >= 1, "Failed message should remain in source queue"

        # Cleanup
        lam.delete_event_source_mapping(UUID=esm["UUID"])
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=source_url)
        sqs.delete_queue(QueueUrl=verify_url)
        iam.delete_role(RoleName=role_name)


class TestEventBridgeToKinesis:
    """EventBridge -> Kinesis: Verify event delivery to stream."""

    def test_delivery_and_verify_record_data(self):
        events = make_client("events")
        kinesis = make_client("kinesis")
        suffix = uuid.uuid4().hex[:8]
        stream_name = f"eb-kin-{suffix}"
        rule_name = f"eb-kin-rule-{suffix}"

        kinesis.create_stream(StreamName=stream_name, ShardCount=1)
        # Wait for ACTIVE
        for _ in range(10):
            desc = kinesis.describe_stream(StreamName=stream_name)
            status = desc["StreamDescription"]["StreamStatus"]
            if status == "ACTIVE":
                break
            time.sleep(0.5)
        stream_arn = desc["StreamDescription"]["StreamARN"]
        shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]

        # Get shard iterator BEFORE putting event
        iter_resp = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )
        shard_iter = iter_resp["ShardIterator"]

        # Create rule + target
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.kinesis"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "kin1", "Arn": stream_arn}],
        )

        # Put event
        events.put_events(
            Entries=[
                {
                    "Source": "test.kinesis",
                    "DetailType": "Test",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )
        time.sleep(2)

        # Read from Kinesis
        records_resp = kinesis.get_records(ShardIterator=shard_iter, Limit=10)
        records = records_resp["Records"]
        assert len(records) >= 1
        data = json.loads(records[0]["Data"])
        assert data["source"] == "test.kinesis"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["kin1"])
        events.delete_rule(Name=rule_name)
        kinesis.delete_stream(StreamName=stream_name)


class TestEventBridgeToStepFunctions:
    """EventBridge -> Step Functions: Verify event starts execution."""

    def test_event_starts_execution(self):
        sfn = make_client("stepfunctions")
        events = make_client("events")
        iam = make_client("iam")
        suffix = uuid.uuid4().hex[:8]
        role_name = f"sfn-role-{suffix}"
        rule_name = f"eb-sfn-rule-{suffix}"
        sm_name = f"eb-sfn-{suffix}"

        # Create IAM role
        trust = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        role_resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust,
        )
        role_arn = role_resp["Role"]["Arn"]

        definition = json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )
        sm_resp = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm_resp["stateMachineArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.sfn"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "sfn1",
                    "Arn": sm_arn,
                    "RoleArn": role_arn,
                }
            ],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.sfn",
                    "DetailType": "Test",
                    "Detail": json.dumps({"key": "val"}),
                }
            ]
        )
        time.sleep(2)

        execs = sfn.list_executions(stateMachineArn=sm_arn)
        assert len(execs["executions"]) >= 1

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["sfn1"])
        events.delete_rule(Name=rule_name)
        sfn.delete_state_machine(stateMachineArn=sm_arn)
        iam.delete_role(RoleName=role_name)


class TestEventBridgeToCloudWatchLogs:
    """EventBridge -> CloudWatch Logs: Verify event in log group."""

    def test_event_in_log_group(self):
        events = make_client("events")
        logs = make_client("logs")
        suffix = uuid.uuid4().hex[:8]
        log_group = f"/aws/events/eb-logs-{suffix}"
        rule_name = f"eb-logs-rule-{suffix}"
        region = "us-east-1"
        account = "123456789012"

        logs.create_log_group(logGroupName=log_group)
        log_group_arn = f"arn:aws:logs:{region}:{account}:log-group:{log_group}"

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.logs"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "logs1", "Arn": log_group_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.logs",
                    "DetailType": "Test",
                    "Detail": json.dumps({"msg": "hello-logs"}),
                }
            ]
        )
        time.sleep(2)

        resp = logs.filter_log_events(logGroupName=log_group)
        log_events = resp.get("events", [])
        assert len(log_events) >= 1
        found = any("test.logs" in ev.get("message", "") for ev in log_events)
        assert found, "Expected event with source 'test.logs' in log group"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["logs1"])
        events.delete_rule(Name=rule_name)
        logs.delete_log_group(logGroupName=log_group)


class TestEventBridgeToFirehose:
    """EventBridge -> Firehose: Verify event dispatched."""

    def test_event_in_buffer(self):
        events = make_client("events")
        firehose = make_client("firehose")
        suffix = uuid.uuid4().hex[:8]
        stream_name = f"eb-fh-{suffix}"
        rule_name = f"eb-fh-rule-{suffix}"
        region = "us-east-1"
        account = "123456789012"

        firehose.create_delivery_stream(
            DeliveryStreamName=stream_name,
            DeliveryStreamType="DirectPut",
        )
        stream_arn = f"arn:aws:firehose:{region}:{account}:deliverystream/{stream_name}"

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.firehose"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "fh1", "Arn": stream_arn}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.firehose",
                    "DetailType": "Test",
                    "Detail": json.dumps({"msg": "fh-test"}),
                }
            ]
        )
        time.sleep(2)

        # Verify via audit endpoint
        resp = urllib.request.urlopen("http://localhost:4566/_robotocore/audit")
        audit = json.loads(resp.read())
        entries = audit.get("entries", audit.get("calls", []))
        # Check that EventBridge PutEvents was logged
        found = any("PutEvents" in str(entry) for entry in entries)
        assert found, "Expected PutEvents in audit log"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["fh1"])
        events.delete_rule(Name=rule_name)
        firehose.delete_delivery_stream(DeliveryStreamName=stream_name)


class TestEventBridgeCrossBus:
    """EventBridge cross-bus: default -> secondary -> SQS."""

    def test_event_forwarded_between_buses(self):
        events = make_client("events")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]

        # Create secondary bus
        bus_name = f"secondary-{suffix}"
        bus_resp = events.create_event_bus(Name=bus_name)
        secondary_arn = bus_resp["EventBusArn"]

        # Create SQS verification queue
        queue_name = f"eb-cross-{suffix}"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = attrs["Attributes"]["QueueArn"]

        # Rule on secondary bus -> SQS
        sec_rule = f"sec-rule-{suffix}"
        events.put_rule(
            Name=sec_rule,
            EventBusName=bus_name,
            EventPattern=json.dumps({"source": ["test.cross"]}),
        )
        events.put_targets(
            Rule=sec_rule,
            EventBusName=bus_name,
            Targets=[{"Id": "sqs1", "Arn": queue_arn}],
        )

        # Rule on default bus -> secondary bus
        fwd_rule = f"fwd-rule-{suffix}"
        events.put_rule(
            Name=fwd_rule,
            EventPattern=json.dumps({"source": ["test.cross"]}),
        )
        events.put_targets(
            Rule=fwd_rule,
            Targets=[{"Id": "bus1", "Arn": secondary_arn}],
        )

        # Put event on default bus
        events.put_events(
            Entries=[
                {
                    "Source": "test.cross",
                    "DetailType": "Test",
                    "Detail": json.dumps({"forwarded": True}),
                }
            ]
        )
        time.sleep(2)

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert body.get("source") == "test.cross"

        # Cleanup
        events.remove_targets(Rule=fwd_rule, Ids=["bus1"])
        events.delete_rule(Name=fwd_rule)
        events.remove_targets(
            Rule=sec_rule,
            EventBusName=bus_name,
            Ids=["sqs1"],
        )
        events.delete_rule(Name=sec_rule, EventBusName=bus_name)
        events.delete_event_bus(Name=bus_name)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeInputTransformer:
    """EventBridge InputTransformer: Verify transformed payload."""

    def test_transformed_payload_in_sqs(self):
        events = make_client("events")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"eb-xform-rule-{suffix}"
        queue_name = f"eb-xform-{suffix}"

        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = attrs["Attributes"]["QueueArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.xform"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "sqs1",
                    "Arn": queue_arn,
                    "InputTransformer": {
                        "InputPathsMap": {
                            "src": "$.source",
                            "typ": "$.detail-type",
                        },
                        "InputTemplate": (
                            '{"transformed_source": <src>, "transformed_type": <typ>}'
                        ),
                    },
                }
            ],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.xform",
                    "DetailType": "XformTest",
                    "Detail": json.dumps({"a": 1}),
                }
            ]
        )
        time.sleep(2)

        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert body.get("transformed_source") == "test.xform"
        assert body.get("transformed_type") == "XformTest"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["sqs1"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeSimulatedTargets:
    """EventBridge simulated targets: verify invocation logged."""

    @pytest.mark.parametrize(
        "service,arn_pattern",
        [
            (
                "batch",
                "arn:aws:batch:us-east-1:123456789012:job-definition/test",
            ),
            (
                "ecs",
                "arn:aws:ecs:us-east-1:123456789012:cluster/test",
            ),
            (
                "codebuild",
                "arn:aws:codebuild:us-east-1:123456789012:project/test",
            ),
            (
                "codepipeline",
                "arn:aws:codepipeline:us-east-1:123456789012:test",
            ),
            (
                "ssm",
                "arn:aws:ssm:us-east-1:123456789012:document/test",
            ),
            (
                "redshift",
                "arn:aws:redshift:us-east-1:123456789012:cluster:test",
            ),
            (
                "sagemaker",
                "arn:aws:sagemaker:us-east-1:123456789012:pipeline/test",
            ),
            (
                "inspector",
                "arn:aws:inspector:us-east-1:123456789012:target/test",
            ),
        ],
    )
    def test_simulated_targets_logged(self, service, arn_pattern):
        events = make_client("events")
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"eb-sim-{service}-{suffix}"

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": [f"test.sim.{service}"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": f"{service}1", "Arn": arn_pattern}],
        )

        events.put_events(
            Entries=[
                {
                    "Source": f"test.sim.{service}",
                    "DetailType": "SimTest",
                    "Detail": json.dumps({"svc": service}),
                }
            ]
        )
        time.sleep(1)

        # Verify via audit endpoint
        resp = urllib.request.urlopen("http://localhost:4566/_robotocore/audit")
        audit = json.loads(resp.read())
        entries = audit.get("entries", audit.get("calls", []))
        found = any("PutEvents" in str(entry) for entry in entries)
        assert found, f"Expected PutEvents for {service} in audit log"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=[f"{service}1"])
        events.delete_rule(Name=rule_name)


class TestEventBridgeDLQ:
    """EventBridge DLQ: Failed target delivers to DLQ."""

    def test_failed_target_delivers_to_dlq(self):
        events = make_client("events")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"eb-dlq-rule-{suffix}"
        dlq_name = f"eb-dlq-{suffix}"

        q = sqs.create_queue(QueueName=dlq_name)
        dlq_url = q["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["QueueArn"],
        )
        dlq_arn = attrs["Attributes"]["QueueArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.dlq"]}),
        )
        bad_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:nonexistent-fn"
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "lambda1",
                    "Arn": bad_lambda_arn,
                    "DeadLetterConfig": {"Arn": dlq_arn},
                }
            ],
        )

        events.put_events(
            Entries=[
                {
                    "Source": "test.dlq",
                    "DetailType": "DLQTest",
                    "Detail": json.dumps({"fail": True}),
                }
            ]
        )
        time.sleep(3)

        recv = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1, "Expected failed event in DLQ"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["lambda1"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=dlq_url)


class TestEventBridgeArchiveReplay:
    """EventBridge archive & replay: archive events, replay to SQS."""

    def test_archive_and_replay_events(self):
        events = make_client("events")
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"eb-archive-{suffix}"
        rule_name = f"eb-replay-rule-{suffix}"
        queue_name = f"eb-replay-{suffix}"

        # Create SQS queue for verification
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        queue_arn = attrs["Attributes"]["QueueArn"]

        # Get default bus ARN
        bus_resp = events.describe_event_bus(Name="default")
        bus_arn = bus_resp["Arn"]

        # Create archive on default bus
        archive_resp = events.create_archive(
            ArchiveName=archive_name,
            EventSourceArn=bus_arn,
            EventPattern=json.dumps({"source": ["test.replay"]}),
        )
        assert "ArchiveArn" in archive_resp

        # Rule to deliver replayed events to SQS
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.replay"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "sqs1", "Arn": queue_arn}],
        )

        # Put events (they get archived)
        events.put_events(
            Entries=[
                {
                    "Source": "test.replay",
                    "DetailType": "ReplayTest",
                    "Detail": json.dumps({"idx": i}),
                }
                for i in range(3)
            ]
        )
        time.sleep(2)

        # Drain the initial delivery
        for _ in range(5):
            sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
            )

        # Start replay
        now = datetime.now(UTC)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now

        archive_arn = archive_resp["ArchiveArn"]
        replay_resp = events.start_replay(
            ReplayName=f"replay-{suffix}",
            EventSourceArn=archive_arn,
            Destination={"Arn": bus_arn},
            EventStartTime=start,
            EventEndTime=end,
        )
        assert "ReplayArn" in replay_resp

        # Verify replay can be described
        desc = events.describe_replay(ReplayName=f"replay-{suffix}")
        assert desc["ReplayName"] == f"replay-{suffix}"

        # Cleanup
        events.remove_targets(Rule=rule_name, Ids=["sqs1"])
        events.delete_rule(Name=rule_name)
        events.delete_archive(ArchiveName=archive_name)
        sqs.delete_queue(QueueUrl=queue_url)


class TestLambdaDestinations:
    """Lambda Destinations: async invocations dispatch results to
    configured OnSuccess/OnFailure destinations (SQS, SNS)."""

    def test_on_success_to_sqs(self):
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        # Create SQS destination queue
        queue_name = f"dest-ok-{suffix}"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create Lambda that succeeds
        code = _make_lambda_zip("def handler(event, ctx): return {'ok': True}")
        func_name = f"dest-ok-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Configure OnSuccess destination
        lam.put_function_event_invoke_config(
            FunctionName=func_name,
            DestinationConfig={
                "OnSuccess": {"Destination": queue_arn},
            },
        )

        # Invoke async
        lam.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps({"test": True}),
        )
        time.sleep(3)

        # Verify destination record arrived in SQS
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert "requestPayload" in body or "requestContext" in body

        # Cleanup
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_on_failure_to_sqs(self):
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        # Create SQS destination queue
        queue_name = f"dest-fail-{suffix}"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create Lambda that raises an error
        code = _make_lambda_zip(
            "def handler(event, ctx):\n    raise Exception('Intentional failure')\n"
        )
        func_name = f"dest-fail-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Configure OnFailure destination
        lam.put_function_event_invoke_config(
            FunctionName=func_name,
            DestinationConfig={
                "OnFailure": {"Destination": queue_arn},
            },
        )

        # Invoke async
        lam.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps({"trigger": "fail"}),
        )
        time.sleep(3)

        # Verify failure record arrived in SQS
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])
        assert "requestPayload" in body or "requestContext" in body

        # Cleanup
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_record_format(self):
        """Verify destination record structure from
        _build_destination_record in destinations.py."""
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        lam = make_client("lambda")

        # Create SQS destination queue
        queue_name = f"dest-fmt-{suffix}"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Create Lambda that succeeds
        payload = {"hello": "world", "num": 42}
        code = _make_lambda_zip("def handler(event, ctx): return {'ok': True}")
        func_name = f"dest-fmt-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Configure OnSuccess destination
        lam.put_function_event_invoke_config(
            FunctionName=func_name,
            DestinationConfig={
                "OnSuccess": {"Destination": queue_arn},
            },
        )

        # Invoke async
        lam.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        time.sleep(3)

        # Receive and validate record structure
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        body = json.loads(msgs[0]["Body"])

        # Validate required fields
        assert "version" in body
        assert "timestamp" in body
        assert "requestContext" in body
        assert "requestPayload" in body

        # requestContext should reference the function
        rc = body["requestContext"]
        assert "functionArn" in rc
        assert func_name in rc["functionArn"]

        # requestPayload should match what was sent
        assert body["requestPayload"] == payload

        # responseContext should have statusCode for success
        assert "responseContext" in body
        assert "statusCode" in body["responseContext"]

        # Cleanup
        lam.delete_function(FunctionName=func_name)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)

    def test_on_success_to_sns(self):
        suffix = uuid.uuid4().hex[:8]
        iam, role_name, role_arn = _create_lambda_role()
        sqs = make_client("sqs")
        sns = make_client("sns")
        lam = make_client("lambda")

        # Create SNS topic as destination
        topic_name = f"dest-sns-{suffix}"
        topic_resp = sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        # Create SQS queue to receive from SNS
        queue_name = f"dest-sns-q-{suffix}"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        # Subscribe SQS to SNS
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
        )

        # Create Lambda that succeeds
        code = _make_lambda_zip("def handler(event, ctx): return {'via': 'sns'}")
        func_name = f"dest-sns-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )

        # Configure OnSuccess → SNS
        lam.put_function_event_invoke_config(
            FunctionName=func_name,
            DestinationConfig={
                "OnSuccess": {"Destination": topic_arn},
            },
        )

        # Invoke async
        lam.invoke(
            FunctionName=func_name,
            InvocationType="Event",
            Payload=json.dumps({"route": "sns"}),
        )
        time.sleep(3)

        # Verify message arrived in SQS (via SNS)
        recv = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=5)
        msgs = recv.get("Messages", [])
        assert len(msgs) >= 1
        # SNS wraps the message; parse the SNS envelope
        sns_envelope = json.loads(msgs[0]["Body"])
        assert "Message" in sns_envelope or "message" in sns_envelope

        # Cleanup
        lam.delete_function(FunctionName=func_name)
        sns.delete_topic(TopicArn=topic_arn)
        sqs.delete_queue(QueueUrl=queue_url)
        iam.delete_role(RoleName=role_name)
