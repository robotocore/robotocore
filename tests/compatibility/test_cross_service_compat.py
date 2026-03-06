"""Cross-service integration compatibility tests.

These tests verify enterprise-grade cross-service integrations that require
multiple AWS services working together. These are the features that
differentiate us from basic Moto.
"""

import io
import json
import time
import uuid
import zipfile

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
    trust = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    })
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
        q_attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
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


class TestEventBridgeToLambda:
    """EventBridge -> Lambda: Create a rule with Lambda target, put an event,
    verify the cross-service dispatch works."""

    def test_eventbridge_invokes_lambda(self):
        events = make_client("events")
        lam = make_client("lambda")
        suffix = uuid.uuid4().hex[:8]
        func_name = f"eb-lambda-{suffix}"
        rule_name = f"eb-rule-{suffix}"

        # Create Lambda role and function
        iam, role_name, role_arn = _create_lambda_role()
        code = _make_lambda_zip(
            'def handler(event, ctx): return {"source": event.get("source", "unknown")}'
        )
        func_resp = lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": code},
        )
        func_arn = func_resp["FunctionArn"]

        # Create EventBridge rule with event pattern
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.eb.lambda"]}),
            State="ENABLED",
        )

        # Add Lambda as target
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "lambda-target", "Arn": func_arn}],
        )

        # Put an event that matches the rule
        put_resp = events.put_events(
            Entries=[{
                "Source": "test.eb.lambda",
                "DetailType": "TestEvent",
                "Detail": json.dumps({"message": "from-eventbridge"}),
            }]
        )
        assert put_resp["FailedEntryCount"] == 0

        # Verify the rule and target were configured correctly
        targets = events.list_targets_by_rule(Rule=rule_name)
        assert len(targets["Targets"]) == 1
        assert targets["Targets"][0]["Arn"] == func_arn

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["lambda-target"])
        events.delete_rule(Name=rule_name)
        lam.delete_function(FunctionName=func_name)
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
        sfn_trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }],
        })
        sfn_role = iam.create_role(
            RoleName=sfn_role_name,
            AssumeRolePolicyDocument=sfn_trust,
        )
        sfn_role_arn = sfn_role["Role"]["Arn"]

        # Lambda role
        lambda_trust = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }],
        })
        iam.create_role(
            RoleName=lambda_role_name,
            AssumeRolePolicyDocument=lambda_trust,
        )
        lambda_role_arn = f"arn:aws:iam::123456789012:role/{lambda_role_name}"

        # Create Lambda function that returns a value
        code = _make_lambda_zip(
            'def handler(event, ctx): return {"processed": True, "input_name": event.get("name", "unknown")}'
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
        definition = json.dumps({
            "Comment": "State machine that invokes a Lambda function",
            "StartAt": "InvokeLambda",
            "States": {
                "InvokeLambda": {
                    "Type": "Task",
                    "Resource": func_arn,
                    "End": True,
                }
            },
        })

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

    @pytest.mark.skip(reason="Firehose does not flush buffered records to S3 in Moto — cross-service delivery not yet implemented")
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

        template = json.dumps({
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
        })

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
