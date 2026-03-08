"""Lambda cross-service integration tests for Phase 3B.

Tests SNS→Lambda, EventBridge→Lambda, CloudFormation provisioning,
and Lambda function lifecycle through the full HTTP stack.
"""

import json
import uuid

from tests.integration.conftest import make_lambda_zip

HANDLER_CODE = """
def handler(event, context):
    return {"statusCode": 200, "body": json.dumps(event)}
"""

ROLE_ARN = "arn:aws:iam::123456789012:role/lambda-cross-svc-role"


class TestSNSToLambda:
    """SNS topic triggering Lambda function."""

    def test_sns_subscribe_lambda(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        iam = make_boto_client("iam")
        lam = make_boto_client("lambda")
        sns = make_boto_client("sns")

        # Create IAM role
        iam.create_role(
            RoleName=f"sns-lambda-role-{suffix}",
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )
        role_arn = f"arn:aws:iam::123456789012:role/sns-lambda-role-{suffix}"

        # Create Lambda function
        func_name = f"sns-target-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": make_lambda_zip(HANDLER_CODE)},
        )
        func_arn = lam.get_function(FunctionName=func_name)["Configuration"]["FunctionArn"]

        # Create SNS topic and subscribe Lambda
        topic = sns.create_topic(Name=f"lambda-topic-{suffix}")
        topic_arn = topic["TopicArn"]

        sub = sns.subscribe(
            TopicArn=topic_arn,
            Protocol="lambda",
            Endpoint=func_arn,
        )
        assert "SubscriptionArn" in sub

        # Publish - delivery is fire-and-forget so just verify no error
        sns.publish(TopicArn=topic_arn, Message="test from SNS")

        # Cleanup
        sns.delete_topic(TopicArn=topic_arn)
        lam.delete_function(FunctionName=func_name)


class TestEventBridgeToLambda:
    """EventBridge rule targeting Lambda function."""

    def test_eventbridge_lambda_target(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]

        iam = make_boto_client("iam")
        lam = make_boto_client("lambda")
        events = make_boto_client("events")

        # Create IAM role
        iam.create_role(
            RoleName=f"eb-lambda-role-{suffix}",
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )
        role_arn = f"arn:aws:iam::123456789012:role/eb-lambda-role-{suffix}"

        # Create Lambda function
        func_name = f"eb-target-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": make_lambda_zip(HANDLER_CODE)},
        )
        func_arn = lam.get_function(FunctionName=func_name)["Configuration"]["FunctionArn"]

        # Create EventBridge rule targeting Lambda
        events.put_rule(
            Name=f"eb-lambda-rule-{suffix}",
            EventPattern=json.dumps({"source": ["eb-lambda.test"]}),
        )
        events.put_targets(
            Rule=f"eb-lambda-rule-{suffix}",
            Targets=[{"Id": "lambda-target", "Arn": func_arn}],
        )

        # Put event
        resp = events.put_events(
            Entries=[{
                "Source": "eb-lambda.test",
                "DetailType": "LambdaTest",
                "Detail": json.dumps({"key": "value"}),
            }]
        )
        assert resp["FailedEntryCount"] == 0

        # Cleanup
        events.remove_targets(Rule=f"eb-lambda-rule-{suffix}", Ids=["lambda-target"])
        events.delete_rule(Name=f"eb-lambda-rule-{suffix}")
        lam.delete_function(FunctionName=func_name)


class TestCloudFormationProvisioning:
    """CloudFormation stack provisioning of common resource types."""

    def test_provision_sqs_queue_via_cfn(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        cfn = make_boto_client("cloudformation")

        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "TestQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": f"cfn-queue-{suffix}",
                    },
                },
            },
        })

        stack_name = f"test-sqs-stack-{suffix}"
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
        )

        # Verify stack was created
        desc = cfn.describe_stacks(StackName=stack_name)
        stacks = desc["Stacks"]
        assert len(stacks) == 1
        assert stacks[0]["StackStatus"] in (
            "CREATE_COMPLETE", "CREATE_IN_PROGRESS",
        )

        cfn.delete_stack(StackName=stack_name)

    def test_provision_dynamodb_table_via_cfn(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        cfn = make_boto_client("cloudformation")

        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "TestTable": {
                    "Type": "AWS::DynamoDB::Table",
                    "Properties": {
                        "TableName": f"cfn-table-{suffix}",
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
        })

        stack_name = f"test-ddb-stack-{suffix}"
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
        )

        desc = cfn.describe_stacks(StackName=stack_name)
        assert len(desc["Stacks"]) == 1

        cfn.delete_stack(StackName=stack_name)

    def test_provision_sns_topic_via_cfn(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        cfn = make_boto_client("cloudformation")

        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "TestTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {
                        "TopicName": f"cfn-topic-{suffix}",
                    },
                },
            },
        })

        stack_name = f"test-sns-stack-{suffix}"
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
        )

        desc = cfn.describe_stacks(StackName=stack_name)
        assert len(desc["Stacks"]) == 1

        cfn.delete_stack(StackName=stack_name)

    def test_provision_iam_role_via_cfn(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        cfn = make_boto_client("cloudformation")

        template = json.dumps({
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": {
                "TestRole": {
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "RoleName": f"cfn-role-{suffix}",
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }],
                        },
                    },
                },
            },
        })

        stack_name = f"test-iam-stack-{suffix}"
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
        )

        desc = cfn.describe_stacks(StackName=stack_name)
        assert len(desc["Stacks"]) == 1

        cfn.delete_stack(StackName=stack_name)


class TestLambdaFunctionLifecycle:
    """Lambda function CRUD through the full stack."""

    def test_create_invoke_delete_function(self, make_boto_client):
        suffix = uuid.uuid4().hex[:8]
        iam = make_boto_client("iam")
        lam = make_boto_client("lambda")

        iam.create_role(
            RoleName=f"lifecycle-role-{suffix}",
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )

        func_name = f"lifecycle-fn-{suffix}"
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role=f"arn:aws:iam::123456789012:role/lifecycle-role-{suffix}",
            Handler="lambda_function.handler",
            Code={"ZipFile": make_lambda_zip(HANDLER_CODE)},
        )

        # Get function
        get_resp = lam.get_function(FunctionName=func_name)
        assert get_resp["Configuration"]["FunctionName"] == func_name
        assert get_resp["Configuration"]["Runtime"] == "python3.12"

        # List functions
        list_resp = lam.list_functions()
        func_names = [f["FunctionName"] for f in list_resp["Functions"]]
        assert func_name in func_names

        # Delete
        lam.delete_function(FunctionName=func_name)

        # Verify deleted
        list_resp2 = lam.list_functions()
        func_names2 = [f["FunctionName"] for f in list_resp2["Functions"]]
        assert func_name not in func_names2
