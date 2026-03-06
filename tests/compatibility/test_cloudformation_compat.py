"""CloudFormation compatibility tests."""

import json
import os
import time

import boto3
import pytest

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def cfn():
    return boto3.client(
        "cloudformation",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def sns():
    return boto3.client(
        "sns",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def dynamodb():
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def ssm():
    return boto3.client(
        "ssm",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def iam():
    return boto3.client(
        "iam",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


SIMPLE_SQS_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Simple SQS queue",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cfn-test-queue",
                },
            },
        },
        "Outputs": {
            "QueueUrl": {
                "Value": {"Ref": "MyQueue"},
                "Description": "Queue URL",
            },
            "QueueArn": {
                "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                "Description": "Queue ARN",
            },
        },
    }
)

SQS_SNS_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "SQS + SNS with subscription",
        "Resources": {
            "MyTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {
                    "TopicName": "cfn-test-topic",
                },
            },
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cfn-test-sub-queue",
                },
            },
            "MySub": {
                "Type": "AWS::SNS::Subscription",
                "DependsOn": ["MyTopic", "MyQueue"],
                "Properties": {
                    "TopicArn": {"Ref": "MyTopic"},
                    "Protocol": "sqs",
                    "Endpoint": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                },
            },
        },
    }
)

PARAMETERIZED_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {
                "Type": "String",
                "Default": "default-queue",
            },
        },
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": {"Ref": "QueueName"},
                },
            },
        },
    }
)

DYNAMODB_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyTable": {
                "Type": "AWS::DynamoDB::Table",
                "Properties": {
                    "TableName": "cfn-test-table",
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
        "Outputs": {
            "TableArn": {
                "Value": {"Fn::GetAtt": ["MyTable", "Arn"]},
            },
            "TableName": {
                "Value": {"Ref": "MyTable"},
            },
        },
    }
)

IAM_ROLE_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": "cfn-test-role",
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
        },
        "Outputs": {
            "RoleArn": {
                "Value": {"Fn::GetAtt": ["MyRole", "Arn"]},
            },
        },
    }
)

SSM_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyParam": {
                "Type": "AWS::SSM::Parameter",
                "Properties": {
                    "Name": "/cfn/test/param",
                    "Type": "String",
                    "Value": "cfn-param-value",
                },
            },
        },
    }
)

MULTI_RESOURCE_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Multi-resource stack with Fn::Sub",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {
                    "QueueName": "cfn-multi-queue",
                },
            },
            "MyBucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": "cfn-multi-bucket",
                },
            },
            "MyLogGroup": {
                "Type": "AWS::Logs::LogGroup",
                "Properties": {
                    "LogGroupName": "/cfn/multi/logs",
                },
            },
        },
        "Outputs": {
            "QueueArn": {
                "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
            },
            "BucketArn": {
                "Value": {"Fn::GetAtt": ["MyBucket", "Arn"]},
            },
            "CombinedInfo": {
                "Value": {
                    "Fn::Join": [
                        "|",
                        [
                            {"Fn::GetAtt": ["MyQueue", "QueueName"]},
                            {"Ref": "MyBucket"},
                        ],
                    ]
                },
            },
        },
    }
)


class TestCloudFormationBasic:
    def test_create_stack(self, cfn, sqs):
        cfn.create_stack(
            StackName="test-simple-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-simple-stack")
        stack = response["Stacks"][0]
        assert stack["StackName"] == "test-simple-stack"
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Verify queue was actually created
        q_url = sqs.get_queue_url(QueueName="cfn-test-queue")
        assert "cfn-test-queue" in q_url["QueueUrl"]

        cfn.delete_stack(StackName="test-simple-stack")

    def test_stack_outputs(self, cfn):
        cfn.create_stack(
            StackName="test-outputs-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-outputs-stack")
        stack = response["Stacks"][0]
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "QueueUrl" in outputs
        assert "QueueArn" in outputs
        assert "cfn-test-queue" in outputs["QueueArn"]

        cfn.delete_stack(StackName="test-outputs-stack")

    def test_delete_stack(self, cfn, sqs):
        cfn.create_stack(
            StackName="test-delete-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        cfn.delete_stack(StackName="test-delete-stack")

        # Queue should be deleted
        with pytest.raises(Exception):
            sqs.get_queue_url(QueueName="cfn-test-queue")

    def test_list_stacks(self, cfn):
        cfn.create_stack(
            StackName="test-list-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.list_stacks()
        names = [s["StackName"] for s in response["StackSummaries"]]
        assert "test-list-stack" in names
        cfn.delete_stack(StackName="test-list-stack")

    def test_describe_stack_resources(self, cfn):
        cfn.create_stack(
            StackName="test-resources-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stack_resources(StackName="test-resources-stack")
        resources = response["StackResources"]
        assert len(resources) >= 1
        assert resources[0]["ResourceType"] == "AWS::SQS::Queue"
        assert resources[0]["ResourceStatus"] == "CREATE_COMPLETE"
        cfn.delete_stack(StackName="test-resources-stack")

    def test_get_template(self, cfn):
        cfn.create_stack(
            StackName="test-get-template-stack",
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.get_template(StackName="test-get-template-stack")
        body = response["TemplateBody"]
        if isinstance(body, str):
            body = json.loads(body)
        assert "Resources" in body
        assert "MyQueue" in body["Resources"]
        cfn.delete_stack(StackName="test-get-template-stack")


class TestCloudFormationCrossService:
    def test_sqs_sns_subscription(self, cfn, sns, sqs):
        cfn.create_stack(
            StackName="test-cross-stack",
            TemplateBody=SQS_SNS_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-cross-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify SNS->SQS subscription works
        sns.publish(
            TopicArn="arn:aws:sns:us-east-1:123456789012:cfn-test-topic",
            Message="cfn integration test",
        )
        time.sleep(0.5)
        q_url = sqs.get_queue_url(QueueName="cfn-test-sub-queue")["QueueUrl"]
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1

        cfn.delete_stack(StackName="test-cross-stack")

    def test_parameterized_stack(self, cfn, sqs):
        cfn.create_stack(
            StackName="test-param-stack",
            TemplateBody=PARAMETERIZED_TEMPLATE,
            Parameters=[
                {"ParameterKey": "QueueName", "ParameterValue": "custom-queue-name"},
            ],
        )
        response = cfn.describe_stacks(StackName="test-param-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName="custom-queue-name")
        assert "custom-queue-name" in q_url["QueueUrl"]

        cfn.delete_stack(StackName="test-param-stack")


class TestCloudFormationResourceTypes:
    def test_dynamodb_table(self, cfn, dynamodb):
        cfn.create_stack(
            StackName="test-ddb-stack",
            TemplateBody=DYNAMODB_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-ddb-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify table exists
        tables = dynamodb.list_tables()
        assert "cfn-test-table" in tables["TableNames"]

        # Verify outputs
        outputs = {
            o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0].get("Outputs", [])
        }
        assert "cfn-test-table" in outputs.get("TableArn", "")

        cfn.delete_stack(StackName="test-ddb-stack")

    def test_iam_role(self, cfn, iam):
        cfn.create_stack(
            StackName="test-iam-stack",
            TemplateBody=IAM_ROLE_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-iam-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify role exists
        role = iam.get_role(RoleName="cfn-test-role")
        assert role["Role"]["RoleName"] == "cfn-test-role"

        outputs = {
            o["OutputKey"]: o["OutputValue"] for o in response["Stacks"][0].get("Outputs", [])
        }
        assert "cfn-test-role" in outputs.get("RoleArn", "")

        cfn.delete_stack(StackName="test-iam-stack")

    def test_ssm_parameter(self, cfn, ssm):
        cfn.create_stack(
            StackName="test-ssm-stack",
            TemplateBody=SSM_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-ssm-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify parameter exists
        param = ssm.get_parameter(Name="/cfn/test/param")
        assert param["Parameter"]["Value"] == "cfn-param-value"

        cfn.delete_stack(StackName="test-ssm-stack")

    def test_multi_resource_stack(self, cfn):
        cfn.create_stack(
            StackName="test-multi-stack",
            TemplateBody=MULTI_RESOURCE_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName="test-multi-stack")
        stack = response["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Verify outputs with Fn::Join
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "cfn-multi-queue" in outputs.get("QueueArn", "")
        assert "cfn-multi-bucket" in outputs.get("BucketArn", "")
        assert "|" in outputs.get("CombinedInfo", "")

        # Verify resources
        res = cfn.describe_stack_resources(StackName="test-multi-stack")
        types = {r["ResourceType"] for r in res["StackResources"]}
        assert "AWS::SQS::Queue" in types
        assert "AWS::S3::Bucket" in types
        assert "AWS::Logs::LogGroup" in types

        cfn.delete_stack(StackName="test-multi-stack")

    def test_fn_sub_intrinsic(self, cfn, sqs):
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {"Fn::Sub": "cfn-sub-${AWS::Region}-queue"},
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName="test-sub-stack", TemplateBody=template)
        response = cfn.describe_stacks(StackName="test-sub-stack")
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName="cfn-sub-us-east-1-queue")
        assert "cfn-sub-us-east-1-queue" in q_url["QueueUrl"]

        cfn.delete_stack(StackName="test-sub-stack")
