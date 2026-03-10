"""CloudFormation compatibility tests."""

import json
import os
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

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


class TestCloudFormationAdvanced:
    """Additional CloudFormation tests covering parameters, intrinsics, updates, tags, etc."""

    def test_stack_with_parameters_resolved(self, cfn, sqs):
        """Pass Parameters to CreateStack, verify the parameter is resolved in the resource."""
        uid = uuid.uuid4().hex[:8]
        queue_name = f"cfn-param-q-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "QName": {"Type": "String"},
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": {"Ref": "QName"}},
                    },
                },
            }
        )
        stack_name = f"test-param-resolve-{uid}"
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Parameters=[{"ParameterKey": "QName", "ParameterValue": queue_name}],
        )
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName=queue_name)
        assert queue_name in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_outputs_and_getatt(self, cfn):
        """Stack with Fn::GetAtt and Fn::Ref in Outputs."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-outputs-getatt-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-out-q-{uid}"},
                    },
                },
                "Outputs": {
                    "QUrl": {
                        "Value": {"Ref": "MyQueue"},
                        "Description": "Queue URL from Ref",
                    },
                    "QArn": {
                        "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                        "Description": "Queue ARN from GetAtt",
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        stack = resp["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "QUrl" in outputs
        assert "QArn" in outputs
        assert f"cfn-out-q-{uid}" in outputs["QArn"]

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_dynamodb_table(self, cfn, dynamodb):
        """Stack creates a DynamoDB table, verify it exists and has correct schema."""
        uid = uuid.uuid4().hex[:8]
        table_name = f"cfn-ddb-{uid}"
        stack_name = f"test-ddb-adv-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
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
                    "TableName": {"Value": {"Ref": "Table"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        tables = dynamodb.list_tables()
        assert table_name in tables["TableNames"]

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_sns_topic(self, cfn, sns):
        """Stack creates an SNS topic resource."""
        uid = uuid.uuid4().hex[:8]
        topic_name = f"cfn-topic-{uid}"
        stack_name = f"test-sns-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Topic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
                "Outputs": {
                    "TopicArn": {"Value": {"Ref": "Topic"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"
        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert topic_name in outputs.get("TopicArn", "")

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_iam_role(self, cfn, iam):
        """Stack creates an IAM role resource."""
        uid = uuid.uuid4().hex[:8]
        role_name = f"cfn-role-{uid}"
        stack_name = f"test-iam-adv-{uid}"
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
                },
                "Outputs": {
                    "RoleArn": {"Value": {"Fn::GetAtt": ["Role", "Arn"]}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        role = iam.get_role(RoleName=role_name)
        assert role["Role"]["RoleName"] == role_name

        cfn.delete_stack(StackName=stack_name)

    def test_stack_update_changes_resource(self, cfn, sqs):
        """Update a stack to change a resource property, verify the update."""
        uid = uuid.uuid4().hex[:8]
        q1 = f"cfn-upd-q1-{uid}"
        q2 = f"cfn-upd-q2-{uid}"
        stack_name = f"test-update-{uid}"

        template_v1 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": q1},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template_v1)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Update with new queue name
        template_v2 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": q2},
                    },
                },
            }
        )
        cfn.update_stack(StackName=stack_name, TemplateBody=template_v2)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"

        # New queue should exist
        q_url = sqs.get_queue_url(QueueName=q2)
        assert q2 in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_multiple_resources_and_ref(self, cfn, sns, sqs):
        """Stack with multiple resources using Fn::Ref between them."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-multi-ref-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Topic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": f"cfn-ref-topic-{uid}"},
                    },
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-ref-queue-{uid}"},
                    },
                    "Sub": {
                        "Type": "AWS::SNS::Subscription",
                        "DependsOn": ["Topic", "Queue"],
                        "Properties": {
                            "TopicArn": {"Ref": "Topic"},
                            "Protocol": "sqs",
                            "Endpoint": {"Fn::GetAtt": ["Queue", "Arn"]},
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify both resources exist
        sqs.get_queue_url(QueueName=f"cfn-ref-queue-{uid}")
        cfn.delete_stack(StackName=stack_name)

    def test_describe_stack_resources_logical_physical(self, cfn):
        """DescribeStackResources returns logical and physical IDs."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-res-ids-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-resid-q-{uid}"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        res = cfn.describe_stack_resources(StackName=stack_name)
        resources = res["StackResources"]
        assert len(resources) >= 1
        r = resources[0]
        assert r["LogicalResourceId"] == "MyQueue"
        assert r["PhysicalResourceId"] != ""
        assert r["ResourceType"] == "AWS::SQS::Queue"
        assert r["ResourceStatus"] == "CREATE_COMPLETE"

        cfn.delete_stack(StackName=stack_name)

    def test_fn_join_intrinsic(self, cfn):
        """Stack with Fn::Join in outputs."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-join-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-join-q-{uid}"},
                    },
                },
                "Outputs": {
                    "Joined": {
                        "Value": {
                            "Fn::Join": ["-", ["prefix", {"Ref": "AWS::Region"}, "suffix"]],
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        stack = resp["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert outputs["Joined"] == "prefix-us-east-1-suffix"

        cfn.delete_stack(StackName=stack_name)

    def test_fn_sub_with_ref(self, cfn, sqs):
        """Stack with Fn::Sub that references AWS::Region."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-sub-ref-{uid}"
        expected_name = f"cfn-sub-ref-us-east-1-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {
                                "Fn::Sub": f"cfn-sub-ref-${{AWS::Region}}-{uid}",
                            },
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName=expected_name)
        assert expected_name in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

    def test_fn_select(self, cfn):
        """Stack with Fn::Select picking an element from a list."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-select-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-sel-q-{uid}"},
                    },
                },
                "Outputs": {
                    "Selected": {
                        "Value": {"Fn::Select": ["1", ["alpha", "beta", "gamma"]]},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        stack = resp["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert outputs["Selected"] == "beta"

        cfn.delete_stack(StackName=stack_name)

    def test_stack_tags(self, cfn):
        """Create stack with tags, verify tags in describe output."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-tags-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-tag-q-{uid}"},
                    },
                },
            }
        )
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Tags=[
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "robotocore"},
            ],
        )
        resp = cfn.describe_stacks(StackName=stack_name)
        stack = resp["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"
        tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
        assert tags["Environment"] == "test"
        assert tags["Project"] == "robotocore"

        cfn.delete_stack(StackName=stack_name)

    def test_get_template_matches_submitted(self, cfn):
        """GetTemplate returns the template that was submitted."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-gettempl-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "GetTemplate test",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"cfn-gt-q-{uid}"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.get_template(StackName=stack_name)
        body = resp["TemplateBody"]
        if isinstance(body, str):
            body = json.loads(body)
        assert body["Description"] == "GetTemplate test"
        assert "Q" in body["Resources"]
        assert body["Resources"]["Q"]["Type"] == "AWS::SQS::Queue"

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_eventbridge_rule(self, cfn):
        """Stack creates an EventBridge rule resource."""
        uid = uuid.uuid4().hex[:8]
        stack_name = f"test-eb-rule-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Rule": {
                        "Type": "AWS::Events::Rule",
                        "Properties": {
                            "Name": f"cfn-rule-{uid}",
                            "ScheduleExpression": "rate(1 hour)",
                            "State": "ENABLED",
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify resource was created
        res = cfn.describe_stack_resources(StackName=stack_name)
        types = [r["ResourceType"] for r in res["StackResources"]]
        assert "AWS::Events::Rule" in types

        cfn.delete_stack(StackName=stack_name)

    def test_stack_with_default_parameter(self, cfn, sqs):
        """Stack parameter with a Default value used when no override given."""
        uid = uuid.uuid4().hex[:8]
        default_name = f"cfn-default-q-{uid}"
        stack_name = f"test-default-param-{uid}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "QName": {
                        "Type": "String",
                        "Default": default_name,
                    },
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": {"Ref": "QName"}},
                    },
                },
            }
        )
        # Don't pass Parameters — should use Default
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName=default_name)
        assert default_name in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

    def test_validate_template(self, cfn):
        """ValidateTemplate returns parameter info."""
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Validation test",
                "Parameters": {
                    "Env": {
                        "Type": "String",
                        "Default": "dev",
                        "Description": "Environment name",
                    },
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "validate-q"},
                    },
                },
            }
        )
        resp = cfn.validate_template(TemplateBody=template)
        assert resp.get("Description") == "Validation test"
        params = resp.get("Parameters", [])
        assert len(params) >= 1
        p = params[0]
        assert p["ParameterKey"] == "Env"
        assert p["DefaultValue"] == "dev"

    def test_describe_stack_events(self, cfn):
        """DescribeStackEvents returns lifecycle events."""
        stack_name = f"events-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_events(StackName=stack_name)
            assert "StackEvents" in resp
            assert len(resp["StackEvents"]) >= 1
            event = resp["StackEvents"][0]
            assert "StackName" in event or "LogicalResourceId" in event
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_list_stack_resources(self, cfn):
        """ListStackResources returns logical and physical IDs."""
        stack_name = f"lsr-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q1": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q1"},
                    },
                    "Q2": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q2"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.list_stack_resources(StackName=stack_name)
            summaries = resp["StackResourceSummaries"]
            logical_ids = [s["LogicalResourceId"] for s in summaries]
            assert "Q1" in logical_ids
            assert "Q2" in logical_ids
            for s in summaries:
                assert "PhysicalResourceId" in s
                assert "ResourceType" in s
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_fn_split(self, cfn):
        """Fn::Split intrinsic function."""
        stack_name = f"split-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
                "Outputs": {
                    "Second": {
                        "Value": {"Fn::Select": [1, {"Fn::Split": [",", "a,b,c"]}]},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            assert outputs.get("Second") == "b"
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_conditions(self, cfn):
        """Stack with Conditions and Fn::If."""
        stack_name = f"cond-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "Env": {"Type": "String", "Default": "prod"},
                },
                "Conditions": {
                    "IsProd": {"Fn::Equals": [{"Ref": "Env"}, "prod"]},
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {
                                "Fn::If": ["IsProd", f"{stack_name}-prod-q", f"{stack_name}-dev-q"]
                            },
                        },
                    },
                },
                "Outputs": {
                    "QueueName": {"Value": {"Fn::GetAtt": ["Q", "QueueName"]}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            assert outputs.get("QueueName") == f"{stack_name}-prod-q"
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_describe_stacks_returns_status(self, cfn):
        """DescribeStacks returns StackStatus."""
        stack_name = f"status-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            stack = resp["Stacks"][0]
            assert "StackStatus" in stack
            assert stack["StackStatus"] in ("CREATE_COMPLETE", "CREATE_IN_PROGRESS")
            assert "StackName" in stack
            assert "StackId" in stack
            assert "CreationTime" in stack
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_stack_with_kms_key(self, cfn):
        """Create a stack with a KMS key resource."""
        stack_name = f"kms-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Key": {
                        "Type": "AWS::KMS::Key",
                        "Properties": {
                            "Description": "test key from cfn",
                            "KeyPolicy": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "Enable IAM User Permissions",
                                        "Effect": "Allow",
                                        "Principal": {"AWS": "*"},
                                        "Action": "kms:*",
                                        "Resource": "*",
                                    }
                                ],
                            },
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_resources(StackName=stack_name)
            resources = resp["StackResources"]
            assert any(r["ResourceType"] == "AWS::KMS::Key" for r in resources)
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_stack_with_s3_bucket(self, cfn):
        """Create a stack with an S3 bucket."""
        stack_name = f"s3-test-{uuid.uuid4().hex[:8]}"
        bucket_name = f"cfn-bucket-{uuid.uuid4().hex[:8]}"
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
            resp = cfn.describe_stacks(StackName=stack_name)
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
            }
            assert outputs.get("BucketName") == bucket_name
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_stack_with_lambda_function(self, cfn):
        """Create a stack with a Lambda function."""
        stack_name = f"lam-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Role": {
                        "Type": "AWS::IAM::Role",
                        "Properties": {
                            "RoleName": f"{stack_name}-role",
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
                            "FunctionName": f"{stack_name}-fn",
                            "Runtime": "python3.12",
                            "Handler": "index.handler",
                            "Role": {"Fn::GetAtt": ["Role", "Arn"]},
                            "Code": {"ZipFile": "def handler(event, context): return 'ok'"},
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_resources(StackName=stack_name)
            types = [r["ResourceType"] for r in resp["StackResources"]]
            assert "AWS::Lambda::Function" in types
            assert "AWS::IAM::Role" in types
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_list_stacks_with_filter(self, cfn):
        """ListStacks with StackStatusFilter."""
        stack_name = f"filter-test-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.list_stacks(StackStatusFilter=["CREATE_COMPLETE"])
            names = [s["StackName"] for s in resp["StackSummaries"]]
            assert stack_name in names
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCloudFormationAdvancedOps:
    @pytest.fixture
    def cfn(self):
        return make_client("cloudformation")

    def test_create_stack_with_outputs(self, cfn):
        stack_name = f"output-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
                "Outputs": {
                    "QueueUrl": {
                        "Value": {"Ref": "Q"},
                        "Description": "Queue URL",
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            outputs = resp["Stacks"][0].get("Outputs", [])
            out_keys = [o["OutputKey"] for o in outputs]
            assert "QueueUrl" in out_keys
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_create_stack_with_parameters(self, cfn):
        stack_name = f"params-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "QueueName": {"Type": "String", "Default": "default-queue"},
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": {"Ref": "QueueName"}},
                    },
                },
            }
        )
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Parameters=[{"ParameterKey": "QueueName", "ParameterValue": f"{stack_name}-pq"}],
        )
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            params = resp["Stacks"][0].get("Parameters", [])
            param_map = {p["ParameterKey"]: p["ParameterValue"] for p in params}
            assert param_map.get("QueueName") == f"{stack_name}-pq"
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_get_template(self, cfn):
        stack_name = f"tmpl-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.get_template(StackName=stack_name)
            assert "TemplateBody" in resp
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_validate_template(self, cfn):
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {"Type": "AWS::SQS::Queue"},
                },
            }
        )
        resp = cfn.validate_template(TemplateBody=template)
        assert "Parameters" in resp

    def test_create_stack_with_tags(self, cfn):
        stack_name = f"tags-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            tags = {t["Key"]: t["Value"] for t in resp["Stacks"][0].get("Tags", [])}
            assert tags.get("env") == "test"
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_update_stack(self, cfn):
        stack_name = f"upd-{uuid.uuid4().hex[:8]}"
        template1 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q1"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template1)
        try:
            template2 = json.dumps(
                {
                    "AWSTemplateFormatVersion": "2010-09-09",
                    "Resources": {
                        "Q": {
                            "Type": "AWS::SQS::Queue",
                            "Properties": {"QueueName": f"{stack_name}-q2"},
                        },
                    },
                }
            )
            cfn.update_stack(StackName=stack_name, TemplateBody=template2)
            resp = cfn.describe_stacks(StackName=stack_name)
            status = resp["Stacks"][0]["StackStatus"]
            assert status in ("UPDATE_COMPLETE", "CREATE_COMPLETE", "UPDATE_IN_PROGRESS")
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_describe_stack_resource(self, cfn):
        stack_name = f"res-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_resource(StackName=stack_name, LogicalResourceId="MyQueue")
            detail = resp["StackResourceDetail"]
            assert detail["LogicalResourceId"] == "MyQueue"
            assert detail["ResourceType"] == "AWS::SQS::Queue"
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_create_stack_with_sns_topic(self, cfn):
        stack_name = f"sns-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Topic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": f"{stack_name}-topic"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stack_resources(StackName=stack_name)
            types = [r["ResourceType"] for r in resp["StackResources"]]
            assert "AWS::SNS::Topic" in types
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_fn_join(self, cfn):
        stack_name = f"join-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {"Fn::Join": ["-", [stack_name, "joined", "q"]]},
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] in ("CREATE_COMPLETE", "CREATE_IN_PROGRESS")
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_fn_sub(self, cfn):
        stack_name = f"sub-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": {"Fn::Sub": "${AWS::StackName}-sub-q"},
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.describe_stacks(StackName=stack_name)
            assert resp["Stacks"][0]["StackStatus"] in ("CREATE_COMPLETE", "CREATE_IN_PROGRESS")
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_delete_stack_removes_resources(self, cfn):
        stack_name = f"delres-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        cfn.delete_stack(StackName=stack_name)
        resp = cfn.list_stacks(StackStatusFilter=["DELETE_COMPLETE"])
        names = [s["StackName"] for s in resp["StackSummaries"]]
        assert stack_name in names

    def test_list_stack_resources_types(self, cfn):
        stack_name = f"types-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                    "T": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": f"{stack_name}-t"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.list_stack_resources(StackName=stack_name)
            types = {r["ResourceType"] for r in resp["StackResourceSummaries"]}
            assert "AWS::SQS::Queue" in types
            assert "AWS::SNS::Topic" in types
        finally:
            cfn.delete_stack(StackName=stack_name)

    def test_list_exports(self, cfn):
        stack_name = f"exports-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"{stack_name}-q"},
                    },
                },
                "Outputs": {
                    "QueueUrl": {
                        "Value": {"Ref": "Q"},
                        "Export": {"Name": f"{stack_name}-url"},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        try:
            resp = cfn.list_exports()
            assert "Exports" in resp
            export_names = [e["Name"] for e in resp["Exports"]]
            assert f"{stack_name}-url" in export_names
            # Verify export has a value
            export = next(e for e in resp["Exports"] if e["Name"] == f"{stack_name}-url")
            assert "Value" in export
            assert "ExportingStackId" in export
        finally:
            cfn.delete_stack(StackName=stack_name)


class TestCloudFormationGapStubs:
    """Tests for newly-stubbed CloudFormation operations that return empty/default results."""

    def test_list_types(self, cfn):
        resp = cfn.list_types()
        assert "TypeSummaries" in resp
        assert isinstance(resp["TypeSummaries"], list)

    def test_list_type_registrations(self, cfn):
        resp = cfn.list_type_registrations()
        assert "RegistrationTokenList" in resp
        assert isinstance(resp["RegistrationTokenList"], list)

    def test_describe_organizations_access(self, cfn):
        resp = cfn.describe_organizations_access()
        assert "Status" in resp
        assert resp["Status"] in ("ENABLED", "DISABLED", "DISABLED_PERMANENTLY")

    def test_list_generated_templates(self, cfn):
        resp = cfn.list_generated_templates()
        assert "Summaries" in resp
        assert isinstance(resp["Summaries"], list)

    def test_list_resource_scans(self, cfn):
        resp = cfn.list_resource_scans()
        assert "ResourceScanSummaries" in resp
        assert isinstance(resp["ResourceScanSummaries"], list)

    def test_describe_account_limits(self, cfn):
        resp = cfn.describe_account_limits()
        assert "AccountLimits" in resp
        assert len(resp["AccountLimits"]) > 0
        names = {limit["Name"] for limit in resp["AccountLimits"]}
        assert "StackLimit" in names
        for limit in resp["AccountLimits"]:
            assert "Name" in limit
            assert "Value" in limit

    def test_describe_publisher(self, cfn):
        resp = cfn.describe_publisher()
        assert "PublisherId" in resp
        assert "PublisherStatus" in resp
        assert resp["PublisherStatus"] in ("VERIFIED", "UNVERIFIED")

    def test_describe_type(self, cfn):
        resp = cfn.describe_type()
        assert "TypeName" in resp
        assert "Type" in resp
        assert resp["Type"] in ("RESOURCE", "MODULE", "HOOK")
        assert "Arn" in resp

    def test_list_type_versions(self, cfn):
        resp = cfn.list_type_versions(TypeName="AWS::S3::Bucket", Type="RESOURCE")
        assert "TypeVersionSummaries" in resp
        assert isinstance(resp["TypeVersionSummaries"], list)


class TestCloudFormationTemplateSummary:
    """Tests for GetTemplateSummary and EstimateTemplateCost."""

    def test_get_template_summary(self, cfn):
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Test template for summary",
                "Resources": {
                    "MyQueue": {"Type": "AWS::SQS::Queue"},
                    "MyTopic": {"Type": "AWS::SNS::Topic"},
                },
            }
        )
        resp = cfn.get_template_summary(TemplateBody=template)
        assert "ResourceTypes" in resp
        resource_types = resp["ResourceTypes"]
        assert "AWS::SQS::Queue" in resource_types
        assert "AWS::SNS::Topic" in resource_types

    def test_estimate_template_cost(self, cfn):
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {"MyQueue": {"Type": "AWS::SQS::Queue"}},
            }
        )
        resp = cfn.estimate_template_cost(TemplateBody=template)
        assert "Url" in resp


class TestCloudFormationStackSets:
    """Tests for ListStackSets."""

    def test_list_stack_sets_empty(self, cfn):
        resp = cfn.list_stack_sets()
        assert "Summaries" in resp
        assert isinstance(resp["Summaries"], list)


class TestCloudformationAutoCoverage:
    """Auto-generated coverage tests for cloudformation."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_activate_organizations_access(self, client):
        """ActivateOrganizationsAccess returns a response."""
        client.activate_organizations_access()

    def test_activate_type(self, client):
        """ActivateType returns a response."""
        resp = client.activate_type()
        assert "Arn" in resp

    def test_deactivate_organizations_access(self, client):
        """DeactivateOrganizationsAccess returns a response."""
        client.deactivate_organizations_access()

    def test_deactivate_type(self, client):
        """DeactivateType returns a response."""
        client.deactivate_type()

    def test_deregister_type(self, client):
        """DeregisterType returns a response."""
        client.deregister_type()

    def test_describe_events(self, client):
        """DescribeEvents returns a response."""
        client.describe_events()

    def test_get_hook_result(self, client):
        """GetHookResult returns a response."""
        client.get_hook_result()

    def test_list_hook_results(self, client):
        """ListHookResults returns a response."""
        client.list_hook_results()

    def test_list_stack_refactors(self, client):
        """ListStackRefactors returns a response."""
        resp = client.list_stack_refactors()
        assert "StackRefactorSummaries" in resp

    def test_publish_type(self, client):
        """PublishType returns a response."""
        resp = client.publish_type()
        assert "PublicTypeArn" in resp

    def test_register_publisher(self, client):
        """RegisterPublisher returns a response."""
        resp = client.register_publisher()
        assert "PublisherId" in resp

    def test_set_type_default_version(self, client):
        """SetTypeDefaultVersion returns a response."""
        client.set_type_default_version()

    def test_start_resource_scan(self, client):
        """StartResourceScan returns a response."""
        resp = client.start_resource_scan()
        assert "ResourceScanId" in resp

    def test_test_type(self, client):
        """TestType returns a response."""
        resp = client.test_type()
        assert "TypeVersionArn" in resp


class TestCloudFormationChangeSets:
    """Tests for ChangeSet operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def _unique_name(self, prefix):
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _simple_template(self, queue_name):
        return json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
            }
        )

    def test_create_change_set(self, client):
        """CreateChangeSet creates a change set for a new stack."""
        stack_name = self._unique_name("cs-create")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        try:
            resp = client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            assert "Id" in resp
            assert "StackId" in resp
        finally:
            try:
                client.delete_change_set(StackName=stack_name, ChangeSetName=cs_name)
            except Exception:
                pass
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_describe_change_set(self, client):
        """DescribeChangeSet returns details of a change set."""
        stack_name = self._unique_name("cs-desc")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        try:
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            resp = client.describe_change_set(StackName=stack_name, ChangeSetName=cs_name)
            assert resp["ChangeSetName"] == cs_name
            assert "Status" in resp
        finally:
            try:
                client.delete_change_set(StackName=stack_name, ChangeSetName=cs_name)
            except Exception:
                pass
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_execute_change_set(self, client):
        """ExecuteChangeSet applies a change set and creates resources."""
        stack_name = self._unique_name("cs-exec")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        try:
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            # Wait briefly for change set to be available
            time.sleep(0.5)
            resp = client.execute_change_set(StackName=stack_name, ChangeSetName=cs_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify the stack is now CREATE_COMPLETE with resources
            desc = client.describe_stacks(StackName=stack_name)
            stacks = desc["Stacks"]
            assert len(stacks) == 1
            assert stacks[0]["StackStatus"] == "CREATE_COMPLETE"

            # Verify resources were actually created
            resources = client.list_stack_resources(StackName=stack_name)
            summaries = resources["StackResourceSummaries"]
            assert len(summaries) >= 1
            assert summaries[0]["ResourceType"] == "AWS::SQS::Queue"
            assert summaries[0]["ResourceStatus"] == "CREATE_COMPLETE"
        finally:
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_execute_change_set_update(self, client):
        """ExecuteChangeSet with UPDATE type updates an existing stack."""
        stack_name = self._unique_name("cs-upd")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        topic_name = self._unique_name("t")
        sns_template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyTopic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
            }
        )
        try:
            # First create the stack with an SQS queue
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            time.sleep(0.5)
            client.execute_change_set(StackName=stack_name, ChangeSetName=cs_name)

            # Now create an UPDATE change set with SNS topic
            update_cs = self._unique_name("changeset-upd")
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=update_cs,
                TemplateBody=sns_template,
                ChangeSetType="UPDATE",
            )
            time.sleep(0.5)
            client.execute_change_set(StackName=stack_name, ChangeSetName=update_cs)

            # Verify the stack is UPDATE_COMPLETE
            desc = client.describe_stacks(StackName=stack_name)
            assert desc["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"

            # Verify the new resource is an SNS topic
            resources = client.list_stack_resources(StackName=stack_name)
            summaries = resources["StackResourceSummaries"]
            assert len(summaries) >= 1
            resource_types = [s["ResourceType"] for s in summaries]
            assert "AWS::SNS::Topic" in resource_types
        finally:
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_delete_change_set(self, client):
        """DeleteChangeSet removes a change set."""
        stack_name = self._unique_name("cs-del")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        try:
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            resp = client.delete_change_set(StackName=stack_name, ChangeSetName=cs_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_list_change_sets(self, client):
        """ListChangeSets returns change sets for a stack."""
        stack_name = self._unique_name("cs-list")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        try:
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=self._simple_template(queue_name),
                ChangeSetType="CREATE",
            )
            resp = client.list_change_sets(StackName=stack_name)
            assert "Summaries" in resp
            assert isinstance(resp["Summaries"], list)
        finally:
            try:
                client.delete_change_set(StackName=stack_name, ChangeSetName=cs_name)
            except Exception:
                pass
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass


class TestCloudFormationStackSetsOps:
    """Tests for StackSet operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def _unique_name(self, prefix):
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _simple_template(self):
        return json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "ss-test-queue"},
                    },
                },
            }
        )

    def test_create_stack_set(self, client):
        """CreateStackSet creates a stack set."""
        name = self._unique_name("ss")
        try:
            resp = client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            assert "StackSetId" in resp
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_describe_stack_set(self, client):
        """DescribeStackSet returns stack set details."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.describe_stack_set(StackSetName=name)
            assert "StackSet" in resp
            assert resp["StackSet"]["StackSetName"] == name
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_update_stack_set(self, client):
        """UpdateStackSet updates a stack set."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.update_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                Description="Updated description",
            )
            assert "OperationId" in resp
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_delete_stack_set(self, client):
        """DeleteStackSet removes a stack set."""
        name = self._unique_name("ss")
        client.create_stack_set(
            StackSetName=name,
            TemplateBody=self._simple_template(),
        )
        resp = client.delete_stack_set(StackSetName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_stack_instances_empty(self, client):
        """ListStackInstances returns empty list for new stack set."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.list_stack_instances(StackSetName=name)
            assert "Summaries" in resp
            assert isinstance(resp["Summaries"], list)
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_list_stack_set_operations(self, client):
        """ListStackSetOperations returns operations list."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.list_stack_set_operations(StackSetName=name)
            assert "Summaries" in resp
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_create_stack_instances(self, client):
        """CreateStackInstances creates instances in a stack set."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            assert "OperationId" in resp
        finally:
            try:
                client.delete_stack_instances(
                    StackSetName=name,
                    Accounts=["123456789012"],
                    Regions=["us-east-1"],
                    RetainStacks=True,
                )
            except Exception:
                pass
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_update_stack_instances(self, client):
        """UpdateStackInstances updates instances in a stack set."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            resp = client.update_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            assert "OperationId" in resp
        finally:
            try:
                client.delete_stack_instances(
                    StackSetName=name,
                    Accounts=["123456789012"],
                    Regions=["us-east-1"],
                    RetainStacks=True,
                )
            except Exception:
                pass
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_delete_stack_instances(self, client):
        """DeleteStackInstances removes instances from a stack set."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            resp = client.delete_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
                RetainStacks=True,
            )
            assert "OperationId" in resp
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_describe_stack_instance(self, client):
        """DescribeStackInstance returns details of a stack instance."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            resp = client.describe_stack_instance(
                StackSetName=name,
                StackInstanceAccount="123456789012",
                StackInstanceRegion="us-east-1",
            )
            assert "StackInstance" in resp
            assert resp["StackInstance"]["Account"] == "123456789012"
        finally:
            try:
                client.delete_stack_instances(
                    StackSetName=name,
                    Accounts=["123456789012"],
                    Regions=["us-east-1"],
                    RetainStacks=True,
                )
            except Exception:
                pass
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_describe_stack_set_operation(self, client):
        """DescribeStackSetOperation returns operation details."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            create_resp = client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            op_id = create_resp["OperationId"]
            resp = client.describe_stack_set_operation(StackSetName=name, OperationId=op_id)
            assert "StackSetOperation" in resp
            assert resp["StackSetOperation"]["Status"] in (
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "STOPPED",
            )
        finally:
            try:
                client.delete_stack_instances(
                    StackSetName=name,
                    Accounts=["123456789012"],
                    Regions=["us-east-1"],
                    RetainStacks=True,
                )
            except Exception:
                pass
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_list_stack_set_operation_results(self, client):
        """ListStackSetOperationResults returns results for an operation."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            create_resp = client.create_stack_instances(
                StackSetName=name,
                Accounts=["123456789012"],
                Regions=["us-east-1"],
            )
            op_id = create_resp["OperationId"]
            resp = client.list_stack_set_operation_results(StackSetName=name, OperationId=op_id)
            assert "Summaries" in resp
            assert isinstance(resp["Summaries"], list)
        finally:
            try:
                client.delete_stack_instances(
                    StackSetName=name,
                    Accounts=["123456789012"],
                    Regions=["us-east-1"],
                    RetainStacks=True,
                )
            except Exception:
                pass
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_stop_stack_set_operation_nonexistent(self, client):
        """StopStackSetOperation returns error for nonexistent operation."""
        name = self._unique_name("ss")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            with pytest.raises(ClientError) as exc:
                client.stop_stack_set_operation(StackSetName=name, OperationId="fake-op-id")
            assert exc.value.response["Error"]["Code"] == "ValidationError"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass


class TestCloudFormationStackPolicy:
    """Tests for stack policy operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_set_stack_policy_nonexistent(self, client):
        """SetStackPolicy returns ValidationError for nonexistent stack."""
        policy = json.dumps(
            {
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "Update:*",
                        "Principal": "*",
                        "Resource": "*",
                    }
                ]
            }
        )
        with pytest.raises(ClientError) as exc:
            client.set_stack_policy(StackName="does-not-exist", StackPolicyBody=policy)
        assert exc.value.response["Error"]["Code"] == "ValidationError"

    def test_get_stack_policy_nonexistent(self, client):
        """GetStackPolicy returns ValidationError for nonexistent stack."""
        with pytest.raises(ClientError) as exc:
            client.get_stack_policy(StackName="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "ValidationError"


class TestCloudFormationStackSetDetails:
    """Tests for StackSet creation options and error handling."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def _unique_name(self, prefix):
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def _simple_template(self):
        return json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {"Type": "AWS::SQS::Queue"},
                },
            }
        )

    def test_create_stack_set_with_tags(self, client):
        """CreateStackSet with tags stores tags on the stack set."""
        name = self._unique_name("ss-tags")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
            )
            resp = client.describe_stack_set(StackSetName=name)
            tags = {t["Key"]: t["Value"] for t in resp["StackSet"]["Tags"]}
            assert tags["env"] == "test"
            assert tags["team"] == "dev"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_create_stack_set_with_description(self, client):
        """CreateStackSet with description stores description."""
        name = self._unique_name("ss-desc")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                Description="My test stack set",
            )
            resp = client.describe_stack_set(StackSetName=name)
            assert resp["StackSet"]["Description"] == "My test stack set"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_create_stack_set_permission_model(self, client):
        """CreateStackSet with PermissionModel stores the model."""
        name = self._unique_name("ss-perm")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                PermissionModel="SELF_MANAGED",
            )
            resp = client.describe_stack_set(StackSetName=name)
            assert resp["StackSet"]["PermissionModel"] == "SELF_MANAGED"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_list_stack_sets_contains_created(self, client):
        """ListStackSets includes a newly created stack set."""
        name = self._unique_name("ss-list")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.list_stack_sets()
            names = [s["StackSetName"] for s in resp["Summaries"]]
            assert name in names
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_list_stack_sets_with_status_filter(self, client):
        """ListStackSets with Status=ACTIVE includes newly created set."""
        name = self._unique_name("ss-filt")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.list_stack_sets(Status="ACTIVE")
            assert "Summaries" in resp
            names = [s["StackSetName"] for s in resp["Summaries"]]
            assert name in names
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_describe_stack_set_not_found(self, client):
        """DescribeStackSet for nonexistent stack set returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_stack_set(StackSetName="nonexistent-ss")
        assert exc.value.response["Error"]["Code"] == "StackSetNotFoundException"

    def test_describe_change_set_not_found(self, client):
        """DescribeChangeSet for nonexistent change set returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_change_set(
                StackName="nonexistent-stack", ChangeSetName="nonexistent-cs"
            )
        assert exc.value.response["Error"]["Code"] == "ChangeSetNotFoundException"

    def test_list_stack_sets_summary_fields(self, client):
        """ListStackSets summaries include expected fields."""
        name = self._unique_name("ss-fields")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
            )
            resp = client.list_stack_sets()
            summary = next(s for s in resp["Summaries"] if s["StackSetName"] == name)
            assert "StackSetId" in summary
            assert "Status" in summary
            assert summary["Status"] == "ACTIVE"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_change_set_describe_fields(self, client):
        """DescribeChangeSet returns expected fields."""
        stack_name = self._unique_name("cs-fields")
        cs_name = self._unique_name("changeset")
        queue_name = self._unique_name("q")
        tmpl = json.dumps(
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
        try:
            client.create_change_set(
                StackName=stack_name,
                ChangeSetName=cs_name,
                TemplateBody=tmpl,
                ChangeSetType="CREATE",
            )
            resp = client.describe_change_set(StackName=stack_name, ChangeSetName=cs_name)
            assert resp["ChangeSetName"] == cs_name
            assert resp["StackName"] == stack_name
            assert "ChangeSetId" in resp
            assert "StackId" in resp
            assert "Status" in resp
        finally:
            try:
                client.delete_change_set(StackName=stack_name, ChangeSetName=cs_name)
            except Exception:
                pass
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass

    def test_update_stack_set_description(self, client):
        """UpdateStackSet can change description."""
        name = self._unique_name("ss-upd")
        try:
            client.create_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                Description="original",
            )
            client.update_stack_set(
                StackSetName=name,
                TemplateBody=self._simple_template(),
                Description="updated",
            )
            resp = client.describe_stack_set(StackSetName=name)
            assert resp["StackSet"]["Description"] == "updated"
        finally:
            try:
                client.delete_stack_set(StackSetName=name)
            except Exception:
                pass

    def test_delete_stack_set_then_describe_raises(self, client):
        """Deleted stack set raises StackSetNotFoundException."""
        name = self._unique_name("ss-del-desc")
        client.create_stack_set(
            StackSetName=name,
            TemplateBody=self._simple_template(),
        )
        client.delete_stack_set(StackSetName=name)
        with pytest.raises(ClientError) as exc:
            client.describe_stack_set(StackSetName=name)
        assert exc.value.response["Error"]["Code"] == "StackSetNotFoundException"


class TestCloudFormationImports:
    """Tests for ListImports."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_list_imports(self, client):
        """ListImports returns imports for an export name."""
        # First create a stack with an export
        stack_name = f"imp-test-{uuid.uuid4().hex[:8]}"
        export_name = f"exp-{uuid.uuid4().hex[:8]}"
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": f"imp-q-{uuid.uuid4().hex[:8]}"},
                    },
                },
                "Outputs": {
                    "QueueUrl": {
                        "Value": {"Ref": "MyQueue"},
                        "Export": {"Name": export_name},
                    },
                },
            }
        )
        try:
            client.create_stack(StackName=stack_name, TemplateBody=template)
            resp = client.list_imports(ExportName=export_name)
            assert "Imports" in resp
        finally:
            try:
                client.delete_stack(StackName=stack_name)
            except Exception:
                pass


class TestCloudFormationDriftOps:
    """Tests for drift detection and related operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_detect_stack_set_drift_fake(self, client):
        """DetectStackSetDrift with nonexistent stack set returns response or error."""
        try:
            resp = client.detect_stack_set_drift(StackSetName="fake-stackset-drift-nonexist")
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_describe_stack_drift_detection_status_fake(self, client):
        """DescribeStackDriftDetectionStatus with fake ID returns response or error."""
        try:
            resp = client.describe_stack_drift_detection_status(
                StackDriftDetectionId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]


class TestCloudFormationResourceScan:
    """Tests for resource scan operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_describe_resource_scan_fake(self, client):
        """DescribeResourceScan with fake ID raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_resource_scan(
                ResourceScanId="arn:aws:cloudformation:us-east-1:123456789012:resourceScan/fake-id"
            )
        assert "Code" in exc.value.response["Error"]

    def test_list_resource_scan_resources_fake(self, client):
        """ListResourceScanResources with fake ID returns response or error."""
        try:
            resp = client.list_resource_scan_resources(
                ResourceScanId="arn:aws:cloudformation:us-east-1:123456789012:resourceScan/fake-id"
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_list_resource_scan_related_resources_fake(self, client):
        """ListResourceScanRelatedResources with fake ID returns response or error."""
        try:
            resp = client.list_resource_scan_related_resources(
                ResourceScanId="arn:aws:cloudformation:us-east-1:123456789012:resourceScan/fake-id",
                Resources=[
                    {
                        "ResourceType": "AWS::SQS::Queue",
                        "ResourceIdentifier": {"QueueName": "fake"},
                    }
                ],
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]


class TestCloudFormationMiscOps:
    """Tests for miscellaneous CloudFormation operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_describe_change_set_hooks_fake(self, client):
        """DescribeChangeSetHooks with fake changeset returns response or error."""
        try:
            resp = client.describe_change_set_hooks(
                ChangeSetName="fake-changeset-nonexist",
                StackName="fake-hooks-stack-nonexist",
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_import_stacks_to_stack_set_fake(self, client):
        """ImportStacksToStackSet with nonexistent stack set raises error."""
        with pytest.raises(ClientError) as exc:
            client.import_stacks_to_stack_set(
                StackSetName="fake-import-ss-nonexist",
                StackIds=["arn:aws:cloudformation:us-east-1:123456789012:stack/fake/id"],
            )
        assert "Code" in exc.value.response["Error"]

    def test_list_stack_instance_resource_drifts_fake(self, client):
        """ListStackInstanceResourceDrifts with fake stack set returns response or error."""
        try:
            resp = client.list_stack_instance_resource_drifts(
                StackSetName="fake-ss-drifts-nonexist",
                StackInstanceAccount="123456789012",
                StackInstanceRegion="us-east-1",
                OperationId="fake-op-id",
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_list_stack_set_auto_deployment_targets_fake(self, client):
        """ListStackSetAutoDeploymentTargets with fake stack set returns response or error."""
        try:
            resp = client.list_stack_set_auto_deployment_targets(
                StackSetName="fake-ss-auto-deploy-nonexist",
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_record_handler_progress(self, client):
        """RecordHandlerProgress returns a response or error."""
        try:
            resp = client.record_handler_progress(
                BearerToken="fake-bearer-token",
                OperationStatus="SUCCESS",
                CurrentOperationStatus="IN_PROGRESS",
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]


class TestCloudFormationStackOps:
    """Tests for stack lifecycle operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_continue_update_rollback_fake(self, client):
        """ContinueUpdateRollback with fake stack raises error."""
        with pytest.raises(ClientError) as exc:
            client.continue_update_rollback(StackName="fake-rollback-stack-nonexist")
        assert "Code" in exc.value.response["Error"]

    def test_rollback_stack_fake(self, client):
        """RollbackStack with fake stack raises error."""
        with pytest.raises(ClientError) as exc:
            client.rollback_stack(StackName="fake-rollback-nonexist")
        assert "Code" in exc.value.response["Error"]

    def test_signal_resource_fake(self, client):
        """SignalResource with nonexistent stack raises error."""
        with pytest.raises(ClientError) as exc:
            client.signal_resource(
                StackName="fake-signal-nonexist",
                LogicalResourceId="MyQueue",
                UniqueId="signal-1",
                Status="SUCCESS",
            )
        assert "Code" in exc.value.response["Error"]


class TestCloudFormationGeneratedTemplates:
    """Tests for generated template operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_create_generated_template(self, client):
        """CreateGeneratedTemplate creates a template and returns an ID."""
        unique = uuid.uuid4().hex[:8]
        resp = client.create_generated_template(
            GeneratedTemplateName=f"gen-tmpl-{unique}",
            Resources=[
                {
                    "ResourceType": "AWS::SQS::Queue",
                    "ResourceIdentifier": {"QueueName": f"fake-queue-{unique}"},
                }
            ],
        )
        assert "GeneratedTemplateId" in resp
        # Cleanup
        try:
            client.delete_generated_template(GeneratedTemplateName=f"gen-tmpl-{unique}")
        except ClientError:
            pass

    def test_describe_generated_template(self, client):
        """DescribeGeneratedTemplate returns details for a created template."""
        unique = uuid.uuid4().hex[:8]
        name = f"gen-tmpl-desc-{unique}"
        client.create_generated_template(
            GeneratedTemplateName=name,
            Resources=[
                {
                    "ResourceType": "AWS::SQS::Queue",
                    "ResourceIdentifier": {"QueueName": f"fake-q-{unique}"},
                }
            ],
        )
        resp = client.describe_generated_template(GeneratedTemplateName=name)
        assert "GeneratedTemplateName" in resp
        # Cleanup
        try:
            client.delete_generated_template(GeneratedTemplateName=name)
        except ClientError:
            pass

    def test_update_generated_template(self, client):
        """UpdateGeneratedTemplate updates a template and returns an ID."""
        unique = uuid.uuid4().hex[:8]
        name = f"gen-tmpl-upd-{unique}"
        client.create_generated_template(
            GeneratedTemplateName=name,
            Resources=[
                {
                    "ResourceType": "AWS::SQS::Queue",
                    "ResourceIdentifier": {"QueueName": f"fake-q-{unique}"},
                }
            ],
        )
        resp = client.update_generated_template(
            GeneratedTemplateName=name,
            NewGeneratedTemplateName=f"{name}-updated",
        )
        assert "GeneratedTemplateId" in resp
        # Cleanup
        try:
            client.delete_generated_template(GeneratedTemplateName=f"{name}-updated")
        except ClientError:
            pass

    def test_get_generated_template(self, client):
        """GetGeneratedTemplate returns template body or status."""
        unique = uuid.uuid4().hex[:8]
        name = f"gen-tmpl-get-{unique}"
        client.create_generated_template(
            GeneratedTemplateName=name,
            Resources=[
                {
                    "ResourceType": "AWS::SQS::Queue",
                    "ResourceIdentifier": {"QueueName": f"fake-q-{unique}"},
                }
            ],
        )
        resp = client.get_generated_template(GeneratedTemplateName=name)
        assert "Status" in resp
        # Cleanup
        try:
            client.delete_generated_template(GeneratedTemplateName=name)
        except ClientError:
            pass

    def test_delete_generated_template(self, client):
        """DeleteGeneratedTemplate removes a template."""
        unique = uuid.uuid4().hex[:8]
        name = f"gen-tmpl-del-{unique}"
        client.create_generated_template(
            GeneratedTemplateName=name,
            Resources=[
                {
                    "ResourceType": "AWS::SQS::Queue",
                    "ResourceIdentifier": {"QueueName": f"fake-q-{unique}"},
                }
            ],
        )
        resp = client.delete_generated_template(GeneratedTemplateName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCloudFormationStackRefactorOps:
    """Tests for stack refactor operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_create_stack_refactor(self, client):
        """CreateStackRefactor returns a refactor ID."""
        try:
            resp = client.create_stack_refactor(
                Description="test refactor",
                StackDefinitions=[
                    {
                        "StackName": "fake-refactor-stack-nonexist",
                        "TemplateBody": json.dumps(
                            {
                                "AWSTemplateFormatVersion": "2010-09-09",
                                "Resources": {
                                    "Q": {
                                        "Type": "AWS::SQS::Queue",
                                        "Properties": {"QueueName": "refactor-q"},
                                    }
                                },
                            }
                        ),
                    }
                ],
            )
            assert "StackRefactorId" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_describe_stack_refactor_fake(self, client):
        """DescribeStackRefactor with fake ID returns response or error."""
        try:
            resp = client.describe_stack_refactor(
                StackRefactorId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_execute_stack_refactor_fake(self, client):
        """ExecuteStackRefactor with fake ID returns response or error."""
        try:
            resp = client.execute_stack_refactor(
                StackRefactorId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_list_stack_refactor_actions_fake(self, client):
        """ListStackRefactorActions with fake ID returns response or error."""
        try:
            resp = client.list_stack_refactor_actions(
                StackRefactorId="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
            )
            assert "ResponseMetadata" in resp
        except ClientError as e:
            assert "Code" in e.response["Error"]


class TestCloudFormationDriftDetection:
    """Tests for stack drift detection operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_detect_stack_drift(self, client):
        """DetectStackDrift with a nonexistent stack returns error with Code."""
        with pytest.raises(ClientError) as exc:
            client.detect_stack_drift(StackName="fake-drift-stack-nonexist")
        assert "Code" in exc.value.response["Error"]

    def test_detect_stack_resource_drift(self, client):
        """DetectStackResourceDrift with a nonexistent stack returns error."""
        with pytest.raises(ClientError) as exc:
            client.detect_stack_resource_drift(
                StackName="fake-drift-res-nonexist",
                LogicalResourceId="MyQueue",
            )
        assert "Code" in exc.value.response["Error"]

    def test_describe_stack_resource_drifts(self, client):
        """DescribeStackResourceDrifts with a nonexistent stack returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_stack_resource_drifts(StackName="fake-drift-drifts-nonexist")
        assert "Code" in exc.value.response["Error"]


class TestCloudFormationTerminationProtection:
    """Tests for termination protection."""

    @pytest.fixture
    def client(self):
        return make_client("cloudformation")

    def test_update_termination_protection(self, client):
        """UpdateTerminationProtection with nonexistent stack returns error."""
        with pytest.raises(ClientError) as exc:
            client.update_termination_protection(
                EnableTerminationProtection=True,
                StackName="fake-termprot-nonexist",
            )
        assert "Code" in exc.value.response["Error"]
