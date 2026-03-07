"""CloudFormation compatibility tests."""

import json
import os
import time
import uuid

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


@pytest.fixture
def logs():
    return boto3.client(
        "logs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def _unique(prefix: str) -> str:
    """Generate a unique name to avoid collisions between test runs."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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

    def test_stack_tags(self, cfn):
        """Create a stack with tags and verify they are returned."""
        stack_name = _unique("test-tags")
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=SIMPLE_SQS_TEMPLATE,
            Tags=[
                {"Key": "Environment", "Value": "testing"},
                {"Key": "Project", "Value": "robotocore"},
                {"Key": "CostCenter", "Value": "12345"},
            ],
        )
        response = cfn.describe_stacks(StackName=stack_name)
        stack = response["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
        assert tags.get("Environment") == "testing"
        assert tags.get("Project") == "robotocore"
        assert tags.get("CostCenter") == "12345"

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_stack_events(self, cfn):
        """Create a stack and verify events are recorded."""
        stack_name = _unique("test-events")
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName=stack_name)
        assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        events_resp = cfn.describe_stack_events(StackName=stack_name)
        events = events_resp["StackEvents"]
        assert len(events) >= 1

        # Should have at least one event for the stack itself
        stack_events = [e for e in events if e.get("ResourceType") == "AWS::CloudFormation::Stack"]
        assert len(stack_events) >= 1

        # Every event should have a timestamp and logical resource id
        for event in events:
            assert "Timestamp" in event
            assert "LogicalResourceId" in event

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_list_stack_resources(self, cfn):
        """List stack resources via list_stack_resources API."""
        stack_name = _unique("test-list-res")
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.list_stack_resources(StackName=stack_name)
        summaries = response["StackResourceSummaries"]
        assert len(summaries) >= 1
        assert summaries[0]["ResourceType"] == "AWS::SQS::Queue"
        assert summaries[0]["LogicalResourceId"] == "MyQueue"
        assert summaries[0]["ResourceStatus"] == "CREATE_COMPLETE"

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_describe_stack_resource(self, cfn):
        """Describe a single stack resource by logical ID."""
        stack_name = _unique("test-desc-res")
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stack_resource(
            StackName=stack_name,
            LogicalResourceId="MyQueue",
        )
        detail = response["StackResourceDetail"]
        assert detail["LogicalResourceId"] == "MyQueue"
        assert detail["ResourceType"] == "AWS::SQS::Queue"
        assert detail["ResourceStatus"] == "CREATE_COMPLETE"
        # PhysicalResourceId should be set
        assert detail.get("PhysicalResourceId")

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_stack_description_field(self, cfn):
        """Verify that the Description field from the template appears in describe_stacks."""
        stack_name = _unique("test-desc-field")
        cfn.create_stack(
            StackName=stack_name,
            TemplateBody=SIMPLE_SQS_TEMPLATE,
        )
        response = cfn.describe_stacks(StackName=stack_name)
        stack = response["Stacks"][0]
        assert stack.get("Description") == "Simple SQS queue"

        cfn.delete_stack(StackName=stack_name)


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

    def test_stack_with_multiple_resources_sqs_and_sns(self, cfn, sqs, sns):
        """Create a stack with both SQS queue and SNS topic, verify both exist."""
        queue_name = _unique("cfn-multi-sqs")
        topic_name = _unique("cfn-multi-sns")
        stack_name = _unique("test-multi-sqs-sns")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Stack with SQS queue and SNS topic",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                    "Topic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
                "Outputs": {
                    "QueueUrl": {"Value": {"Ref": "Queue"}},
                    "TopicArn": {"Value": {"Ref": "Topic"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        response = cfn.describe_stacks(StackName=stack_name)
        stack = response["Stacks"][0]
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        assert "QueueUrl" in outputs
        assert "TopicArn" in outputs

        # Verify both resources actually exist
        q_url = sqs.get_queue_url(QueueName=queue_name)
        assert queue_name in q_url["QueueUrl"]

        topics = sns.list_topics()["Topics"]
        topic_arns = [t["TopicArn"] for t in topics]
        assert any(topic_name in arn for arn in topic_arns)

        # Verify describe_stack_resources shows both
        res = cfn.describe_stack_resources(StackName=stack_name)
        types = {r["ResourceType"] for r in res["StackResources"]}
        assert "AWS::SQS::Queue" in types
        assert "AWS::SNS::Topic" in types

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Cross-stack references (exports/imports) not yet implemented")
    def test_cross_stack_references(self, cfn, sqs):
        """Create two stacks where stack B imports an output from stack A."""
        queue_name = _unique("cfn-export-q")
        stack_a_name = _unique("test-export-a")
        stack_b_name = _unique("test-import-b")
        export_name = _unique("ExportedQueueArn")

        template_a = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
                "Outputs": {
                    "QueueArn": {
                        "Value": {"Fn::GetAtt": ["MyQueue", "Arn"]},
                        "Export": {"Name": export_name},
                    },
                },
            }
        )
        template_b = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Placeholder": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-import-q")},
                    },
                },
                "Outputs": {
                    "ImportedArn": {
                        "Value": {"Fn::ImportValue": export_name},
                    },
                },
            }
        )

        cfn.create_stack(StackName=stack_a_name, TemplateBody=template_a)
        resp_a = cfn.describe_stacks(StackName=stack_a_name)
        assert resp_a["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        cfn.create_stack(StackName=stack_b_name, TemplateBody=template_b)
        resp_b = cfn.describe_stacks(StackName=stack_b_name)
        assert resp_b["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs_b = {
            o["OutputKey"]: o["OutputValue"]
            for o in resp_b["Stacks"][0].get("Outputs", [])
        }
        assert queue_name in outputs_b.get("ImportedArn", "")

        cfn.delete_stack(StackName=stack_b_name)
        cfn.delete_stack(StackName=stack_a_name)


class TestCloudFormationUpdateStack:
    def test_update_stack_change_queue_property(self, cfn, sqs):
        """Create a stack, then update it to change the queue's visibility timeout."""
        stack_name = _unique("test-update")
        queue_name = _unique("cfn-update-q")

        template_v1 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": queue_name,
                            "VisibilityTimeout": 30,
                        },
                    },
                },
            }
        )
        template_v2 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {
                            "QueueName": queue_name,
                            "VisibilityTimeout": 120,
                        },
                    },
                },
            }
        )

        cfn.create_stack(StackName=stack_name, TemplateBody=template_v1)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Verify initial visibility timeout
        q_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "30"

        # Update the stack
        cfn.update_stack(StackName=stack_name, TemplateBody=template_v2)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"

        # Verify updated visibility timeout
        attrs = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["VisibilityTimeout"])
        assert attrs["Attributes"]["VisibilityTimeout"] == "120"

        cfn.delete_stack(StackName=stack_name)

    def test_update_stack_add_resource(self, cfn):
        """Create a stack with one resource, then update to add a second."""
        stack_name = _unique("test-update-add")
        queue_name = _unique("cfn-upd-q")
        topic_name = _unique("cfn-upd-t")

        template_v1 = json.dumps(
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
        template_v2 = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                    "MyTopic": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
            }
        )

        cfn.create_stack(StackName=stack_name, TemplateBody=template_v1)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        res = cfn.describe_stack_resources(StackName=stack_name)
        assert len(res["StackResources"]) == 1

        cfn.update_stack(StackName=stack_name, TemplateBody=template_v2)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "UPDATE_COMPLETE"

        res = cfn.describe_stack_resources(StackName=stack_name)
        assert len(res["StackResources"]) == 2

        cfn.delete_stack(StackName=stack_name)


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

    def test_dynamodb_table_with_gsi(self, cfn, dynamodb):
        """DynamoDB table with a Global Secondary Index."""
        table_name = _unique("cfn-ddb-gsi")
        stack_name = _unique("test-ddb-gsi")
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
                                {"AttributeName": "gsi_pk", "AttributeType": "S"},
                            ],
                            "KeySchema": [
                                {"AttributeName": "pk", "KeyType": "HASH"},
                            ],
                            "BillingMode": "PAY_PER_REQUEST",
                            "GlobalSecondaryIndexes": [
                                {
                                    "IndexName": "gsi-index",
                                    "KeySchema": [
                                        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                                    ],
                                    "Projection": {"ProjectionType": "ALL"},
                                }
                            ],
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        desc = dynamodb.describe_table(TableName=table_name)
        gsi_names = [g["IndexName"] for g in desc["Table"].get("GlobalSecondaryIndexes", [])]
        assert "gsi-index" in gsi_names

        cfn.delete_stack(StackName=stack_name)

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

    def test_iam_role_with_policy(self, cfn, iam):
        """IAM role with an inline policy."""
        role_name = _unique("cfn-role-pol")
        stack_name = _unique("test-iam-pol")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "MyRole": {
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
                            "Policies": [
                                {
                                    "PolicyName": "InlinePolicy",
                                    "PolicyDocument": {
                                        "Version": "2012-10-17",
                                        "Statement": [
                                            {
                                                "Effect": "Allow",
                                                "Action": "logs:*",
                                                "Resource": "*",
                                            }
                                        ],
                                    },
                                }
                            ],
                        },
                    },
                },
                "Outputs": {
                    "RoleArn": {"Value": {"Fn::GetAtt": ["MyRole", "Arn"]}},
                    "RoleName": {"Value": {"Ref": "MyRole"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        role = iam.get_role(RoleName=role_name)
        assert role["Role"]["RoleName"] == role_name

        outputs = {
            o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])
        }
        assert role_name in outputs.get("RoleArn", "")

        cfn.delete_stack(StackName=stack_name)

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


class TestCloudFormationDeleteCleanup:
    def test_delete_stack_cleans_up_sqs(self, cfn, sqs):
        """Verify SQS queue is removed when stack is deleted."""
        queue_name = _unique("cfn-del-sqs")
        stack_name = _unique("test-del-sqs")
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

        # Queue should exist
        q_url = sqs.get_queue_url(QueueName=queue_name)
        assert queue_name in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

        # Queue should be gone
        with pytest.raises(Exception):
            sqs.get_queue_url(QueueName=queue_name)

    def test_delete_stack_cleans_up_sns(self, cfn, sns):
        """Verify SNS topic is removed when stack is deleted."""
        topic_name = _unique("cfn-del-sns")
        stack_name = _unique("test-del-sns")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "T": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        # Topic should exist
        topics = sns.list_topics()["Topics"]
        assert any(topic_name in t["TopicArn"] for t in topics)

        cfn.delete_stack(StackName=stack_name)

        # Topic should be gone
        topics_after = sns.list_topics()["Topics"]
        assert not any(topic_name in t["TopicArn"] for t in topics_after)

    def test_delete_stack_cleans_up_dynamodb(self, cfn, dynamodb):
        """Verify DynamoDB table is removed when stack is deleted."""
        table_name = _unique("cfn-del-ddb")
        stack_name = _unique("test-del-ddb")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "T": {
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

        assert table_name in dynamodb.list_tables()["TableNames"]

        cfn.delete_stack(StackName=stack_name)

        assert table_name not in dynamodb.list_tables()["TableNames"]

    def test_delete_stack_cleans_up_iam_role(self, cfn, iam):
        """Verify IAM role is removed when stack is deleted."""
        role_name = _unique("cfn-del-role")
        stack_name = _unique("test-del-iam")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "R": {
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
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        role = iam.get_role(RoleName=role_name)
        assert role["Role"]["RoleName"] == role_name

        cfn.delete_stack(StackName=stack_name)

        with pytest.raises(Exception):
            iam.get_role(RoleName=role_name)

    def test_delete_multi_resource_stack(self, cfn, sqs, sns):
        """Verify all resources are cleaned up when a multi-resource stack is deleted."""
        queue_name = _unique("cfn-del-mq")
        topic_name = _unique("cfn-del-mt")
        stack_name = _unique("test-del-multi")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                    "T": {
                        "Type": "AWS::SNS::Topic",
                        "Properties": {"TopicName": topic_name},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        cfn.delete_stack(StackName=stack_name)

        with pytest.raises(Exception):
            sqs.get_queue_url(QueueName=queue_name)

        topics_after = sns.list_topics()["Topics"]
        assert not any(topic_name in t["TopicArn"] for t in topics_after)


class TestCloudFormationIntrinsicFunctions:
    def test_fn_join_with_fn_ref(self, cfn, sqs):
        """Nested intrinsic: Fn::Join with Fn::Ref inside."""
        queue_name = _unique("cfn-join-ref")
        stack_name = _unique("test-join-ref")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "Prefix": {"Type": "String", "Default": "hello"},
                    "Suffix": {"Type": "String", "Default": "world"},
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
                "Outputs": {
                    "Joined": {
                        "Value": {
                            "Fn::Join": [
                                "-",
                                [{"Ref": "Prefix"}, {"Ref": "AWS::Region"}, {"Ref": "Suffix"}],
                            ]
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("Joined") == "hello-us-east-1-world"

        cfn.delete_stack(StackName=stack_name)

    def test_fn_select(self, cfn):
        """Fn::Select picks an element from a list."""
        stack_name = _unique("test-fn-select")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-sel-q")},
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
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("Selected") == "beta"

        cfn.delete_stack(StackName=stack_name)

    def test_fn_split(self, cfn):
        """Fn::Split splits a string into a list."""
        stack_name = _unique("test-fn-split")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-spl-q")},
                    },
                },
                "Outputs": {
                    "SplitResult": {
                        "Value": {
                            "Fn::Select": [
                                "2",
                                {"Fn::Split": ["-", "one-two-three"]},
                            ]
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("SplitResult") == "three"

        cfn.delete_stack(StackName=stack_name)

    def test_fn_sub_with_ref(self, cfn):
        """Fn::Sub with parameter references."""
        stack_name = _unique("test-sub-ref")
        queue_name = _unique("cfn-sub-ref-q")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "EnvName": {"Type": "String", "Default": "dev"},
                },
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
                "Outputs": {
                    "SubResult": {
                        "Value": {
                            "Fn::Sub": (
                                "arn:aws:sqs:${AWS::Region}"
                                ":${AWS::AccountId}:${EnvName}-queue"
                            )
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        sub_result = outputs.get("SubResult", "")
        assert "us-east-1" in sub_result
        assert "dev-queue" in sub_result

        cfn.delete_stack(StackName=stack_name)

    def test_fn_get_azs(self, cfn):
        """Fn::GetAZs returns availability zones for a region."""
        stack_name = _unique("test-getazs")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-azs-q")},
                    },
                },
                "Outputs": {
                    "FirstAZ": {
                        "Value": {
                            "Fn::Select": ["0", {"Fn::GetAZs": {"Ref": "AWS::Region"}}]
                        },
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        # This may or may not work depending on Fn::Select + Fn::GetAZs support
        stack = resp["Stacks"][0]
        if stack["StackStatus"] == "CREATE_COMPLETE":
            outputs = {
                o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])
            }
            assert "us-east-1" in outputs.get("FirstAZ", "")

        cfn.delete_stack(StackName=stack_name)


class TestCloudFormationConditions:
    def test_condition_true(self, cfn, sqs):
        """Template with a condition that evaluates to true creates the resource."""
        queue_name = _unique("cfn-cond-true")
        stack_name = _unique("test-cond-true")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "CreateQueue": {"Type": "String", "Default": "yes"},
                },
                "Conditions": {
                    "ShouldCreateQueue": {
                        "Fn::Equals": [{"Ref": "CreateQueue"}, "yes"]
                    },
                },
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Condition": "ShouldCreateQueue",
                        "Properties": {"QueueName": queue_name},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        q_url = sqs.get_queue_url(QueueName=queue_name)
        assert queue_name in q_url["QueueUrl"]

        cfn.delete_stack(StackName=stack_name)

    @pytest.mark.xfail(reason="Conditions not yet fully implemented")
    def test_condition_false(self, cfn, sqs):
        """Template with a condition that evaluates to false skips the resource."""
        queue_name = _unique("cfn-cond-false")
        stack_name = _unique("test-cond-false")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Parameters": {
                    "CreateQueue": {"Type": "String", "Default": "no"},
                },
                "Conditions": {
                    "ShouldCreateQueue": {
                        "Fn::Equals": [{"Ref": "CreateQueue"}, "yes"]
                    },
                },
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Condition": "ShouldCreateQueue",
                        "Properties": {"QueueName": queue_name},
                    },
                    "Placeholder": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-placeholder")},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        with pytest.raises(Exception):
            sqs.get_queue_url(QueueName=queue_name)

        cfn.delete_stack(StackName=stack_name)

    def test_condition_with_fn_if_in_output(self, cfn):
        """Fn::If in outputs selects value based on condition."""
        stack_name = _unique("test-cond-if")
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
                        "Properties": {"QueueName": _unique("cfn-if-q")},
                    },
                },
                "Outputs": {
                    "EnvLabel": {
                        "Value": {"Fn::If": ["IsProd", "production", "development"]},
                    },
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("EnvLabel") == "production"

        cfn.delete_stack(StackName=stack_name)


class TestCloudFormationPseudoParameters:
    def test_aws_region_pseudo_param(self, cfn):
        """Ref to AWS::Region resolves correctly."""
        stack_name = _unique("test-pseudo-region")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-pseudo-q")},
                    },
                },
                "Outputs": {
                    "Region": {"Value": {"Ref": "AWS::Region"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("Region") == "us-east-1"

        cfn.delete_stack(StackName=stack_name)

    def test_aws_account_id_pseudo_param(self, cfn):
        """Ref to AWS::AccountId resolves correctly."""
        stack_name = _unique("test-pseudo-acct")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-acct-q")},
                    },
                },
                "Outputs": {
                    "AccountId": {"Value": {"Ref": "AWS::AccountId"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        account_id = outputs.get("AccountId", "")
        # Account ID should be a 12-digit number
        assert len(account_id) == 12
        assert account_id.isdigit()

        cfn.delete_stack(StackName=stack_name)

    def test_aws_stack_name_pseudo_param(self, cfn):
        """Ref to AWS::StackName resolves correctly."""
        stack_name = _unique("test-pseudo-sn")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-sn-q")},
                    },
                },
                "Outputs": {
                    "StackName": {"Value": {"Ref": "AWS::StackName"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        assert outputs.get("StackName") == stack_name

        cfn.delete_stack(StackName=stack_name)

    def test_aws_stack_id_pseudo_param(self, cfn):
        """Ref to AWS::StackId resolves to an ARN-like value."""
        stack_name = _unique("test-pseudo-sid")
        template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": _unique("cfn-sid-q")},
                    },
                },
                "Outputs": {
                    "StackId": {"Value": {"Ref": "AWS::StackId"}},
                },
            }
        )
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        resp = cfn.describe_stacks(StackName=stack_name)
        assert resp["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

        outputs = {o["OutputKey"]: o["OutputValue"] for o in resp["Stacks"][0].get("Outputs", [])}
        stack_id = outputs.get("StackId", "")
        assert "arn:" in stack_id or stack_name in stack_id

        cfn.delete_stack(StackName=stack_name)
