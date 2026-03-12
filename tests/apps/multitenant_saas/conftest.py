"""
Fixtures for the multi-tenant SaaS platform tests.

Provides a fully-wired SaaSPlatform instance with two pre-provisioned tenants
on different plans.
"""

import pytest

from .app import SaaSPlatform
from .models import PLAN_CATALOGUE


@pytest.fixture
def platform(dynamodb, s3, ssm, secretsmanager, sqs, cloudwatch, unique_name):
    """
    A fully initialised SaaSPlatform with shared AWS resources created and
    torn down automatically.
    """
    table_name = f"saas-data-{unique_name}"
    bucket_name = f"saas-files-{unique_name}"
    queue_name = f"saas-onboarding-{unique_name}"
    ssm_prefix = f"/saas/{unique_name}"
    secret_prefix = f"saas-{unique_name}"
    metrics_ns = f"SaaS/{unique_name}"

    # Create DynamoDB table
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "tenant_id", "KeyType": "HASH"},
            {"AttributeName": "entity_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "tenant_id", "AttributeType": "S"},
            {"AttributeName": "entity_key", "AttributeType": "S"},
            {"AttributeName": "entity_type", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-entity-type",
                "KeySchema": [
                    {"AttributeName": "entity_type", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    # Create S3 bucket
    s3.create_bucket(Bucket=bucket_name)

    # Create SQS queue
    queue_resp = sqs.create_queue(QueueName=queue_name)
    queue_url = queue_resp["QueueUrl"]

    p = SaaSPlatform(
        dynamodb=dynamodb,
        s3=s3,
        ssm=ssm,
        secretsmanager=secretsmanager,
        sqs=sqs,
        cloudwatch=cloudwatch,
        table_name=table_name,
        bucket_name=bucket_name,
        queue_url=queue_url,
        ssm_prefix=ssm_prefix,
        secret_prefix=secret_prefix,
        metrics_namespace=metrics_ns,
        plan_catalogue=PLAN_CATALOGUE,
    )

    yield p

    # ---------- Cleanup ----------
    # Delete all DynamoDB items then the table
    scan = dynamodb.scan(TableName=table_name)
    for item in scan.get("Items", []):
        dynamodb.delete_item(
            TableName=table_name,
            Key={"tenant_id": item["tenant_id"], "entity_key": item["entity_key"]},
        )
    dynamodb.delete_table(TableName=table_name)

    # Delete all S3 objects then the bucket
    objs = s3.list_objects_v2(Bucket=bucket_name)
    for obj in objs.get("Contents", []):
        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket_name)

    # Delete SQS queue
    sqs.delete_queue(QueueUrl=queue_url)

    # Delete SSM parameters
    resp = ssm.get_parameters_by_path(Path=ssm_prefix, Recursive=True)
    for param in resp.get("Parameters", []):
        ssm.delete_parameter(Name=param["Name"])

    # Secrets are cleaned up by deprovision or individually in tests


@pytest.fixture
def tenant_a(platform):
    """A pre-provisioned tenant on the 'starter' plan."""
    return platform.provision_tenant(
        tenant_id="tenant-a",
        name="Acme Analytics",
        plan="starter",
        admin_email="admin@acme.example.com",
    )


@pytest.fixture
def tenant_b(platform):
    """A pre-provisioned tenant on the 'enterprise' plan."""
    return platform.provision_tenant(
        tenant_id="tenant-b",
        name="BigCorp Insights",
        plan="enterprise",
        admin_email="admin@bigcorp.example.com",
    )


@pytest.fixture
def plan_definitions():
    """The canonical plan catalogue."""
    return dict(PLAN_CATALOGUE)
