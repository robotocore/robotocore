"""Fixtures for CI/CD pipeline tests."""

import os

import pytest

from .app import CICDPipeline

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def pipeline(s3, dynamodb, ssm, sns, sqs, logs, stepfunctions, iam, boto_session, unique_name):
    """Create a fully configured CICDPipeline with all resources."""
    bucket_name = f"cicd-artifacts-{unique_name}"
    table_name = f"cicd-builds-{unique_name}"
    config_prefix = f"/cicd/{unique_name}"
    log_group_prefix = f"/cicd/{unique_name}/logs"

    # Create S3 bucket
    s3.create_bucket(Bucket=bucket_name)

    # Create DynamoDB table with GSIs
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "build_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "build_id", "AttributeType": "S"},
            {"AttributeName": "repo_name", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "started_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-repo",
                "KeySchema": [
                    {"AttributeName": "repo_name", "KeyType": "HASH"},
                    {"AttributeName": "started_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "started_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    p = CICDPipeline(
        s3_client=s3,
        dynamodb_client=dynamodb,
        ssm_client=ssm,
        sns_client=sns,
        sqs_client=sqs,
        logs_client=logs,
        stepfunctions_client=stepfunctions,
        iam_client=iam,
        artifact_bucket=bucket_name,
        builds_table=table_name,
        config_prefix=config_prefix,
        log_group_prefix=log_group_prefix,
    )

    yield p

    # Cleanup
    # Delete all objects in bucket
    objs = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    if objs:
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
        )
    s3.delete_bucket(Bucket=bucket_name)

    # Delete DynamoDB table
    dynamodb.delete_table(TableName=table_name)

    # Cleanup SSM params (best-effort)
    try:
        resp = ssm.get_parameters_by_path(Path=config_prefix, Recursive=True)
        for param in resp["Parameters"]:
            ssm.delete_parameter(Name=param["Name"])
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def sample_build(pipeline):
    """Create a pre-built build in SUCCESS state with an artifact."""
    build = pipeline.queue_build(
        repo="org/sample-app",
        branch="main",
        commit_sha="abc123def456",
        build_number=1,
    )
    pipeline.transition_build(build.build_id, pipeline.BUILDING)
    pipeline.transition_build(build.build_id, pipeline.TESTING)
    pipeline.transition_build(build.build_id, pipeline.DEPLOYING)
    pipeline.upload_artifact(
        build_id=build.build_id,
        artifact_name="app.zip",
        content=b"sample artifact content",
        commit_sha="abc123def456",
        branch="main",
        build_number=1,
    )
    pipeline.transition_build(build.build_id, pipeline.SUCCESS)
    return pipeline.get_build(build.build_id)
