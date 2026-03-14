"""Lifecycle tests for Batch 3 operations.

Step Functions: StateMachineAlias CRUD
DynamoDB: DescribeContributorInsights, DescribeGlobalTableSettings,
  DescribeKinesisStreamingDestination
Lambda: Standalone CodeSigningConfig CRUD
ECS: DescribeServiceDeployments, ListServiceDeployments, DescribeServiceRevisions
"""

import json

import boto3
import pytest

ENDPOINT = "http://localhost:4566"
CREDS = {
    "endpoint_url": ENDPOINT,
    "region_name": "us-east-1",
    "aws_access_key_id": "testing",
    "aws_secret_access_key": "testing",
}


# --- Step Functions: StateMachineAlias ---


@pytest.fixture
def sfn_client():
    return boto3.client("stepfunctions", **CREDS)


@pytest.fixture
def state_machine_arn(sfn_client):
    iam = boto3.client("iam", **CREDS)
    try:
        iam.create_role(
            RoleName="sfn-test-role",
            AssumeRolePolicyDocument="{}",
            Path="/",
        )
    except Exception:
        pass
    sm = sfn_client.create_state_machine(
        name="test-sm-alias",
        definition=json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        ),
        roleArn="arn:aws:iam::123456789012:role/sfn-test-role",
    )
    sm_arn = sm["stateMachineArn"]
    sfn_client.publish_state_machine_version(stateMachineArn=sm_arn)
    yield sm_arn
    try:
        sfn_client.delete_state_machine(stateMachineArn=sm["stateMachineArn"])
    except Exception:
        pass


def test_state_machine_alias_lifecycle(sfn_client, state_machine_arn):
    """Create, describe, list, update, delete a state machine alias."""
    version_arn = state_machine_arn + ":1"
    create_resp = sfn_client.create_state_machine_alias(
        name="prod",
        routingConfiguration=[{"stateMachineVersionArn": version_arn, "weight": 100}],
        description="Production alias",
    )
    alias_arn = create_resp["stateMachineAliasArn"]
    assert alias_arn.endswith(":prod")

    desc = sfn_client.describe_state_machine_alias(stateMachineAliasArn=alias_arn)
    assert desc["name"] == "prod"
    assert desc["description"] == "Production alias"
    assert len(desc["routingConfiguration"]) == 1

    aliases = sfn_client.list_state_machine_aliases(stateMachineArn=state_machine_arn)
    assert len(aliases["stateMachineAliases"]) >= 1
    alias_arns = [a["stateMachineAliasArn"] for a in aliases["stateMachineAliases"]]
    assert alias_arn in alias_arns

    sfn_client.update_state_machine_alias(
        stateMachineAliasArn=alias_arn,
        description="Updated description",
    )
    desc2 = sfn_client.describe_state_machine_alias(stateMachineAliasArn=alias_arn)
    assert desc2["description"] == "Updated description"

    sfn_client.delete_state_machine_alias(stateMachineAliasArn=alias_arn)
    with pytest.raises(sfn_client.exceptions.ResourceNotFound):
        sfn_client.describe_state_machine_alias(stateMachineAliasArn=alias_arn)


def test_state_machine_alias_not_found(sfn_client):
    """Describe a nonexistent alias raises ResourceNotFound."""
    with pytest.raises(sfn_client.exceptions.ResourceNotFound):
        sfn_client.describe_state_machine_alias(
            stateMachineAliasArn="arn:aws:states:us-east-1:123456789012:stateMachine:fake:fake"
        )


# --- DynamoDB: Standalone Describe Operations ---


@pytest.fixture
def ddb_client():
    return boto3.client("dynamodb", **CREDS)


@pytest.fixture
def ddb_table(ddb_client):
    table_name = "test-batch3-table"
    ddb_client.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = ddb_client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)
    yield table_name
    try:
        ddb_client.delete_table(TableName=table_name)
    except Exception:
        pass


def test_describe_contributor_insights(ddb_client, ddb_table):
    """DescribeContributorInsights returns status for a table."""
    resp = ddb_client.describe_contributor_insights(TableName=ddb_table)
    assert resp["TableName"] == ddb_table
    assert resp["ContributorInsightsStatus"] in ("ENABLED", "DISABLED")


def test_describe_global_table_settings(ddb_client, ddb_table):
    """DescribeGlobalTableSettings returns settings for a table."""
    resp = ddb_client.describe_global_table_settings(GlobalTableName=ddb_table)
    assert resp["GlobalTableName"] == ddb_table
    assert isinstance(resp["ReplicaSettings"], list)


def test_describe_kinesis_streaming_destination(ddb_client, ddb_table):
    """DescribeKinesisStreamingDestination returns dest info."""
    resp = ddb_client.describe_kinesis_streaming_destination(TableName=ddb_table)
    assert resp["TableName"] == ddb_table
    assert isinstance(resp["KinesisDataStreamDestinations"], list)


# --- Lambda: Standalone CodeSigningConfig ---


@pytest.fixture
def lambda_client():
    return boto3.client("lambda", **CREDS)


def test_code_signing_config_lifecycle(lambda_client):
    """Create, get, list, delete a standalone code signing config."""
    create_resp = lambda_client.create_code_signing_config(
        AllowedPublishers={
            "SigningProfileVersionArns": [
                "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile"
            ]
        },
        Description="Test CSC",
    )
    csc = create_resp["CodeSigningConfig"]
    csc_arn = csc["CodeSigningConfigArn"]
    assert csc["Description"] == "Test CSC"
    assert "csc-" in csc["CodeSigningConfigId"]

    get_resp = lambda_client.get_code_signing_config(CodeSigningConfigArn=csc_arn)
    assert get_resp["CodeSigningConfig"]["CodeSigningConfigArn"] == csc_arn

    list_resp = lambda_client.list_code_signing_configs()
    arns = [c["CodeSigningConfigArn"] for c in list_resp["CodeSigningConfigs"]]
    assert csc_arn in arns

    lambda_client.delete_code_signing_config(CodeSigningConfigArn=csc_arn)

    with pytest.raises(lambda_client.exceptions.ResourceNotFoundException):
        lambda_client.get_code_signing_config(CodeSigningConfigArn=csc_arn)


def test_code_signing_config_not_found(lambda_client):
    """Get a nonexistent code signing config raises not found."""
    with pytest.raises(lambda_client.exceptions.ResourceNotFoundException):
        lambda_client.get_code_signing_config(
            CodeSigningConfigArn="arn:aws:lambda:us-east-1:123456789012:code-signing-config:csc-0000000000000fake"
        )


# --- ECS: ServiceDeployments & ServiceRevisions ---


@pytest.fixture
def ecs_client():
    return boto3.client("ecs", **CREDS)


@pytest.fixture
def ecs_service(ecs_client):
    cluster_name = "test-batch3-cluster"
    ecs_client.create_cluster(clusterName=cluster_name)
    ecs_client.register_task_definition(
        family="test-batch3-td",
        containerDefinitions=[
            {
                "name": "app",
                "image": "nginx",
                "memory": 256,
            }
        ],
    )
    svc = ecs_client.create_service(
        cluster=cluster_name,
        serviceName="test-batch3-svc",
        taskDefinition="test-batch3-td",
        desiredCount=1,
    )
    yield {
        "cluster": cluster_name,
        "service_name": "test-batch3-svc",
        "service_arn": svc["service"]["serviceArn"],
    }
    try:
        ecs_client.delete_service(
            cluster=cluster_name,
            service="test-batch3-svc",
            force=True,
        )
        ecs_client.delete_cluster(cluster=cluster_name)
    except Exception:
        pass


def test_list_service_deployments(ecs_client, ecs_service):
    """ListServiceDeployments returns deployments for a service."""
    resp = ecs_client.list_service_deployments(
        service=ecs_service["service_name"],
        cluster=ecs_service["cluster"],
    )
    assert "serviceDeployments" in resp
    deps = resp["serviceDeployments"]
    assert len(deps) >= 1
    dep = deps[0]
    assert "serviceDeploymentArn" in dep
    assert "status" in dep


def test_describe_service_deployments(ecs_client, ecs_service):
    """DescribeServiceDeployments returns deployment details."""
    list_resp = ecs_client.list_service_deployments(
        service=ecs_service["service_name"],
        cluster=ecs_service["cluster"],
    )
    dep_arns = [d["serviceDeploymentArn"] for d in list_resp["serviceDeployments"]]
    assert len(dep_arns) >= 1

    desc_resp = ecs_client.describe_service_deployments(serviceDeploymentArns=dep_arns[:1])
    assert "serviceDeployments" in desc_resp
    assert len(desc_resp["serviceDeployments"]) >= 1


def test_describe_service_revisions(ecs_client, ecs_service):
    """DescribeServiceRevisions returns revision info."""
    cluster = ecs_service["cluster"]
    svc = ecs_service["service_name"]
    rev_arn = f"arn:aws:ecs:us-east-1:123456789012:service-revision/{cluster}/{svc}:1"
    resp = ecs_client.describe_service_revisions(serviceRevisionArns=[rev_arn])
    assert "serviceRevisions" in resp or "failures" in resp
