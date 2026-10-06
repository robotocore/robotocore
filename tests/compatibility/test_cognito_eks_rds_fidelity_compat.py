"""Read-back and catalog fixes: Cognito resource servers, EKS pod identity, RDS engines."""

import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestCognitoResourceServer:
    def test_delete_resource_server(self):
        idp = _client("cognito-idp")
        pool = idp.create_user_pool(PoolName=f"tf-compat-{uuid.uuid4().hex[:8]}")["UserPool"]["Id"]
        ident = "https://api.example.internal"
        idp.create_resource_server(UserPoolId=pool, Identifier=ident, Name="api")
        idp.delete_resource_server(UserPoolId=pool, Identifier=ident)
        assert idp.list_resource_servers(UserPoolId=pool, MaxResults=10)["ResourceServers"] == []
        with pytest.raises(ClientError) as exc:
            idp.delete_resource_server(UserPoolId=pool, Identifier=ident)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestRdsEngineVersions:
    def test_rds_engine_version_prefix_and_mode_filter(self):
        got = _client("rds").describe_db_engine_versions(
            Engine="aurora-postgresql",
            EngineVersion="16",
            Filters=[{"Name": "engine-mode", "Values": ["provisioned"]}],
        )["DBEngineVersions"]
        assert got and all(v["EngineVersion"].startswith("16.") for v in got)


class TestEksPodIdentityReadBack:
    def test_association_returns_session_tags_and_stable_external_id(self):
        eks = _client("eks")
        cluster = f"tf-compat-{uuid.uuid4().hex[:8]}"
        eks.create_cluster(
            name=cluster,
            roleArn="arn:aws:iam::123456789012:role/eks-cluster",
            resourcesVpcConfig={"subnetIds": []},
        )
        created = eks.create_pod_identity_association(
            clusterName=cluster,
            namespace="default",
            serviceAccount="app",
            roleArn="arn:aws:iam::123456789012:role/app",
            disableSessionTags=True,
        )["association"]
        assert created["disableSessionTags"] is True
        assert created["externalId"]
        described = eks.describe_pod_identity_association(
            clusterName=cluster, associationId=created["associationId"]
        )["association"]
        assert described["disableSessionTags"] is True
        assert described["externalId"] == created["externalId"]
