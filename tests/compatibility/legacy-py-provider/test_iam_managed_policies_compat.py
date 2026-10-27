"""IAM behaviours Terraform depends on: AWS-managed policies and STS token preferences."""

import os
import uuid

import boto3

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


class TestAwsManagedPolicies:
    def test_attach_aws_managed_policy_to_role(self):
        import json

        iam = _client("iam", "300000000010")
        name = f"tf-compat-{uuid.uuid4().hex[:8]}"
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": []}),
        )
        arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
        iam.attach_role_policy(RoleName=name, PolicyArn=arn)
        attached = iam.list_attached_role_policies(RoleName=name)["AttachedPolicies"]
        assert [p["PolicyArn"] for p in attached] == [arn]

    def test_get_aws_managed_policy(self):
        iam = _client("iam", "300000000011")
        arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
        assert iam.get_policy(PolicyArn=arn)["Policy"]["Arn"] == arn

    def test_local_scope_list_is_not_polluted(self):
        iam = _client("iam", "300000000012")
        iam.get_policy(PolicyArn="arn:aws:iam::aws:policy/ReadOnlyAccess")
        assert iam.list_policies(Scope="Local")["Policies"] == []


class TestStsPreferences:
    def test_sts_preferences_reflected_in_account_summary(self):
        iam = _client("iam", "300000000021")
        iam.set_security_token_service_preferences(GlobalEndpointTokenVersion="v2Token")
        summary = iam.get_account_summary()["SummaryMap"]
        assert summary["GlobalEndpointTokenVersion"] == 2
