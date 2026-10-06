"""Calls signed with STS/IAM-issued access keys act in the account that owns the key."""

import os

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


class TestAssumedRoleAccount:
    def test_assumed_role_credentials_act_in_target_account(self):
        target = "222222222222"
        creds = _client("sts", "111111111111").assume_role(
            RoleArn=f"arn:aws:iam::{target}:role/DeployRole", RoleSessionName="tf"
        )["Credentials"]
        sts = _client(
            "sts",
            aws_access_key_id=creds["AccessKeyId"],
            aws_session_token=creds["SessionToken"],
        )
        assert sts.get_caller_identity()["Account"] == target
