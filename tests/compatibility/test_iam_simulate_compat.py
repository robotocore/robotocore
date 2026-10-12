"""IAM policy simulator compatibility: SimulateCustomPolicy and SimulatePrincipalPolicy."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

OBJECT_ARN = "arn:aws:s3:::example-bucket/reports/2024.csv"


def _policy(*statements: dict) -> str:
    return json.dumps({"Version": "2012-10-17", "Statement": list(statements)})


ALLOW_S3 = _policy({"Effect": "Allow", "Action": "s3:*", "Resource": "*"})
DENY_DELETE = _policy({"Effect": "Deny", "Action": "s3:Delete*", "Resource": "*"})
READ_BUCKET = _policy(
    {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::example-bucket/*"}
)
TRUST_POLICY = _policy(
    {
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }
)


@pytest.fixture
def account():
    return str(uuid.uuid4().int)[:12].zfill(12)


@pytest.fixture
def iam(account):
    return make_client("iam", aws_access_key_id=account)


def _by_action(resp: dict) -> dict:
    return {r["EvalActionName"]: r for r in resp["EvaluationResults"]}


class TestSimulateCustomPolicy:
    def test_allowed_explicit_and_implicit_deny(self, iam):
        resp = iam.simulate_custom_policy(
            PolicyInputList=[ALLOW_S3, DENY_DELETE],
            ActionNames=["s3:GetObject", "s3:DeleteObject", "ec2:RunInstances"],
        )
        results = _by_action(resp)
        assert results["s3:GetObject"]["EvalDecision"] == "allowed"
        assert results["s3:GetObject"]["EvalResourceName"] == "*"
        assert results["s3:GetObject"]["MatchedStatements"][0]["SourcePolicyId"] == (
            "PolicyInputList.1"
        )
        assert results["s3:DeleteObject"]["EvalDecision"] == "explicitDeny"
        assert results["s3:DeleteObject"]["MatchedStatements"][0]["SourcePolicyId"] == (
            "PolicyInputList.2"
        )
        assert results["ec2:RunInstances"]["EvalDecision"] == "implicitDeny"
        assert results["ec2:RunInstances"]["MatchedStatements"] == []
        assert resp["IsTruncated"] is False

    def test_resource_arns(self, iam):
        other = "arn:aws:s3:::other-bucket/key"
        resp = iam.simulate_custom_policy(
            PolicyInputList=[READ_BUCKET],
            ActionNames=["s3:GetObject"],
            ResourceArns=[OBJECT_ARN, other],
        )
        decisions = [(r["EvalResourceName"], r["EvalDecision"]) for r in resp["EvaluationResults"]]
        assert decisions == [(OBJECT_ARN, "allowed"), (other, "implicitDeny")]
        specific = resp["EvaluationResults"][0]["ResourceSpecificResults"][0]
        assert specific["EvalResourceName"] == OBJECT_ARN
        assert specific["EvalResourceDecision"] == "allowed"

    def test_not_action_and_wildcards(self, iam):
        policy = _policy({"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"})
        results = _by_action(
            iam.simulate_custom_policy(
                PolicyInputList=[policy], ActionNames=["sqs:SendMessage", "iam:CreateUser"]
            )
        )
        assert results["sqs:SendMessage"]["EvalDecision"] == "allowed"
        assert results["iam:CreateUser"]["EvalDecision"] == "implicitDeny"

    def test_condition_with_context_entries(self, iam):
        policy = _policy(
            {
                "Effect": "Allow",
                "Action": "ec2:RunInstances",
                "Resource": "*",
                "Condition": {"StringEquals": {"ec2:InstanceType": "t3.micro"}},
            }
        )
        missing = iam.simulate_custom_policy(
            PolicyInputList=[policy], ActionNames=["ec2:RunInstances"]
        )["EvaluationResults"][0]
        assert missing["EvalDecision"] == "implicitDeny"
        assert missing["MissingContextValues"] == ["ec2:InstanceType"]

        for value, expected in (("t3.micro", "allowed"), ("m5.large", "implicitDeny")):
            result = iam.simulate_custom_policy(
                PolicyInputList=[policy],
                ActionNames=["ec2:RunInstances"],
                ContextEntries=[
                    {
                        "ContextKeyName": "ec2:InstanceType",
                        "ContextKeyValues": [value],
                        "ContextKeyType": "string",
                    }
                ],
            )["EvaluationResults"][0]
            assert result["EvalDecision"] == expected, value
            assert result["MissingContextValues"] == []

    def test_permissions_boundary_limits_allow(self, iam):
        boundary = _policy({"Effect": "Allow", "Action": "s3:List*", "Resource": "*"})
        results = _by_action(
            iam.simulate_custom_policy(
                PolicyInputList=[ALLOW_S3],
                PermissionsBoundaryPolicyInputList=[boundary],
                ActionNames=["s3:GetObject", "s3:ListBucket"],
            )
        )
        assert results["s3:GetObject"]["EvalDecision"] == "implicitDeny"
        assert results["s3:GetObject"]["PermissionsBoundaryDecisionDetail"] == {
            "AllowedByPermissionsBoundary": False
        }
        assert results["s3:ListBucket"]["EvalDecision"] == "allowed"

    def test_resource_policy_for_caller(self, iam, account):
        caller = f"arn:aws:iam::{account}:user/alice"
        bucket_policy = _policy(
            {
                "Effect": "Allow",
                "Principal": {"AWS": caller},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::example-bucket/*",
            }
        )
        resp = iam.simulate_custom_policy(
            PolicyInputList=[_policy({"Effect": "Allow", "Action": "ec2:*", "Resource": "*"})],
            ActionNames=["s3:GetObject"],
            ResourceArns=[OBJECT_ARN],
            ResourcePolicy=bucket_policy,
            CallerArn=caller,
        )
        result = resp["EvaluationResults"][0]
        assert result["EvalDecision"] == "allowed"
        assert result["MatchedStatements"][0]["SourcePolicyId"] == "ResourcePolicy"

    def test_pagination(self, iam):
        actions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        first = iam.simulate_custom_policy(
            PolicyInputList=[ALLOW_S3], ActionNames=actions, MaxItems=2
        )
        assert len(first["EvaluationResults"]) == 2
        assert first["IsTruncated"] is True
        second = iam.simulate_custom_policy(
            PolicyInputList=[ALLOW_S3], ActionNames=actions, MaxItems=2, Marker=first["Marker"]
        )
        assert [r["EvalActionName"] for r in second["EvaluationResults"]] == ["s3:ListBucket"]
        assert second["IsTruncated"] is False

    def test_malformed_policy_is_invalid_input(self, iam):
        with pytest.raises(ClientError) as exc:
            iam.simulate_custom_policy(PolicyInputList=["{not json"], ActionNames=["s3:GetObject"])
        assert exc.value.response["Error"]["Code"] == "InvalidInput"


class TestSimulatePrincipalPolicy:
    def test_user_inline_managed_and_group_policies(self, iam):
        iam.create_user(UserName="alice")
        iam.put_user_policy(UserName="alice", PolicyName="S3Full", PolicyDocument=ALLOW_S3)
        managed = iam.create_policy(
            PolicyName="QueueWriter",
            PolicyDocument=_policy(
                {"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "*"}
            ),
        )["Policy"]["Arn"]
        iam.attach_user_policy(UserName="alice", PolicyArn=managed)
        iam.create_group(GroupName="guards")
        iam.put_group_policy(GroupName="guards", PolicyName="NoDeletes", PolicyDocument=DENY_DELETE)
        iam.add_user_to_group(GroupName="guards", UserName="alice")
        user_arn = iam.get_user(UserName="alice")["User"]["Arn"]

        results = _by_action(
            iam.simulate_principal_policy(
                PolicySourceArn=user_arn,
                ActionNames=[
                    "s3:GetObject",
                    "sqs:SendMessage",
                    "s3:DeleteObject",
                    "ec2:RunInstances",
                ],
            )
        )
        assert results["s3:GetObject"]["EvalDecision"] == "allowed"
        assert results["s3:GetObject"]["MatchedStatements"][0]["SourcePolicyId"] == "S3Full"
        assert results["sqs:SendMessage"]["EvalDecision"] == "allowed"
        assert results["sqs:SendMessage"]["MatchedStatements"][0]["SourcePolicyId"] == (
            "QueueWriter"
        )
        assert results["s3:DeleteObject"]["EvalDecision"] == "explicitDeny"
        assert results["s3:DeleteObject"]["MatchedStatements"][0]["SourcePolicyId"] == "NoDeletes"
        assert results["ec2:RunInstances"]["EvalDecision"] == "implicitDeny"

    def test_group(self, iam):
        iam.create_group(GroupName="readers")
        iam.put_group_policy(GroupName="readers", PolicyName="Read", PolicyDocument=READ_BUCKET)
        group_arn = iam.get_group(GroupName="readers")["Group"]["Arn"]
        result = iam.simulate_principal_policy(
            PolicySourceArn=group_arn, ActionNames=["s3:GetObject"], ResourceArns=[OBJECT_ARN]
        )["EvaluationResults"][0]
        assert result["EvalDecision"] == "allowed"
        assert result["EvalResourceName"] == OBJECT_ARN

    def test_role_with_aws_managed_policy_and_boundary(self, iam):
        iam.create_role(
            RoleName="app",
            AssumeRolePolicyDocument=TRUST_POLICY,
            PermissionsBoundary="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
        iam.attach_role_policy(
            RoleName="app", PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
        role_arn = iam.get_role(RoleName="app")["Role"]["Arn"]
        results = _by_action(
            iam.simulate_principal_policy(
                PolicySourceArn=role_arn, ActionNames=["s3:GetObject", "s3:PutObject"]
            )
        )
        assert results["s3:GetObject"]["EvalDecision"] == "allowed"
        assert results["s3:PutObject"]["EvalDecision"] == "implicitDeny"
        assert results["s3:PutObject"]["PermissionsBoundaryDecisionDetail"] == {
            "AllowedByPermissionsBoundary": False
        }

    def test_user_permissions_boundary(self, iam):
        iam.create_user(UserName="bob")
        iam.put_user_policy(UserName="bob", PolicyName="S3Full", PolicyDocument=ALLOW_S3)
        iam.put_user_permissions_boundary(
            UserName="bob", PermissionsBoundary="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        user_arn = iam.get_user(UserName="bob")["User"]["Arn"]
        results = _by_action(
            iam.simulate_principal_policy(
                PolicySourceArn=user_arn, ActionNames=["s3:GetObject", "s3:PutObject"]
            )
        )
        assert results["s3:GetObject"]["EvalDecision"] == "allowed"
        assert results["s3:PutObject"]["EvalDecision"] == "implicitDeny"

    def test_extra_policy_inputs(self, iam):
        iam.create_role(RoleName="worker", AssumeRolePolicyDocument=TRUST_POLICY)
        role_arn = iam.get_role(RoleName="worker")["Role"]["Arn"]
        result = iam.simulate_principal_policy(
            PolicySourceArn=role_arn, PolicyInputList=[ALLOW_S3], ActionNames=["s3:GetObject"]
        )["EvaluationResults"][0]
        assert result["EvalDecision"] == "allowed"
        assert result["MatchedStatements"][0]["SourcePolicyId"] == "PolicyInputList.1"

    def test_missing_principal_is_no_such_entity(self, iam, account):
        with pytest.raises(ClientError) as exc:
            iam.simulate_principal_policy(
                PolicySourceArn=f"arn:aws:iam::{account}:user/nobody",
                ActionNames=["s3:GetObject"],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchEntity"
