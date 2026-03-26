"""IAM test data factories with automatic cleanup.

Provides context managers for creating IAM users, roles, and policies that are
automatically cleaned up after the test.

Usage:
    from tests.factories.iam import user, role, policy

    def test_user_operations(iam):
        with user(iam) as user_name:
            iam.get_user(UserName=user_name)
            iam.list_users()

    def test_role_operations(iam):
        with role(iam) as role_name:
            iam.get_role(RoleName=role_name)
"""

import json
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from botocore.exceptions import ClientError

from . import unique_name

__all__ = ["user", "role", "policy", "user_with_policy"]

# Default trust policy for roles (allows EC2 to assume the role)
DEFAULT_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

# Default policy document (allows nothing)
DEFAULT_POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*"}],
}


@contextmanager
def user(client: Any, name: str | None = None, path: str = "/") -> Generator[str, None, None]:
    """Create an IAM user with automatic cleanup.

    Args:
        client: boto3 IAM client
        name: Optional user name (auto-generated if not provided)
        path: Optional path for the user (default "/")

    Yields:
        User name

    Example:
        with user(iam) as user_name:
            iam.get_user(UserName=user_name)
    """
    user_name = name or unique_name("test-user")

    client.create_user(UserName=user_name, Path=path)

    try:
        yield user_name
    finally:
        try:
            # Detach any attached policies
            attached = client.list_attached_user_policies(UserName=user_name)
            for policy in attached.get("AttachedPolicies", []):
                client.detach_user_policy(UserName=user_name, PolicyArn=policy["PolicyArn"])

            # Delete inline policies
            policies = client.list_user_policies(UserName=user_name)
            for policy_name in policies.get("PolicyNames", []):
                client.delete_user_policy(UserName=user_name, PolicyName=policy_name)

            # Remove from groups
            groups = client.list_groups_for_user(UserName=user_name)
            for group in groups.get("Groups", []):
                client.remove_user_from_group(GroupName=group["GroupName"], UserName=user_name)

            # Delete access keys
            keys = client.list_access_keys(UserName=user_name)
            for key in keys.get("AccessKeyMetadata", []):
                client.delete_access_key(UserName=user_name, AccessKeyId=key["AccessKeyId"])

            client.delete_user(UserName=user_name)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def role(
    client: Any,
    name: str | None = None,
    trust_policy: dict | None = None,
    path: str = "/",
) -> Generator[str, None, None]:
    """Create an IAM role with automatic cleanup.

    Args:
        client: boto3 IAM client
        name: Optional role name (auto-generated if not provided)
        trust_policy: Optional trust policy document (default allows EC2)
        path: Optional path for the role (default "/")

    Yields:
        Role name

    Example:
        with role(iam) as role_name:
            iam.get_role(RoleName=role_name)

        with role(iam, trust_policy=custom_policy) as role_name:
            ...
    """
    role_name = name or unique_name("test-role")
    policy = trust_policy or DEFAULT_TRUST_POLICY

    client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(policy),
        Path=path,
    )

    try:
        yield role_name
    finally:
        try:
            # Detach any attached policies
            attached = client.list_attached_role_policies(RoleName=role_name)
            for p in attached.get("AttachedPolicies", []):
                client.detach_role_policy(RoleName=role_name, PolicyArn=p["PolicyArn"])

            # Delete inline policies
            policies = client.list_role_policies(RoleName=role_name)
            for policy_name in policies.get("PolicyNames", []):
                client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

            # Remove instance profiles
            profiles = client.list_instance_profiles_for_role(RoleName=role_name)
            for profile in profiles.get("InstanceProfiles", []):
                client.remove_role_from_instance_profile(
                    InstanceProfileName=profile["InstanceProfileName"],
                    RoleName=role_name,
                )

            client.delete_role(RoleName=role_name)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def policy(
    client: Any,
    name: str | None = None,
    document: dict | None = None,
    path: str = "/",
) -> Generator[str, None, None]:
    """Create an IAM policy with automatic cleanup.

    Args:
        client: boto3 IAM client
        name: Optional policy name (auto-generated if not provided)
        document: Policy document (default is deny-all)
        path: Optional path for the policy (default "/")

    Yields:
        Policy ARN

    Example:
        with policy(iam) as policy_arn:
            iam.get_policy(PolicyArn=policy_arn)
    """
    policy_name = name or unique_name("test-policy")
    doc = document or DEFAULT_POLICY_DOCUMENT

    response = client.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(doc),
        Path=path,
    )
    policy_arn = response["Policy"]["Arn"]

    try:
        yield policy_arn
    finally:
        try:
            # Delete non-default policy versions
            versions = client.list_policy_versions(PolicyArn=policy_arn)
            for version in versions.get("Versions", []):
                if not version["IsDefaultVersion"]:
                    client.delete_policy_version(
                        PolicyArn=policy_arn, VersionId=version["VersionId"]
                    )

            client.delete_policy(PolicyArn=policy_arn)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def user_with_policy(
    client: Any,
    user_name: str | None = None,
    policy_document: dict | None = None,
) -> Generator[tuple[str, str], None, None]:
    """Create an IAM user with an attached policy.

    Args:
        client: boto3 IAM client
        user_name: Optional user name (auto-generated if not provided)
        policy_document: Policy document (default is deny-all)

    Yields:
        Tuple of (user_name, policy_arn)

    Example:
        with user_with_policy(iam) as (user_name, policy_arn):
            attached = iam.list_attached_user_policies(UserName=user_name)
            assert len(attached["AttachedPolicies"]) == 1
    """
    with user(client, name=user_name) as u_name:
        with policy(client, document=policy_document) as policy_arn:
            client.attach_user_policy(UserName=u_name, PolicyArn=policy_arn)
            yield u_name, policy_arn
