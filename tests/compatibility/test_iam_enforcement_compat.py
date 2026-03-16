"""End-to-end tests for IAM policy enforcement (ENFORCE_IAM=1 mode).

Requires the server to be running with ENABLE_CONFIG_UPDATES=1 so tests
can toggle ENFORCE_IAM at runtime via the /_robotocore/config endpoint.

The server must also have the IAM middleware reading from RuntimeConfig
(not raw os.environ) for the toggle to take effect.

IAM and STS requests are always skipped by enforcement to avoid bootstrap
deadlocks — so tests create IAM users/policies with enforcement OFF,
then enable enforcement and test service calls.
"""

import json
import uuid

import boto3
import pytest
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

from tests.compatibility.conftest import ENDPOINT_URL

CONFIG_URL = f"{ENDPOINT_URL}/_robotocore/config"


def _set_enforce_iam(enabled: bool) -> None:
    """Toggle IAM enforcement via the runtime config endpoint."""
    resp = requests.post(CONFIG_URL, json={"ENFORCE_IAM": "1" if enabled else "0"})
    if resp.status_code == 403:
        pytest.skip(
            "Server does not have ENABLE_CONFIG_UPDATES=1. "
            "Restart with: ENABLE_CONFIG_UPDATES=1 uv run python -m robotocore.main"
        )
    resp.raise_for_status()


def _make_iam_client():
    """Create an IAM client (IAM is exempt from enforcement)."""
    return boto3.client(
        "iam",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def _make_s3_client(access_key_id: str, secret_access_key: str):
    """Create an S3 client with specific credentials."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=Config(s3={"addressing_style": "path"}),
    )


def _make_dynamodb_client(access_key_id: str, secret_access_key: str):
    """Create a DynamoDB client with specific credentials."""
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )


def _make_sqs_client(access_key_id: str, secret_access_key: str):
    """Create an SQS client with specific credentials."""
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )


def _create_user_with_policy(iam, user_name: str, policy_document: dict) -> dict:
    """Create an IAM user with an inline policy, return access key info."""
    try:
        iam.create_user(UserName=user_name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
    iam.put_user_policy(
        UserName=user_name,
        PolicyName="TestPolicy",
        PolicyDocument=json.dumps(policy_document),
    )
    resp = iam.create_access_key(UserName=user_name)
    return resp["AccessKey"]


def _cleanup_user(iam, user_name: str) -> None:
    """Best-effort cleanup of an IAM user and its resources."""
    try:
        # Delete access keys
        keys = iam.list_access_keys(UserName=user_name)
        for meta in keys.get("AccessKeyMetadata", []):
            iam.delete_access_key(UserName=user_name, AccessKeyId=meta["AccessKeyId"])
        # Delete inline policies
        policies = iam.list_user_policies(UserName=user_name)
        for name in policies.get("PolicyNames", []):
            iam.delete_user_policy(UserName=user_name, PolicyName=name)
        # Delete user
        iam.delete_user(UserName=user_name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture(autouse=True)
def _enforce_iam_lifecycle():
    """Ensure IAM enforcement is ON during test and OFF after."""
    _set_enforce_iam(False)  # Start clean (create resources without enforcement)
    yield
    _set_enforce_iam(False)  # Always restore to OFF after test


@pytest.fixture
def iam():
    """IAM client (exempt from enforcement)."""
    return _make_iam_client()


class TestNoCredentials:
    """Test behavior when credentials are present but have no policies."""

    def test_credentials_with_no_policies_denied(self, iam):
        """A user with credentials but zero policies is denied."""
        user_name = f"no-policy-user-{uuid.uuid4().hex[:8]}"
        try:
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            _set_enforce_iam(True)

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            with pytest.raises(ClientError) as exc_info:
                s3.list_buckets()
            assert exc_info.value.response["Error"]["Code"] == "AccessDenied"
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)


class TestAdminAccess:
    """Test that a user with full admin policy can access everything."""

    def test_admin_user_can_list_buckets(self, iam):
        """IAM user with Allow * on * can call S3."""
        user_name = f"admin-user-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = s3.list_buckets()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Buckets" in resp
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)

    def test_admin_user_can_access_dynamodb(self, iam):
        """IAM user with Allow * on * can call DynamoDB."""
        user_name = f"admin-ddb-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            ddb = _make_dynamodb_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = ddb.list_tables()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "TableNames" in resp
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)


class TestServiceSpecificPolicy:
    """Test that service-scoped policies allow/deny correctly."""

    def test_s3_only_user_allowed_for_s3(self, iam):
        """User with s3:* policy can access S3."""
        user_name = f"s3only-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = s3.list_buckets()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)

    def test_s3_only_user_denied_for_dynamodb(self, iam):
        """User with s3:* policy is denied for DynamoDB."""
        user_name = f"s3only-deny-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            ddb = _make_dynamodb_client(key["AccessKeyId"], key["SecretAccessKey"])
            with pytest.raises(ClientError) as exc_info:
                ddb.list_tables()
            assert exc_info.value.response["Error"]["Code"] in (
                "AccessDenied",
                "AccessDeniedException",
            )
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)

    def test_s3_only_user_denied_for_sqs(self, iam):
        """User with s3:* policy is denied for SQS."""
        user_name = f"s3only-sqs-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            sqs = _make_sqs_client(key["AccessKeyId"], key["SecretAccessKey"])
            with pytest.raises(ClientError) as exc_info:
                sqs.list_queues()
            assert exc_info.value.response["Error"]["Code"] in (
                "AccessDenied",
                "AccessDeniedException",
            )
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)


class TestWildcardActions:
    """Test wildcard action matching (e.g., s3:*)."""

    def test_wildcard_service_actions(self, iam):
        """s3:* allows all S3 operations including CreateBucket."""
        user_name = f"wildcard-{uuid.uuid4().hex[:8]}"
        bucket_name = f"iam-test-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                },
            )
            _set_enforce_iam(True)

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            # CreateBucket should be allowed under s3:*
            resp = s3.create_bucket(Bucket=bucket_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # ListBuckets should also be allowed
            resp = s3.list_buckets()
            bucket_names = [b["Name"] for b in resp["Buckets"]]
            assert bucket_name in bucket_names
        finally:
            _set_enforce_iam(False)
            # Cleanup bucket
            try:
                admin_s3 = _make_s3_client("testing", "testing")
                admin_s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass  # best-effort cleanup
            _cleanup_user(iam, user_name)


class TestExplicitDeny:
    """Test that explicit Deny overrides Allow."""

    def test_deny_overrides_allow(self, iam):
        """Explicit Deny on s3:* overrides Allow on * for S3."""
        user_name = f"deny-override-{uuid.uuid4().hex[:8]}"
        try:
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            # Attach Allow-all policy
            iam.put_user_policy(
                UserName=user_name,
                PolicyName="AllowAll",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
                    }
                ),
            )
            # Attach Deny-S3 policy
            iam.put_user_policy(
                UserName=user_name,
                PolicyName="DenyS3",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [{"Effect": "Deny", "Action": "s3:*", "Resource": "*"}],
                    }
                ),
            )
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            _set_enforce_iam(True)

            # S3 should be denied (explicit Deny beats Allow)
            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            with pytest.raises(ClientError) as exc_info:
                s3.list_buckets()
            assert exc_info.value.response["Error"]["Code"] == "AccessDenied"

            # DynamoDB should still work (Allow * and no Deny for DynamoDB)
            ddb = _make_dynamodb_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = ddb.list_tables()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)

    def test_deny_specific_action_allows_others_dynamodb(self, iam):
        """Deny dynamodb:DeleteTable still allows dynamodb:ListTables.

        Uses DynamoDB because it sends X-Amz-Target headers, which lets
        the IAM middleware resolve action-level operations. REST-protocol
        services like S3 currently resolve to service:* wildcards.
        """
        user_name = f"deny-action-{uuid.uuid4().hex[:8]}"
        try:
            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                        {
                            "Effect": "Deny",
                            "Action": "dynamodb:DeleteTable",
                            "Resource": "*",
                        },
                    ],
                },
            )
            _set_enforce_iam(True)

            ddb = _make_dynamodb_client(key["AccessKeyId"], key["SecretAccessKey"])
            # ListTables should be allowed
            resp = ddb.list_tables()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # DeleteTable should be denied
            with pytest.raises(ClientError) as exc_info:
                ddb.delete_table(TableName="nonexistent-table-12345")
            assert exc_info.value.response["Error"]["Code"] in (
                "AccessDenied",
                "AccessDeniedException",
            )
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)


class TestResourceSpecificPolicy:
    """Test resource-specific ARN patterns in policies."""

    def test_bucket_specific_policy(self, iam):
        """Policy for a specific bucket ARN allows that bucket, denies others."""
        user_name = f"bucket-specific-{uuid.uuid4().hex[:8]}"
        allowed_bucket = f"iam-allowed-{uuid.uuid4().hex[:8]}"
        denied_bucket = f"iam-denied-{uuid.uuid4().hex[:8]}"
        try:
            # Create buckets first (without enforcement)
            admin_s3 = _make_s3_client("testing", "testing")
            admin_s3.create_bucket(Bucket=allowed_bucket)
            admin_s3.create_bucket(Bucket=denied_bucket)

            key = _create_user_with_policy(
                iam,
                user_name,
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "s3:*",
                            "Resource": [
                                f"arn:aws:s3:::{allowed_bucket}",
                                f"arn:aws:s3:::{allowed_bucket}/*",
                            ],
                        },
                    ],
                },
            )
            _set_enforce_iam(True)

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])

            # Allowed bucket: HeadBucket should succeed
            resp = s3.head_bucket(Bucket=allowed_bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Denied bucket: HeadBucket should fail
            with pytest.raises(ClientError) as exc_info:
                s3.head_bucket(Bucket=denied_bucket)
            assert exc_info.value.response["Error"]["Code"] in ("AccessDenied", "403")
        finally:
            _set_enforce_iam(False)
            try:
                admin_s3 = _make_s3_client("testing", "testing")
                admin_s3.delete_bucket(Bucket=allowed_bucket)
                admin_s3.delete_bucket(Bucket=denied_bucket)
            except Exception:
                pass  # best-effort cleanup
            _cleanup_user(iam, user_name)


class TestIAMAndSTSExempt:
    """Test that IAM and STS requests bypass enforcement."""

    def test_iam_requests_always_allowed(self, iam):
        """IAM operations work even with enforcement on and no policies."""
        user_name = f"iam-exempt-{uuid.uuid4().hex[:8]}"
        try:
            # Create a user with no policies
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            _set_enforce_iam(True)

            # IAM client with the no-policy user's creds
            user_iam = boto3.client(
                "iam",
                endpoint_url=ENDPOINT_URL,
                region_name="us-east-1",
                aws_access_key_id=key["AccessKeyId"],
                aws_secret_access_key=key["SecretAccessKey"],
            )
            # IAM is exempt from enforcement
            resp = user_iam.list_users()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Users" in resp
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)

    def test_sts_requests_always_allowed(self, iam):
        """STS operations work even with enforcement on."""
        _set_enforce_iam(True)

        sts = boto3.client(
            "sts",
            endpoint_url=ENDPOINT_URL,
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        # STS is exempt from enforcement
        resp = sts.get_caller_identity()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Account" in resp


class TestMultiplePolicies:
    """Test that multiple policies are evaluated together."""

    def test_union_of_multiple_allow_policies(self, iam):
        """Multiple Allow policies: union of permissions."""
        user_name = f"multi-policy-{uuid.uuid4().hex[:8]}"
        try:
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            # S3 policy
            iam.put_user_policy(
                UserName=user_name,
                PolicyName="S3Policy",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
                    }
                ),
            )
            # DynamoDB policy
            iam.put_user_policy(
                UserName=user_name,
                PolicyName="DDBPolicy",
                PolicyDocument=json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}],
                    }
                ),
            )
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            _set_enforce_iam(True)

            # Both S3 and DynamoDB should work
            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = s3.list_buckets()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            ddb = _make_dynamodb_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = ddb.list_tables()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # SQS should be denied (no policy)
            sqs = _make_sqs_client(key["AccessKeyId"], key["SecretAccessKey"])
            with pytest.raises(ClientError) as exc_info:
                sqs.list_queues()
            assert exc_info.value.response["Error"]["Code"] in (
                "AccessDenied",
                "AccessDeniedException",
            )
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)


class TestEnforcementToggle:
    """Test that toggling ENFORCE_IAM on/off works at runtime."""

    def test_enforcement_off_allows_everything(self, iam):
        """With ENFORCE_IAM=0, even users with no policies are allowed."""
        user_name = f"toggle-off-{uuid.uuid4().hex[:8]}"
        try:
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            # Enforcement OFF — should be allowed
            _set_enforce_iam(False)
            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])
            resp = s3.list_buckets()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            _cleanup_user(iam, user_name)

    def test_toggle_on_then_off(self, iam):
        """Toggling enforcement on then off works correctly."""
        user_name = f"toggle-both-{uuid.uuid4().hex[:8]}"
        try:
            try:
                iam.create_user(UserName=user_name)
            except ClientError:
                pass  # resource may already be cleaned up
            key_resp = iam.create_access_key(UserName=user_name)
            key = key_resp["AccessKey"]

            s3 = _make_s3_client(key["AccessKeyId"], key["SecretAccessKey"])

            # ON: should be denied (no policies)
            _set_enforce_iam(True)
            with pytest.raises(ClientError) as exc_info:
                s3.list_buckets()
            assert exc_info.value.response["Error"]["Code"] == "AccessDenied"

            # OFF: should be allowed
            _set_enforce_iam(False)
            resp = s3.list_buckets()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            _set_enforce_iam(False)
            _cleanup_user(iam, user_name)
