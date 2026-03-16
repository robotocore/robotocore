"""
Fixtures for the User Authentication & Identity Service tests.

Creates all AWS resources (DynamoDB tables, S3 bucket, Secrets Manager secrets,
SSM parameters, CloudWatch log groups) and provides a configured AuthService
instance plus pre-registered test users.
"""

import pytest

from .app import AuthService


@pytest.fixture
def reset_tokens_table(dynamodb, unique_name):
    """DynamoDB table for password reset tokens with TTL."""
    table_name = f"reset-tokens-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "token", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "token", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def users_table(dynamodb, unique_name):
    """DynamoDB table for user profiles with email GSI."""
    table_name = f"users-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-email",
                "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def sessions_table(dynamodb, unique_name):
    """DynamoDB table for sessions with TTL and user GSI."""
    table_name = f"sessions-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "session_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-user",
                "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    dynamodb.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def avatar_bucket(s3, unique_name):
    """S3 bucket for user avatars."""
    bucket = f"avatars-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass  # best-effort cleanup
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def secrets_prefix(unique_name):
    """Prefix for Secrets Manager secret names."""
    return f"auth/{unique_name}"


@pytest.fixture
def ssm_prefix(unique_name):
    """Prefix for SSM parameter paths."""
    return f"/auth/{unique_name}"


@pytest.fixture
def metrics_namespace(unique_name):
    """CloudWatch metrics namespace."""
    return f"AuthService/{unique_name}"


@pytest.fixture
def audit_log_group(unique_name):
    """CloudWatch Logs log group name."""
    return f"/auth/audit/{unique_name}"


@pytest.fixture
def auth_config(ssm, ssm_prefix):
    """Pre-loaded SSM auth configuration parameters."""
    params = {
        f"{ssm_prefix}/token_expiry_hours": "24",
        f"{ssm_prefix}/max_failed_attempts": "5",
        f"{ssm_prefix}/lockout_duration_minutes": "30",
        f"{ssm_prefix}/min_password_length": "8",
        f"{ssm_prefix}/require_special_chars": "true",
    }
    for name, value in params.items():
        ssm.put_parameter(Name=name, Value=value, Type="String")
    yield {"prefix": ssm_prefix, "params": params}
    for name in params:
        try:
            ssm.delete_parameter(Name=name)
        except Exception:
            pass  # best-effort cleanup


@pytest.fixture
def auth(
    dynamodb,
    s3,
    secretsmanager,
    ssm,
    cloudwatch,
    logs,
    users_table,
    sessions_table,
    reset_tokens_table,
    avatar_bucket,
    secrets_prefix,
    ssm_prefix,
    metrics_namespace,
    audit_log_group,
    auth_config,
):
    """Fully configured AuthService with all resources created."""
    return AuthService(
        dynamodb=dynamodb,
        s3=s3,
        secretsmanager=secretsmanager,
        ssm=ssm,
        cloudwatch=cloudwatch,
        logs=logs,
        users_table=users_table,
        sessions_table=sessions_table,
        reset_tokens_table=reset_tokens_table,
        avatar_bucket=avatar_bucket,
        secrets_prefix=secrets_prefix,
        ssm_prefix=ssm_prefix,
        metrics_namespace=metrics_namespace,
        audit_log_group=audit_log_group,
    )


@pytest.fixture
def registered_user(auth):
    """A pre-registered user for tests that need an existing account."""
    user = auth.register_user(
        email="testuser@example.com",
        password="SecureP@ss123!",
        name="Test User",
        bio="A test user for auth tests",
    )
    return {"user": user, "password": "SecureP@ss123!"}
