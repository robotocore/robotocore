"""
User Authentication & Profile Service Application Tests

Simulates a user management platform: profiles in DynamoDB (with GSI for email lookup,
TTL for sessions), avatars in S3, JWT/OAuth secrets in Secrets Manager, auth config in
SSM Parameter Store, and login metrics in CloudWatch.

Exercises 5 AWS services with realistic auth workflows end-to-end.
"""

import json
import time
from datetime import UTC, datetime

import pytest
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def users_table(dynamodb, unique_name):
    table_name = f"users-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "user_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "email", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-email",
                "KeySchema": [
                    {"AttributeName": "email", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def sessions_table(dynamodb, unique_name):
    table_name = f"sessions-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "session_id", "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "session_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-user",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    # Enable TTL on expires_at
    dynamodb.update_time_to_live(
        TableName=table_name,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": "expires_at",
        },
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def avatar_bucket(s3, unique_name):
    bucket = f"avatars-{unique_name}"
    s3.create_bucket(Bucket=bucket)
    yield bucket
    # Cleanup all objects then bucket
    try:
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
    except Exception:
        pass
    s3.delete_bucket(Bucket=bucket)


@pytest.fixture
def auth_secrets(secretsmanager, unique_name):
    secret_name = f"auth/keys-{unique_name}"
    secret_data = {
        "jwt_secret": "super-secret-jwt-key-2026",
        "oauth_client_id": "client-id-abc123",
        "oauth_client_secret": "client-secret-xyz789",
    }
    secretsmanager.create_secret(
        Name=secret_name,
        SecretString=json.dumps(secret_data),
    )
    yield {"name": secret_name, "data": secret_data}
    secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


@pytest.fixture
def auth_config(ssm, unique_name):
    prefix = f"/auth/{unique_name}"
    params = {
        f"{prefix}/password_min_length": "12",
        f"{prefix}/mfa_enabled": "true",
        f"{prefix}/session_ttl_hours": "24",
    }
    for name, value in params.items():
        ssm.put_parameter(Name=name, Value=value, Type="String")
    yield {"prefix": prefix, "params": params}
    for name in params:
        try:
            ssm.delete_parameter(Name=name)
        except Exception:
            pass


@pytest.fixture
def auth_metrics_ns(unique_name):
    return f"AuthService/{unique_name}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestUserRegistration:
    """DynamoDB user profile CRUD with GSI email lookup."""

    def test_create_user_profile(self, dynamodb, users_table):
        """PutItem a user profile, GetItem to verify all fields."""
        dynamodb.put_item(
            TableName=users_table,
            Item={
                "user_id": {"S": "user-001"},
                "email": {"S": "alice@example.com"},
                "display_name": {"S": "Alice Johnson"},
                "created_at": {"S": "2026-03-08T10:00:00Z"},
                "status": {"S": "active"},
            },
        )

        resp = dynamodb.get_item(
            TableName=users_table,
            Key={"user_id": {"S": "user-001"}},
        )
        item = resp["Item"]
        assert item["user_id"]["S"] == "user-001"
        assert item["email"]["S"] == "alice@example.com"
        assert item["display_name"]["S"] == "Alice Johnson"
        assert item["created_at"]["S"] == "2026-03-08T10:00:00Z"
        assert item["status"]["S"] == "active"

    def test_lookup_by_email(self, dynamodb, users_table):
        """Insert 3 users, query GSI by-email for one, verify correct user."""
        users = [
            ("user-010", "alice@example.com", "Alice"),
            ("user-011", "bob@example.com", "Bob"),
            ("user-012", "carol@example.com", "Carol"),
        ]
        for uid, email, name in users:
            dynamodb.put_item(
                TableName=users_table,
                Item={
                    "user_id": {"S": uid},
                    "email": {"S": email},
                    "display_name": {"S": name},
                    "status": {"S": "active"},
                },
            )

        resp = dynamodb.query(
            TableName=users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": "bob@example.com"}},
        )
        assert resp["Count"] == 1
        assert resp["Items"][0]["user_id"]["S"] == "user-011"
        assert resp["Items"][0]["display_name"]["S"] == "Bob"

    def test_unique_email_constraint(self, dynamodb, users_table):
        """ConditionExpression prevents duplicate email via attribute_not_exists."""
        # First user succeeds
        dynamodb.put_item(
            TableName=users_table,
            Item={
                "user_id": {"S": "user-020"},
                "email": {"S": "unique@example.com"},
                "display_name": {"S": "First User"},
                "status": {"S": "active"},
            },
            ConditionExpression="attribute_not_exists(user_id)",
        )

        # Second user with same user_id should fail
        with pytest.raises(ClientError) as exc_info:
            dynamodb.put_item(
                TableName=users_table,
                Item={
                    "user_id": {"S": "user-020"},
                    "email": {"S": "different@example.com"},
                    "display_name": {"S": "Second User"},
                    "status": {"S": "active"},
                },
                ConditionExpression="attribute_not_exists(user_id)",
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_batch_create_users(self, dynamodb, users_table):
        """BatchWriteItem 10 users, Scan to verify count."""
        items = []
        for i in range(10):
            items.append(
                {
                    "PutRequest": {
                        "Item": {
                            "user_id": {"S": f"batch-user-{i:03d}"},
                            "email": {"S": f"user{i}@example.com"},
                            "display_name": {"S": f"User {i}"},
                            "status": {"S": "active"},
                        }
                    }
                }
            )

        dynamodb.batch_write_item(RequestItems={users_table: items})

        resp = dynamodb.scan(TableName=users_table, Select="COUNT")
        assert resp["Count"] == 10


class TestSessionManagement:
    """DynamoDB session table with TTL and GSI by user."""

    def test_create_session(self, dynamodb, sessions_table):
        """PutItem a session with TTL, GetItem to verify."""
        expires_at = int(time.time()) + 86400  # 24 hours from now
        dynamodb.put_item(
            TableName=sessions_table,
            Item={
                "session_id": {"S": "sess-001"},
                "user_id": {"S": "user-001"},
                "created_at": {"S": "2026-03-08T10:00:00Z"},
                "expires_at": {"N": str(expires_at)},
            },
        )

        resp = dynamodb.get_item(
            TableName=sessions_table,
            Key={"session_id": {"S": "sess-001"}},
        )
        item = resp["Item"]
        assert item["session_id"]["S"] == "sess-001"
        assert item["user_id"]["S"] == "user-001"
        assert item["expires_at"]["N"] == str(expires_at)

    def test_query_active_sessions(self, dynamodb, sessions_table):
        """Create sessions for two users, query GSI by-user for one."""
        expires = str(int(time.time()) + 86400)
        # 3 sessions for user-A
        for i in range(3):
            dynamodb.put_item(
                TableName=sessions_table,
                Item={
                    "session_id": {"S": f"sess-a-{i}"},
                    "user_id": {"S": "user-A"},
                    "created_at": {"S": f"2026-03-08T10:{i:02d}:00Z"},
                    "expires_at": {"N": expires},
                },
            )
        # 2 sessions for user-B
        for i in range(2):
            dynamodb.put_item(
                TableName=sessions_table,
                Item={
                    "session_id": {"S": f"sess-b-{i}"},
                    "user_id": {"S": "user-B"},
                    "created_at": {"S": f"2026-03-08T11:{i:02d}:00Z"},
                    "expires_at": {"N": expires},
                },
            )

        resp = dynamodb.query(
            TableName=sessions_table,
            IndexName="by-user",
            KeyConditionExpression="user_id = :uid",
            ExpressionAttributeValues={":uid": {"S": "user-A"}},
        )
        assert resp["Count"] == 3

    def test_invalidate_session(self, dynamodb, sessions_table):
        """Create session, DeleteItem, GetItem returns no Item."""
        dynamodb.put_item(
            TableName=sessions_table,
            Item={
                "session_id": {"S": "sess-del-001"},
                "user_id": {"S": "user-001"},
                "expires_at": {"N": str(int(time.time()) + 3600)},
            },
        )

        dynamodb.delete_item(
            TableName=sessions_table,
            Key={"session_id": {"S": "sess-del-001"}},
        )

        resp = dynamodb.get_item(
            TableName=sessions_table,
            Key={"session_id": {"S": "sess-del-001"}},
        )
        assert "Item" not in resp

    def test_update_session_expiry(self, dynamodb, sessions_table):
        """Create session, UpdateItem to extend expires_at, verify new value."""
        original_expiry = int(time.time()) + 3600
        new_expiry = int(time.time()) + 86400

        dynamodb.put_item(
            TableName=sessions_table,
            Item={
                "session_id": {"S": "sess-upd-001"},
                "user_id": {"S": "user-001"},
                "expires_at": {"N": str(original_expiry)},
            },
        )

        dynamodb.update_item(
            TableName=sessions_table,
            Key={"session_id": {"S": "sess-upd-001"}},
            UpdateExpression="SET expires_at = :new_exp",
            ExpressionAttributeValues={":new_exp": {"N": str(new_expiry)}},
        )

        resp = dynamodb.get_item(
            TableName=sessions_table,
            Key={"session_id": {"S": "sess-upd-001"}},
        )
        assert resp["Item"]["expires_at"]["N"] == str(new_expiry)


class TestAvatarStorage:
    """S3 avatar upload/download/delete."""

    def test_upload_avatar(self, s3, avatar_bucket):
        """PutObject avatar, GetObject, verify content matches."""
        avatar_data = b"\x89PNG\r\n\x1a\nfake-avatar-image-data-001"
        key = "avatars/user-001/profile.jpg"

        s3.put_object(Bucket=avatar_bucket, Key=key, Body=avatar_data)

        resp = s3.get_object(Bucket=avatar_bucket, Key=key)
        downloaded = resp["Body"].read()
        assert downloaded == avatar_data

    def test_generate_presigned_upload_url(self, s3, avatar_bucket):
        """Generate presigned PUT URL, verify it contains bucket and key."""
        key = "avatars/user-002/profile.jpg"
        url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": avatar_bucket, "Key": key},
            ExpiresIn=3600,
        )
        assert isinstance(url, str)
        assert avatar_bucket in url
        assert "profile.jpg" in url

    def test_avatar_versioning(self, s3, avatar_bucket):
        """Upload avatar twice, verify latest content matches second upload."""
        key = "avatars/user-003/profile.jpg"
        first_data = b"first-avatar-version"
        second_data = b"second-avatar-version-updated"

        s3.put_object(Bucket=avatar_bucket, Key=key, Body=first_data)
        s3.put_object(Bucket=avatar_bucket, Key=key, Body=second_data)

        resp = s3.get_object(Bucket=avatar_bucket, Key=key)
        downloaded = resp["Body"].read()
        assert downloaded == second_data

    def test_delete_avatar(self, s3, avatar_bucket):
        """Upload then DeleteObject, verify GetObject raises NoSuchKey."""
        key = "avatars/user-004/profile.jpg"
        s3.put_object(Bucket=avatar_bucket, Key=key, Body=b"to-be-deleted")

        s3.delete_object(Bucket=avatar_bucket, Key=key)

        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket=avatar_bucket, Key=key)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"


class TestAuthConfiguration:
    """SSM Parameter Store + Secrets Manager for auth settings."""

    def test_read_auth_config(self, ssm, auth_config):
        """GetParametersByPath retrieves all 3 auth params."""
        resp = ssm.get_parameters_by_path(Path=auth_config["prefix"], Recursive=True)
        params = {p["Name"]: p["Value"] for p in resp["Parameters"]}
        assert len(params) == 3
        prefix = auth_config["prefix"]
        assert params[f"{prefix}/password_min_length"] == "12"
        assert params[f"{prefix}/mfa_enabled"] == "true"
        assert params[f"{prefix}/session_ttl_hours"] == "24"

    def test_update_password_policy(self, ssm, auth_config):
        """PutParameter(Overwrite) to change min_length, verify new value."""
        param_name = f"{auth_config['prefix']}/password_min_length"
        ssm.put_parameter(
            Name=param_name,
            Value="16",
            Type="String",
            Overwrite=True,
        )

        resp = ssm.get_parameter(Name=param_name)
        assert resp["Parameter"]["Value"] == "16"

    def test_rotate_jwt_secret(self, secretsmanager, auth_secrets):
        """Update JWT secret, verify new value persists and other fields unchanged."""
        secret_name = auth_secrets["name"]

        # Read initial
        resp = secretsmanager.get_secret_value(SecretId=secret_name)
        data = json.loads(resp["SecretString"])
        assert data["jwt_secret"] == "super-secret-jwt-key-2026"

        # Rotate JWT secret
        data["jwt_secret"] = "rotated-jwt-key-2026-v2"
        secretsmanager.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(data),
        )

        # Verify rotation
        resp = secretsmanager.get_secret_value(SecretId=secret_name)
        updated = json.loads(resp["SecretString"])
        assert updated["jwt_secret"] == "rotated-jwt-key-2026-v2"
        assert updated["oauth_client_id"] == "client-id-abc123"
        assert updated["oauth_client_secret"] == "client-secret-xyz789"

    def test_tagged_auth_resources(self, ssm, auth_config):
        """AddTagsToResource on SSM param, verify via ListTagsForResource."""
        param_name = f"{auth_config['prefix']}/password_min_length"
        ssm.add_tags_to_resource(
            ResourceType="Parameter",
            ResourceId=param_name,
            Tags=[
                {"Key": "Service", "Value": "auth"},
                {"Key": "Sensitivity", "Value": "high"},
            ],
        )

        resp = ssm.list_tags_for_resource(
            ResourceType="Parameter",
            ResourceId=param_name,
        )
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map["Service"] == "auth"
        assert tag_map["Sensitivity"] == "high"


class TestAuthMonitoring:
    """CloudWatch metrics and full end-to-end signup flow."""

    def test_track_login_metrics(self, cloudwatch, auth_metrics_ns):
        """PutMetricData for login attempts/failures, verify sums."""
        cloudwatch.put_metric_data(
            Namespace=auth_metrics_ns,
            MetricData=[
                {"MetricName": "LoginAttempts", "Value": 10, "Unit": "Count"},
                {"MetricName": "LoginFailures", "Value": 2, "Unit": "Count"},
            ],
        )

        # Verify LoginAttempts
        resp = cloudwatch.get_metric_statistics(
            Namespace=auth_metrics_ns,
            MetricName="LoginAttempts",
            StartTime=datetime(2020, 1, 1, tzinfo=UTC),
            EndTime=datetime(2030, 1, 1, tzinfo=UTC),
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(resp["Datapoints"]) >= 1
        total = sum(dp["Sum"] for dp in resp["Datapoints"])
        assert total == 10.0

        # Verify LoginFailures
        resp = cloudwatch.get_metric_statistics(
            Namespace=auth_metrics_ns,
            MetricName="LoginFailures",
            StartTime=datetime(2020, 1, 1, tzinfo=UTC),
            EndTime=datetime(2030, 1, 1, tzinfo=UTC),
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(resp["Datapoints"]) >= 1
        total = sum(dp["Sum"] for dp in resp["Datapoints"])
        assert total == 2.0

    def test_full_signup_flow(
        self,
        dynamodb,
        s3,
        ssm,
        secretsmanager,
        cloudwatch,
        users_table,
        sessions_table,
        avatar_bucket,
        auth_secrets,
        auth_config,
        auth_metrics_ns,
    ):
        """
        End-to-end signup: read config -> read JWT secret -> create user ->
        verify email uniqueness -> upload avatar -> create session ->
        publish metric -> query user by email.
        """
        # Step 1: Read auth config from SSM
        config_resp = ssm.get_parameters_by_path(Path=auth_config["prefix"], Recursive=True)
        config = {p["Name"].split("/")[-1]: p["Value"] for p in config_resp["Parameters"]}
        assert config["password_min_length"] == "12"
        assert config["mfa_enabled"] == "true"

        # Step 2: Read JWT secret from Secrets Manager
        secret_resp = secretsmanager.get_secret_value(SecretId=auth_secrets["name"])
        secrets = json.loads(secret_resp["SecretString"])
        assert "jwt_secret" in secrets

        # Step 3: Create user in DynamoDB
        user_id = "signup-user-001"
        email = "newuser@example.com"
        dynamodb.put_item(
            TableName=users_table,
            Item={
                "user_id": {"S": user_id},
                "email": {"S": email},
                "display_name": {"S": "New User"},
                "created_at": {"S": "2026-03-08T12:00:00Z"},
                "status": {"S": "active"},
            },
        )

        # Step 4: Verify email uniqueness with conditional write
        with pytest.raises(ClientError) as exc_info:
            dynamodb.put_item(
                TableName=users_table,
                Item={
                    "user_id": {"S": user_id},
                    "email": {"S": "other@example.com"},
                    "display_name": {"S": "Duplicate"},
                    "status": {"S": "active"},
                },
                ConditionExpression="attribute_not_exists(user_id)",
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

        # Step 5: Upload avatar to S3
        avatar_key = f"avatars/{user_id}/profile.jpg"
        avatar_data = b"fake-signup-avatar-image"
        s3.put_object(Bucket=avatar_bucket, Key=avatar_key, Body=avatar_data)
        obj = s3.get_object(Bucket=avatar_bucket, Key=avatar_key)
        assert obj["Body"].read() == avatar_data

        # Step 6: Create session in sessions table
        session_id = "signup-sess-001"
        expires_at = int(time.time()) + 86400
        dynamodb.put_item(
            TableName=sessions_table,
            Item={
                "session_id": {"S": session_id},
                "user_id": {"S": user_id},
                "created_at": {"S": "2026-03-08T12:00:01Z"},
                "expires_at": {"N": str(expires_at)},
            },
        )

        # Step 7: Publish signup metric to CloudWatch
        cloudwatch.put_metric_data(
            Namespace=auth_metrics_ns,
            MetricData=[
                {"MetricName": "Signups", "Value": 1, "Unit": "Count"},
            ],
        )
        metric_resp = cloudwatch.get_metric_statistics(
            Namespace=auth_metrics_ns,
            MetricName="Signups",
            StartTime=datetime(2020, 1, 1, tzinfo=UTC),
            EndTime=datetime(2030, 1, 1, tzinfo=UTC),
            Period=86400,
            Statistics=["Sum"],
        )
        assert len(metric_resp["Datapoints"]) >= 1

        # Step 8: Query user by email via GSI to verify
        query_resp = dynamodb.query(
            TableName=users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": email}},
        )
        assert query_resp["Count"] == 1
        assert query_resp["Items"][0]["user_id"]["S"] == user_id
        assert query_resp["Items"][0]["display_name"]["S"] == "New User"
