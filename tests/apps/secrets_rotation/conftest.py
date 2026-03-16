"""
Fixtures for the secrets rotation application tests.
"""

import pytest

from .app import SecretsVault


@pytest.fixture
def vault(secretsmanager, dynamodb, unique_name):
    """
    A fully initialized SecretsVault with DynamoDB tables created.
    Cleans up all tables and any created secrets on teardown.
    """
    audit_table = f"audit-{unique_name}"
    rotation_table = f"rotation-{unique_name}"
    policy_table = f"policy-{unique_name}"

    v = SecretsVault(
        secretsmanager_client=secretsmanager,
        dynamodb_client=dynamodb,
        audit_table_name=audit_table,
        rotation_table_name=rotation_table,
        policy_table_name=policy_table,
    )
    v.create_tables()

    # Track created secrets for cleanup
    v._created_secrets = []
    original_create = v.create_secret

    def tracked_create(*args, **kwargs):
        secret = original_create(*args, **kwargs)
        v._created_secrets.append(secret.full_name)
        return secret

    v.create_secret = tracked_create

    yield v

    # Cleanup secrets
    for full_name in v._created_secrets:
        try:
            secretsmanager.delete_secret(SecretId=full_name, ForceDeleteWithoutRecovery=True)
        except Exception:
            pass  # best-effort cleanup

    v.delete_tables()


@pytest.fixture
def db_credentials():
    """Sample database credential secret value."""
    return {
        "host": "db.prod.example.com",
        "port": 5432,
        "username": "app_service",
        "password": "s3cur3-pr0d-p@ssw0rd!",
    }


@pytest.fixture
def api_key_secret():
    """Sample API key secret value."""
    return {
        "key": "ak-0123456789abcdef",
        "service": "payment-gateway",
    }


@pytest.fixture
def certificate_secret():
    """Sample TLS certificate secret value."""
    return {
        "cert_body": "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW...\n-----END CERTIFICATE-----",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBAD...\n-----END PRIVATE KEY-----",
    }
