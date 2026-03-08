"""Shield compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def shield():
    return make_client("shield")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _make_resource_arn():
    return f"arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-{uuid.uuid4().hex[:8]}"


class TestShieldSubscription:
    """Tests for Shield subscription operations."""

    def test_create_subscription(self, shield):
        resp = shield.create_subscription()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_subscription(self, shield):
        shield.create_subscription()
        resp = shield.describe_subscription()
        sub = resp["Subscription"]
        assert "StartTime" in sub
        assert "EndTime" in sub
        assert "TimeCommitmentInSeconds" in sub
        assert "AutoRenew" in sub
        assert "SubscriptionArn" in sub


class TestShieldProtectionOperations:
    """Tests for Shield protection create, describe, list, delete."""

    def test_create_and_describe_protection(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]
        assert protection_id

        desc = shield.describe_protection(ProtectionId=protection_id)
        prot = desc["Protection"]
        assert prot["Id"] == protection_id
        assert prot["Name"] == name
        assert prot["ResourceArn"] == arn

        # Cleanup
        shield.delete_protection(ProtectionId=protection_id)

    def test_list_protections(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]

        listed = shield.list_protections()
        names = [p["Name"] for p in listed["Protections"]]
        assert name in names

        # Cleanup
        shield.delete_protection(ProtectionId=protection_id)

    def test_delete_protection(self, shield):
        name = _unique("prot")
        arn = _make_resource_arn()
        resp = shield.create_protection(Name=name, ResourceArn=arn)
        protection_id = resp["ProtectionId"]

        shield.delete_protection(ProtectionId=protection_id)

        # Verify deletion
        with pytest.raises(shield.exceptions.ResourceNotFoundException):
            shield.describe_protection(ProtectionId=protection_id)
