"""Batch 7 lifecycle tests: IdentityStore, Batch ConsumableResource."""

import boto3
import pytest

CREDS = {
    "endpoint_url": "http://localhost:4566",
    "aws_access_key_id": "123456789012",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


@pytest.fixture
def ids_client():
    return boto3.client("identitystore", **CREDS)


@pytest.fixture
def batch_client():
    return boto3.client("batch", **CREDS)


STORE_ID = "d-1234567890"


# ---------------------------------------------------------------------------
# IdentityStore: UpdateGroup
# ---------------------------------------------------------------------------


def test_identitystore_update_group(ids_client):
    grp = ids_client.create_group(
        IdentityStoreId=STORE_ID,
        DisplayName="test-grp-b7",
        Description="original",
    )
    group_id = grp["GroupId"]

    ids_client.update_group(
        IdentityStoreId=STORE_ID,
        GroupId=group_id,
        Operations=[
            {
                "AttributePath": "DisplayName",
                "AttributeValue": "renamed-grp-b7",
            }
        ],
    )
    desc = ids_client.describe_group(IdentityStoreId=STORE_ID, GroupId=group_id)
    assert desc["DisplayName"] == "renamed-grp-b7"

    ids_client.delete_group(IdentityStoreId=STORE_ID, GroupId=group_id)


# ---------------------------------------------------------------------------
# IdentityStore: UpdateUser
# ---------------------------------------------------------------------------


def test_identitystore_update_user(ids_client):
    usr = ids_client.create_user(
        IdentityStoreId=STORE_ID,
        UserName="test-user-b7",
        DisplayName="Original Name",
        Name={"GivenName": "Test", "FamilyName": "User"},
    )
    user_id = usr["UserId"]

    ids_client.update_user(
        IdentityStoreId=STORE_ID,
        UserId=user_id,
        Operations=[
            {
                "AttributePath": "DisplayName",
                "AttributeValue": "Updated Name",
            }
        ],
    )
    desc = ids_client.describe_user(IdentityStoreId=STORE_ID, UserId=user_id)
    assert desc["DisplayName"] == "Updated Name"

    ids_client.delete_user(IdentityStoreId=STORE_ID, UserId=user_id)


# ---------------------------------------------------------------------------
# IdentityStore: DescribeGroupMembership
# ---------------------------------------------------------------------------


def test_identitystore_describe_group_membership(ids_client):
    grp = ids_client.create_group(
        IdentityStoreId=STORE_ID,
        DisplayName="test-grp-memb-b7",
    )
    group_id = grp["GroupId"]

    usr = ids_client.create_user(
        IdentityStoreId=STORE_ID,
        UserName="test-user-memb-b7",
        DisplayName="Test Memb User",
        Name={"GivenName": "Test", "FamilyName": "Memb"},
    )
    user_id = usr["UserId"]

    membership = ids_client.create_group_membership(
        IdentityStoreId=STORE_ID,
        GroupId=group_id,
        MemberId={"UserId": user_id},
    )
    membership_id = membership["MembershipId"]

    desc = ids_client.describe_group_membership(
        IdentityStoreId=STORE_ID,
        MembershipId=membership_id,
    )
    assert desc["MembershipId"] == membership_id
    assert desc["GroupId"] == group_id
    assert desc["MemberId"]["UserId"] == user_id

    ids_client.delete_group_membership(IdentityStoreId=STORE_ID, MembershipId=membership_id)
    ids_client.delete_user(IdentityStoreId=STORE_ID, UserId=user_id)
    ids_client.delete_group(IdentityStoreId=STORE_ID, GroupId=group_id)


def test_identitystore_describe_group_membership_not_found(ids_client):
    with pytest.raises(Exception) as exc_info:
        ids_client.describe_group_membership(
            IdentityStoreId=STORE_ID,
            MembershipId="fake-membership-id",
        )
    assert (
        "ResourceNotFoundException" in str(type(exc_info.value).__name__)
        or "not found" in str(exc_info.value).lower()
    )


# ---------------------------------------------------------------------------
# Batch: ConsumableResource CRUD (already implemented, just verify)
# ---------------------------------------------------------------------------


def test_batch_consumable_resource_lifecycle(batch_client):
    resp = batch_client.create_consumable_resource(
        consumableResourceName="test-cr-b7",
        totalQuantity=100,
        resourceType="REPLENISHABLE",
    )
    assert resp["consumableResourceName"] == "test-cr-b7"
    arn = resp["consumableResourceArn"]
    assert "test-cr-b7" in arn

    desc = batch_client.describe_consumable_resource(
        consumableResource=arn,
    )
    assert desc["consumableResourceName"] == "test-cr-b7"
    assert desc["totalQuantity"] == 100

    resources = batch_client.list_consumable_resources()
    names = [r["consumableResourceName"] for r in resources["consumableResources"]]
    assert "test-cr-b7" in names

    batch_client.delete_consumable_resource(consumableResource=arn)
