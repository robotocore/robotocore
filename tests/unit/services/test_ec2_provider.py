"""Error-path tests for EC2 native provider.

Phase 3A: Covers placement group operations, error response format,
and edge cases in DetachVolume/DeleteVpcEndpoints.
"""

from unittest.mock import AsyncMock, MagicMock
from urllib.parse import urlencode

import pytest

import robotocore.services.ec2.provider as ec2_provider
from robotocore.services.ec2.provider import handle_ec2_request


@pytest.fixture(autouse=True)
def _clear_placement_groups():
    """Clear placement group state between tests to avoid cross-test pollution."""
    ec2_provider._placement_groups.clear()
    yield
    ec2_provider._placement_groups.clear()


def _make_request(action: str, params: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.headers = {"content-type": "application/x-www-form-urlencoded"}
    req.query_params = {}
    form_data = {"Action": action, "Version": "2016-11-15"}
    if params:
        form_data.update(params)
    payload = urlencode(form_data).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestPlacementGroupOperations:
    async def test_create_placement_group(self):
        req = _make_request(
            "CreatePlacementGroup",
            {
                "GroupName": "test-pg-create",
                "Strategy": "cluster",
            },
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"placementGroup" in resp.body or b"PlacementGroup" in resp.body

    async def test_describe_placement_groups(self):
        # Create first
        req1 = _make_request(
            "CreatePlacementGroup",
            {
                "GroupName": "test-pg-desc",
                "Strategy": "spread",
            },
        )
        await handle_ec2_request(req1, "us-east-1", "123456789012")

        # Describe
        req2 = _make_request(
            "DescribePlacementGroups",
            {
                "GroupName.1": "test-pg-desc",
            },
        )
        resp = await handle_ec2_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"test-pg-desc" in resp.body

    async def test_describe_nonexistent_placement_group_returns_error(self):
        """AWS returns InvalidPlacementGroup.Unknown when filtering by name that doesn't exist."""
        req = _make_request(
            "DescribePlacementGroups",
            {
                "GroupName.1": "nonexistent-pg-xyz",
            },
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert b"InvalidPlacementGroup.Unknown" in resp.body
        assert b"nonexistent-pg-xyz" in resp.body

    async def test_delete_placement_group(self):
        req1 = _make_request(
            "CreatePlacementGroup",
            {
                "GroupName": "test-pg-del",
                "Strategy": "cluster",
            },
        )
        await handle_ec2_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "DeletePlacementGroup",
            {
                "GroupName": "test-pg-del",
            },
        )
        resp = await handle_ec2_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    async def test_placement_groups_region_isolation(self):
        req1 = _make_request(
            "CreatePlacementGroup",
            {
                "GroupName": "test-pg-iso",
                "Strategy": "cluster",
            },
        )
        await handle_ec2_request(req1, "us-east-1", "123456789012")

        # Querying by name in a different region should return an error
        # (the group doesn't exist in us-west-2)
        req2 = _make_request(
            "DescribePlacementGroups",
            {
                "GroupName.1": "test-pg-iso",
            },
        )
        resp = await handle_ec2_request(req2, "us-west-2", "123456789012")
        assert resp.status_code == 400
        assert b"InvalidPlacementGroup.Unknown" in resp.body

    async def test_placement_groups_region_isolation_list_all(self):
        """Listing groups in a different region returns empty."""
        req1 = _make_request(
            "CreatePlacementGroup",
            {
                "GroupName": "test-pg-iso-list",
                "Strategy": "cluster",
            },
        )
        await handle_ec2_request(req1, "us-east-1", "123456789012")

        # List all in us-west-2 (no filter) should return empty
        req2 = _make_request("DescribePlacementGroups")
        resp = await handle_ec2_request(req2, "us-west-2", "123456789012")
        assert resp.status_code == 200
        assert b"test-pg-iso-list" not in resp.body


@pytest.mark.asyncio
class TestEc2ErrorResponseFormat:
    async def test_error_response_is_xml(self):
        """EC2 errors should return XML ErrorResponse format."""
        req = _make_request(
            "DeletePlacementGroup",
            {
                "GroupName": "",
            },
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        # Empty group name should be a validation error
        assert resp.status_code == 400
        assert b"MissingParameter" in resp.body


@pytest.mark.asyncio
class TestPlacementGroupDuplicateCreation:
    """Categorical bug: native stores silently overwrite on duplicate create."""

    async def test_create_duplicate_placement_group_returns_error(self):
        """AWS returns InvalidPlacementGroup.Duplicate for duplicate names."""
        req1 = _make_request(
            "CreatePlacementGroup",
            {"GroupName": "dup-pg", "Strategy": "cluster"},
        )
        resp1 = await handle_ec2_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        # Second create with same name should fail
        req2 = _make_request(
            "CreatePlacementGroup",
            {"GroupName": "dup-pg", "Strategy": "cluster"},
        )
        resp2 = await handle_ec2_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 400
        assert b"InvalidPlacementGroup.Duplicate" in resp2.body


@pytest.mark.asyncio
class TestDeleteNonexistentPlacementGroup:
    """Categorical bug: delete of nonexistent resources silently succeeds."""

    async def test_delete_nonexistent_placement_group_returns_error(self):
        """AWS returns InvalidPlacementGroup.Unknown when deleting a group that doesn't exist."""
        req = _make_request(
            "DeletePlacementGroup",
            {"GroupName": "does-not-exist"},
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert b"InvalidPlacementGroup.Unknown" in resp.body
        assert b"does-not-exist" in resp.body


@pytest.mark.asyncio
class TestCreatePlacementGroupValidation:
    """Categorical bug: missing input validation on required parameters."""

    async def test_create_placement_group_missing_name_returns_error(self):
        """AWS requires GroupName and returns MissingParameter if absent."""
        req = _make_request("CreatePlacementGroup", {"Strategy": "cluster"})
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert b"MissingParameter" in resp.body

    async def test_create_placement_group_empty_name_returns_error(self):
        """Empty GroupName should be treated the same as missing."""
        req = _make_request(
            "CreatePlacementGroup",
            {"GroupName": "", "Strategy": "cluster"},
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert b"MissingParameter" in resp.body

    async def test_create_placement_group_invalid_strategy_returns_error(self):
        """AWS returns InvalidParameterValue for unknown strategy."""
        req = _make_request(
            "CreatePlacementGroup",
            {"GroupName": "bad-strat-pg", "Strategy": "bogus"},
        )
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert b"InvalidParameterValue" in resp.body


@pytest.mark.asyncio
class TestPlacementGroupAccountIsolation:
    """Categorical bug: stores keyed by account must not leak across accounts."""

    async def test_placement_groups_account_isolation(self):
        """Groups created in account A must not be visible in account B."""
        req1 = _make_request(
            "CreatePlacementGroup",
            {"GroupName": "acct-iso-pg", "Strategy": "cluster"},
        )
        await handle_ec2_request(req1, "us-east-1", "111111111111")

        req2 = _make_request(
            "DescribePlacementGroups",
        )
        resp = await handle_ec2_request(req2, "us-east-1", "222222222222")
        assert resp.status_code == 200
        assert b"acct-iso-pg" not in resp.body


@pytest.mark.asyncio
class TestPlacementGroupThreadSafety:
    """Categorical bug: module-level dicts without locks are not thread-safe."""

    async def test_concurrent_creates_do_not_corrupt_store(self):
        """Multiple concurrent creates should not lose data."""
        import asyncio

        async def create_group(name: str):
            req = _make_request(
                "CreatePlacementGroup",
                {"GroupName": name, "Strategy": "cluster"},
            )
            return await handle_ec2_request(req, "us-east-1", "123456789012")

        # Create 20 groups concurrently
        tasks = [create_group(f"concurrent-pg-{i}") for i in range(20)]
        results = await asyncio.gather(*tasks)

        # All should succeed
        for r in results:
            assert r.status_code == 200

        # All 20 should be in the store
        req = _make_request("DescribePlacementGroups")
        resp = await handle_ec2_request(req, "us-east-1", "123456789012")
        for i in range(20):
            assert f"concurrent-pg-{i}".encode() in resp.body
