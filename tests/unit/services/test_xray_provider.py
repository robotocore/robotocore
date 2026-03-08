"""Error-path tests for X-Ray native provider.

Phase 3A: Covers sampling rules, groups, encryption config, and tagging.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.xray import provider as xray_provider
from robotocore.services.xray.provider import handle_xray_request


@pytest.fixture(autouse=True)
def _reset_xray_state():
    """Reset module-level state between tests to prevent cross-test pollution."""
    xray_provider._sampling_rules.clear()
    xray_provider._groups.clear()
    xray_provider._tags.clear()
    xray_provider._encryption_config.clear()
    yield
    xray_provider._sampling_rules.clear()
    xray_provider._groups.clear()
    xray_provider._tags.clear()
    xray_provider._encryption_config.clear()


def _make_request(method: str, path: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    payload = json.dumps(body or {}).encode() if body else b""
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestSamplingRules:
    async def test_create_and_get_sampling_rule(self):
        create_req = _make_request(
            "POST",
            "/CreateSamplingRule",
            {
                "SamplingRule": {
                    "RuleName": "test-rule",
                    "Priority": 100,
                    "FixedRate": 0.1,
                    "ReservoirSize": 5,
                    "ServiceName": "*",
                    "ServiceType": "*",
                    "Host": "*",
                    "ResourceARN": "*",
                    "HTTPMethod": "*",
                    "URLPath": "*",
                    "Version": 1,
                },
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "SamplingRuleRecord" in body

        # Get all rules
        get_req = _make_request("POST", "/GetSamplingRules")
        resp2 = await handle_xray_request(get_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        body2 = json.loads(resp2.body)
        assert "SamplingRuleRecords" in body2
        names = [r["SamplingRule"]["RuleName"] for r in body2["SamplingRuleRecords"]]
        assert "test-rule" in names

    async def test_delete_nonexistent_sampling_rule(self):
        req = _make_request(
            "POST",
            "/DeleteSamplingRule",
            {
                "RuleName": "nonexistent-rule-xyz",
            },
        )
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        # Should return empty record, not error
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["SamplingRuleRecord"] == {}


@pytest.mark.asyncio
class TestGroups:
    async def test_create_and_get_group(self):
        create_req = _make_request(
            "POST",
            "/CreateGroup",
            {
                "GroupName": "test-group",
                "FilterExpression": 'service("test")',
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "Group" in body
        assert body["Group"]["GroupName"] == "test-group"

        # Get the group
        get_req = _make_request(
            "POST",
            "/GetGroup",
            {
                "GroupName": "test-group",
            },
        )
        resp2 = await handle_xray_request(get_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200

    async def test_get_nonexistent_group(self):
        req = _make_request(
            "POST",
            "/GetGroup",
            {
                "GroupName": "nonexistent-group-xyz",
            },
        )
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["Group"] == {}

    async def test_delete_nonexistent_group(self):
        req = _make_request(
            "POST",
            "/DeleteGroup",
            {
                "GroupName": "nonexistent-group-xyz",
            },
        )
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200


@pytest.mark.asyncio
class TestEncryptionConfig:
    async def test_put_and_get_encryption_config(self):
        put_req = _make_request(
            "POST",
            "/PutEncryptionConfig",
            {
                "Type": "KMS",
                "KeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
            },
        )
        resp = await handle_xray_request(put_req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        get_req = _make_request("POST", "/EncryptionConfig")
        resp2 = await handle_xray_request(get_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        body = json.loads(resp2.body)
        assert "EncryptionConfig" in body


@pytest.mark.asyncio
class TestXRayTagging:
    async def test_tag_and_list_tags(self):
        arn = "arn:aws:xray:us-east-1:123456789012:group/test"
        tag_req = _make_request(
            "POST",
            "/TagResource",
            {
                "ResourceARN": arn,
                "Tags": [{"Key": "env", "Value": "test"}],
            },
        )
        resp = await handle_xray_request(tag_req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        list_req = _make_request(
            "POST",
            "/ListTagsForResource",
            {
                "ResourceARN": arn,
            },
        )
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        body = json.loads(resp2.body)
        assert "Tags" in body
        assert len(body["Tags"]) == 1
        assert body["Tags"][0]["Key"] == "env"

    async def test_untag_resource(self):
        arn = "arn:aws:xray:us-east-1:123456789012:group/test-untag"
        # Tag first
        tag_req = _make_request(
            "POST",
            "/TagResource",
            {
                "ResourceARN": arn,
                "Tags": [{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
            },
        )
        await handle_xray_request(tag_req, "us-east-1", "123456789012")

        # Untag k1
        untag_req = _make_request(
            "POST",
            "/UntagResource",
            {
                "ResourceARN": arn,
                "TagKeys": ["k1"],
            },
        )
        resp = await handle_xray_request(untag_req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # Verify only k2 remains
        list_req = _make_request(
            "POST",
            "/ListTagsForResource",
            {
                "ResourceARN": arn,
            },
        )
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        body = json.loads(resp2.body)
        assert len(body["Tags"]) == 1
        assert body["Tags"][0]["Key"] == "k2"


@pytest.mark.asyncio
class TestCategoricalBugs:
    """Tests for categorical bug patterns found across native providers."""

    async def test_delete_sampling_rule_cleans_up_tags(self):
        """BUG: Deleting a sampling rule leaves orphaned tags in _tags store.

        Categorical pattern: parent-child cascade — when a resource is deleted,
        its associated tags must also be removed.
        """
        # Create rule with tags
        create_req = _make_request(
            "POST",
            "/CreateSamplingRule",
            {
                "SamplingRule": {
                    "RuleName": "tagged-rule",
                    "Priority": 100,
                    "FixedRate": 0.1,
                    "ReservoirSize": 5,
                    "ServiceName": "*",
                    "ServiceType": "*",
                    "Host": "*",
                    "ResourceARN": "*",
                    "HTTPMethod": "*",
                    "URLPath": "*",
                    "Version": 1,
                },
                "Tags": [{"Key": "env", "Value": "prod"}],
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        rule_arn = body["SamplingRuleRecord"]["SamplingRule"]["RuleARN"]

        # Verify tags exist
        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": rule_arn})
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        assert len(json.loads(resp2.body)["Tags"]) == 1

        # Delete the rule
        del_req = _make_request("POST", "/DeleteSamplingRule", {"RuleName": "tagged-rule"})
        await handle_xray_request(del_req, "us-east-1", "123456789012")

        # Tags should be cleaned up — NOT left orphaned
        resp3 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        tags_after = json.loads(resp3.body)["Tags"]
        assert tags_after == [], f"Orphaned tags remain after sampling rule deletion: {tags_after}"

    async def test_delete_sampling_rule_by_arn_cleans_up_tags(self):
        """Same cascade bug but exercising the ARN-based delete path."""
        create_req = _make_request(
            "POST",
            "/CreateSamplingRule",
            {
                "SamplingRule": {
                    "RuleName": "arn-del-rule",
                    "Priority": 50,
                    "FixedRate": 0.05,
                    "ReservoirSize": 1,
                    "ServiceName": "*",
                    "ServiceType": "*",
                    "Host": "*",
                    "ResourceARN": "*",
                    "HTTPMethod": "*",
                    "URLPath": "*",
                    "Version": 1,
                },
                "Tags": [{"Key": "team", "Value": "infra"}],
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        rule_arn = body["SamplingRuleRecord"]["SamplingRule"]["RuleARN"]

        # Delete by ARN
        del_req = _make_request("POST", "/DeleteSamplingRule", {"RuleARN": rule_arn})
        await handle_xray_request(del_req, "us-east-1", "123456789012")

        # Tags should be cleaned up
        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": rule_arn})
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        assert json.loads(resp2.body)["Tags"] == []

    async def test_create_sampling_rule_tags_are_retrievable_via_list(self):
        """BUG: Tags passed at creation time must be retrievable via ListTagsForResource.

        Categorical pattern: creation-time tags — many providers accept Tags in
        the Create call but store them separately from the ListTags path.
        """
        create_req = _make_request(
            "POST",
            "/CreateSamplingRule",
            {
                "SamplingRule": {
                    "RuleName": "creation-tags-rule",
                    "Priority": 200,
                    "FixedRate": 0.5,
                    "ReservoirSize": 10,
                    "ServiceName": "*",
                    "ServiceType": "*",
                    "Host": "*",
                    "ResourceARN": "*",
                    "HTTPMethod": "*",
                    "URLPath": "*",
                    "Version": 1,
                },
                "Tags": [
                    {"Key": "created-by", "Value": "automation"},
                    {"Key": "env", "Value": "staging"},
                ],
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        rule_arn = body["SamplingRuleRecord"]["SamplingRule"]["RuleARN"]

        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": rule_arn})
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        tags = json.loads(resp2.body)["Tags"]
        tag_keys = {t["Key"] for t in tags}
        assert tag_keys == {"created-by", "env"}

    async def test_create_group_tags_are_retrievable_via_list(self):
        """Same creation-time tag test for groups."""
        create_req = _make_request(
            "POST",
            "/CreateGroup",
            {
                "GroupName": "tagged-group",
                "FilterExpression": 'service("api")',
                "Tags": [{"Key": "team", "Value": "platform"}],
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        group_arn = body["Group"]["GroupARN"]

        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": group_arn})
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        tags = json.loads(resp2.body)["Tags"]
        assert len(tags) == 1
        assert tags[0]["Key"] == "team"

    async def test_tag_merge_overwrites_existing_value(self):
        """Categorical pattern: TagResource must merge (upsert), not append duplicates."""
        arn = "arn:aws:xray:us-east-1:123456789012:sampling-rule/merge-test"

        # First tag
        tag_req1 = _make_request(
            "POST",
            "/TagResource",
            {"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "dev"}]},
        )
        await handle_xray_request(tag_req1, "us-east-1", "123456789012")

        # Update same key with new value
        tag_req2 = _make_request(
            "POST",
            "/TagResource",
            {"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "prod"}]},
        )
        await handle_xray_request(tag_req2, "us-east-1", "123456789012")

        # Should have exactly 1 tag, not 2
        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": arn})
        resp = await handle_xray_request(list_req, "us-east-1", "123456789012")
        tags = json.loads(resp.body)["Tags"]
        assert len(tags) == 1
        assert tags[0]["Value"] == "prod"

    async def test_delete_group_cleans_up_tags(self):
        """Verify group deletion cascade (this already works, regression guard)."""
        create_req = _make_request(
            "POST",
            "/CreateGroup",
            {
                "GroupName": "cascade-group",
                "Tags": [{"Key": "x", "Value": "y"}],
            },
        )
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        group_arn = json.loads(resp.body)["Group"]["GroupARN"]

        del_req = _make_request("POST", "/DeleteGroup", {"GroupName": "cascade-group"})
        await handle_xray_request(del_req, "us-east-1", "123456789012")

        list_req = _make_request("POST", "/ListTagsForResource", {"ResourceARN": group_arn})
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        assert json.loads(resp2.body)["Tags"] == []

    async def test_encryption_config_is_per_region(self):
        """BUG: Encryption config is a global singleton — should be per-region.

        Categorical pattern: global state that should be per-region or per-account.
        """
        # Set KMS in us-east-1
        put_req = _make_request(
            "POST",
            "/PutEncryptionConfig",
            {"Type": "KMS", "KeyId": "arn:aws:kms:us-east-1:123456789012:key/k1"},
        )
        await handle_xray_request(put_req, "us-east-1", "123456789012")

        # Get in us-west-2 — should be default (NONE), not KMS
        get_req = _make_request("POST", "/EncryptionConfig")
        resp = await handle_xray_request(get_req, "us-west-2", "123456789012")
        body = json.loads(resp.body)
        assert body["EncryptionConfig"]["Type"] == "NONE", (
            "Encryption config leaked across regions — global singleton bug"
        )
