"""Error-path tests for X-Ray native provider.

Phase 3A: Covers sampling rules, groups, encryption config, and tagging.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.xray.provider import handle_xray_request


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
        create_req = _make_request("POST", "/CreateSamplingRule", {
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
        })
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
        req = _make_request("POST", "/DeleteSamplingRule", {
            "RuleName": "nonexistent-rule-xyz",
        })
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        # Should return empty record, not error
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["SamplingRuleRecord"] == {}


@pytest.mark.asyncio
class TestGroups:
    async def test_create_and_get_group(self):
        create_req = _make_request("POST", "/CreateGroup", {
            "GroupName": "test-group",
            "FilterExpression": 'service("test")',
        })
        resp = await handle_xray_request(create_req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "Group" in body
        assert body["Group"]["GroupName"] == "test-group"

        # Get the group
        get_req = _make_request("POST", "/GetGroup", {
            "GroupName": "test-group",
        })
        resp2 = await handle_xray_request(get_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200

    async def test_get_nonexistent_group(self):
        req = _make_request("POST", "/GetGroup", {
            "GroupName": "nonexistent-group-xyz",
        })
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["Group"] == {}

    async def test_delete_nonexistent_group(self):
        req = _make_request("POST", "/DeleteGroup", {
            "GroupName": "nonexistent-group-xyz",
        })
        resp = await handle_xray_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200


@pytest.mark.asyncio
class TestEncryptionConfig:
    async def test_put_and_get_encryption_config(self):
        put_req = _make_request("POST", "/PutEncryptionConfig", {
            "Type": "KMS",
            "KeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
        })
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
        tag_req = _make_request("POST", "/TagResource", {
            "ResourceARN": arn,
            "Tags": [{"Key": "env", "Value": "test"}],
        })
        resp = await handle_xray_request(tag_req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        list_req = _make_request("POST", "/ListTagsForResource", {
            "ResourceARN": arn,
        })
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        body = json.loads(resp2.body)
        assert "Tags" in body
        assert len(body["Tags"]) == 1
        assert body["Tags"][0]["Key"] == "env"

    async def test_untag_resource(self):
        arn = "arn:aws:xray:us-east-1:123456789012:group/test-untag"
        # Tag first
        tag_req = _make_request("POST", "/TagResource", {
            "ResourceARN": arn,
            "Tags": [{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
        })
        await handle_xray_request(tag_req, "us-east-1", "123456789012")

        # Untag k1
        untag_req = _make_request("POST", "/UntagResource", {
            "ResourceARN": arn,
            "TagKeys": ["k1"],
        })
        resp = await handle_xray_request(untag_req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        # Verify only k2 remains
        list_req = _make_request("POST", "/ListTagsForResource", {
            "ResourceARN": arn,
        })
        resp2 = await handle_xray_request(list_req, "us-east-1", "123456789012")
        body = json.loads(resp2.body)
        assert len(body["Tags"]) == 1
        assert body["Tags"][0]["Key"] == "k2"
