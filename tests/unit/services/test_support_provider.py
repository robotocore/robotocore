"""Error-path tests for Support native provider.

Phase 3A: Covers DescribeServices, DescribeSeverityLevels,
DescribeTrustedAdvisorCheckResult, and communication operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.support.provider import handle_support_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"AWSSupport_20130415.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestDescribeServices:
    async def test_returns_service_list(self):
        req = _make_request("DescribeServices")
        resp = await handle_support_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "services" in body
        assert len(body["services"]) > 0
        # Each service should have code and name
        for svc in body["services"]:
            assert "code" in svc
            assert "name" in svc


@pytest.mark.asyncio
class TestDescribeSeverityLevels:
    async def test_returns_severity_levels(self):
        req = _make_request("DescribeSeverityLevels")
        resp = await handle_support_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "severityLevels" in body
        assert len(body["severityLevels"]) >= 4


@pytest.mark.asyncio
class TestTrustedAdvisor:
    async def test_check_result(self):
        req = _make_request("DescribeTrustedAdvisorCheckResult", {
            "checkId": "some-check-id",
        })
        resp = await handle_support_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "result" in body

    async def test_check_summaries(self):
        req = _make_request("DescribeTrustedAdvisorCheckSummaries", {
            "checkIds": ["check-1", "check-2"],
        })
        resp = await handle_support_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "summaries" in body
        assert len(body["summaries"]) == 2


@pytest.mark.asyncio
class TestCommunications:
    async def test_add_communication_to_case(self):
        req = _make_request("AddCommunicationToCase", {
            "caseId": "case-123",
            "communicationBody": "Test message",
        })
        resp = await handle_support_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body.get("result") is True

    async def test_describe_communications(self):
        # Add a communication first
        add_req = _make_request("AddCommunicationToCase", {
            "caseId": "case-456",
            "communicationBody": "First message",
        })
        await handle_support_request(add_req, "us-east-1", "123456789012")

        # Now describe
        desc_req = _make_request("DescribeCommunications", {
            "caseId": "case-456",
        })
        resp = await handle_support_request(desc_req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "communications" in body
