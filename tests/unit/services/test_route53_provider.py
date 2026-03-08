"""Error-path tests for Route53 native provider.

Phase 3A: Covers TestDNSAnswer, CreateQueryLoggingConfig,
and VPC association operations.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.route53.provider import handle_route53_request


def _make_request(method: str, path: str, body: str = "") -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    req.body = AsyncMock(return_value=body.encode() if body else b"")
    return req


@pytest.mark.asyncio
class TestDNSAnswer:
    async def test_test_dns_answer_basic(self):
        req = _make_request("GET", "/2013-04-01/testdnsanswer")
        req.query_params = {
            "hostedzoneid": "Z1234567890",
            "recordname": "example.com",
            "recordtype": "A",
        }
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"TestDNSAnswerResponse" in resp.body

    async def test_test_dns_answer_nonexistent_zone(self):
        req = _make_request("GET", "/2013-04-01/testdnsanswer")
        req.query_params = {
            "hostedzoneid": "ZNONEXISTENT",
            "recordname": "example.com",
            "recordtype": "A",
        }
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        # Should still return 200 with fallback answer
        assert resp.status_code == 200
        assert b"TestDNSAnswerResponse" in resp.body


@pytest.mark.asyncio
class TestQueryLoggingConfig:
    async def test_create_query_logging_config(self):
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <CreateQueryLoggingConfigRequest>
            <HostedZoneId>Z1234567890</HostedZoneId>
            <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:test</CloudWatchLogsLogGroupArn>
        </CreateQueryLoggingConfigRequest>"""
        req = _make_request("POST", "/2013-04-01/queryloggingconfig", body)
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        assert b"QueryLoggingConfig" in resp.body


@pytest.mark.asyncio
class TestVPCAssociation:
    async def test_associate_vpc_nonexistent_zone(self):
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <AssociateVPCWithHostedZoneRequest>
            <VPC>
                <VPCRegion>us-east-1</VPCRegion>
                <VPCId>vpc-12345</VPCId>
            </VPC>
        </AssociateVPCWithHostedZoneRequest>"""
        req = _make_request(
            "POST",
            "/2013-04-01/hostedzone/ZNONEXISTENT/associatevpc",
            body,
        )
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        # Should succeed silently even for nonexistent zone
        assert resp.status_code == 200

    async def test_disassociate_vpc(self):
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <DisassociateVPCFromHostedZoneRequest>
            <VPC>
                <VPCRegion>us-east-1</VPCRegion>
                <VPCId>vpc-12345</VPCId>
            </VPC>
        </DisassociateVPCFromHostedZoneRequest>"""
        req = _make_request(
            "POST",
            "/2013-04-01/hostedzone/Z1234567890/disassociatevpc",
            body,
        )
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"ChangeInfo" in resp.body
