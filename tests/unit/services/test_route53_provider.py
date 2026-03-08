"""Error-path tests for Route53 native provider.

Phase 3A: Covers TestDNSAnswer, CreateQueryLoggingConfig,
and VPC association operations.

Categorical bug tests: silent error swallowing, module-level state leaks,
missing error responses for nonexistent resources.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.route53.provider import (
    _query_log_configs,
    handle_route53_request,
)


def _make_request(method: str, path: str, body: str = "") -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    req.body = AsyncMock(return_value=body.encode() if body else b"")
    return req


@pytest.fixture(autouse=True)
def _clear_query_log_configs():
    """Categorical bug: module-level mutable state leaks between tests."""
    _query_log_configs.clear()
    yield
    _query_log_configs.clear()


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

    async def test_test_dns_answer_xml_content_type(self):
        """Categorical: Route53 responses must have text/xml content type."""
        req = _make_request("GET", "/2013-04-01/testdnsanswer")
        req.query_params = {
            "hostedzoneid": "Z1234567890",
            "recordname": "example.com",
            "recordtype": "A",
        }
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.media_type == "text/xml"

    async def test_test_dns_answer_contains_record_name_in_response(self):
        """Categorical: response must echo back the queried record name."""
        req = _make_request("GET", "/2013-04-01/testdnsanswer")
        req.query_params = {
            "hostedzoneid": "Z1234567890",
            "recordname": "foo.example.com",
            "recordtype": "CNAME",
        }
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert b"<RecordName>foo.example.com</RecordName>" in resp.body
        assert b"<RecordType>CNAME</RecordType>" in resp.body


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

    async def test_create_query_logging_config_populates_store(self):
        """Categorical: native state stores must actually persist data."""
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <CreateQueryLoggingConfigRequest>
            <HostedZoneId>Z1234567890</HostedZoneId>
            <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:test</CloudWatchLogsLogGroupArn>
        </CreateQueryLoggingConfigRequest>"""
        req = _make_request("POST", "/2013-04-01/queryloggingconfig", body)
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        # Verify the config was stored
        assert len(_query_log_configs) == 1
        config = next(iter(_query_log_configs.values()))
        assert config["HostedZoneId"] == "Z1234567890"

    async def test_create_query_logging_config_returns_location_header(self):
        """Categorical: 201 responses must include Location header."""
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <CreateQueryLoggingConfigRequest>
            <HostedZoneId>Z1234567890</HostedZoneId>
            <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:test</CloudWatchLogsLogGroupArn>
        </CreateQueryLoggingConfigRequest>"""
        req = _make_request("POST", "/2013-04-01/queryloggingconfig", body)
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 201
        assert "Location" in resp.headers
        assert "queryloggingconfig/" in resp.headers["Location"]

    async def test_module_state_isolation_between_calls(self):
        """Categorical: module-level state must not leak between independent calls.

        Two CreateQueryLoggingConfig calls should each add one entry.
        Without cleanup, repeated test runs would see stale data.
        """
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <CreateQueryLoggingConfigRequest>
            <HostedZoneId>ZONE1</HostedZoneId>
            <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:g1</CloudWatchLogsLogGroupArn>
        </CreateQueryLoggingConfigRequest>"""
        req = _make_request("POST", "/2013-04-01/queryloggingconfig", body)
        await handle_route53_request(req, "us-east-1", "123456789012")

        body2 = """<?xml version="1.0" encoding="UTF-8"?>
        <CreateQueryLoggingConfigRequest>
            <HostedZoneId>ZONE2</HostedZoneId>
            <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:g2</CloudWatchLogsLogGroupArn>
        </CreateQueryLoggingConfigRequest>"""
        req2 = _make_request("POST", "/2013-04-01/queryloggingconfig", body2)
        await handle_route53_request(req2, "us-east-1", "123456789012")

        assert len(_query_log_configs) == 2
        zone_ids = {c["HostedZoneId"] for c in _query_log_configs.values()}
        assert zone_ids == {"ZONE1", "ZONE2"}


@pytest.mark.asyncio
class TestVPCAssociation:
    async def test_associate_vpc_nonexistent_zone_returns_404(self):
        """Categorical: operations on nonexistent resources must return error, not 200.

        AWS Route53 returns 404 NoSuchHostedZone when you try to associate
        a VPC with a zone that doesn't exist. Silently returning 200 masks
        real client bugs.
        """
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
        # Must return 404, not 200 — nonexistent zone is an error
        assert resp.status_code == 404
        assert b"NoSuchHostedZone" in resp.body

    async def test_disassociate_vpc_nonexistent_zone_returns_404(self):
        """Categorical: disassociate on nonexistent zone must return 404."""
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <DisassociateVPCFromHostedZoneRequest>
            <VPC>
                <VPCRegion>us-east-1</VPCRegion>
                <VPCId>vpc-12345</VPCId>
            </VPC>
        </DisassociateVPCFromHostedZoneRequest>"""
        req = _make_request(
            "POST",
            "/2013-04-01/hostedzone/ZNONEXISTENT/disassociatevpc",
            body,
        )
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404
        assert b"NoSuchHostedZone" in resp.body

    async def test_associate_vpc_error_response_is_xml(self):
        """Categorical: error responses must use XML format for REST-XML services."""
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
        assert resp.media_type == "text/xml"
        # Error response must have proper XML structure
        assert b"<ErrorResponse" in resp.body or b"<Error>" in resp.body


@pytest.mark.asyncio
class TestStaticEndpoints:
    """Categorical: static/stub endpoints must return well-formed XML."""

    async def test_checker_ip_ranges_xml_structure(self):
        req = _make_request("GET", "/2013-04-01/checkeripranges")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert resp.media_type == "text/xml"
        assert b"<CheckerIpRanges>" in resp.body
        assert b"<member>" in resp.body

    async def test_get_health_check_count_returns_zero(self):
        req = _make_request("GET", "/2013-04-01/healthcheckcount")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<HealthCheckCount>" in resp.body

    async def test_list_geo_locations_has_all_continents(self):
        req = _make_request("GET", "/2013-04-01/geolocations")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = resp.body
        for code in [b"AF", b"AN", b"AS", b"EU", b"NA", b"OC", b"SA"]:
            assert code in body, f"Missing continent {code}"

    async def test_get_geo_location_with_continent(self):
        req = _make_request("GET", "/2013-04-01/geolocation")
        req.query_params = {"continentcode": "EU"}
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<ContinentCode>EU</ContinentCode>" in resp.body
        assert b"<ContinentName>Europe</ContinentName>" in resp.body

    async def test_traffic_policy_instance_count(self):
        req = _make_request("GET", "/2013-04-01/trafficpolicyinstancecount")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<TrafficPolicyInstanceCount>0</TrafficPolicyInstanceCount>" in resp.body

    async def test_list_cidr_collections_empty(self):
        req = _make_request("GET", "/2013-04-01/cidrcollection")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<CidrCollections/>" in resp.body

    async def test_list_traffic_policies_empty(self):
        req = _make_request("GET", "/2013-04-01/trafficpolicies")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<TrafficPolicySummaries/>" in resp.body

    async def test_list_traffic_policy_instances_empty(self):
        req = _make_request("GET", "/2013-04-01/trafficpolicyinstances")
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert b"<TrafficPolicyInstances/>" in resp.body
