"""Failing tests for bugs found in Route53 native provider.

Each test documents a specific correctness bug. All tests should FAIL
against the current provider implementation.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import xmltodict

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
class TestDisassociateVPCBug:
    """Bug: DisassociateVPCFromHostedZone doesn't actually remove the VPC.

    The handler ignores zone_id and never modifies state. After associate + disassociate,
    the VPC is still listed on the zone.
    """

    async def test_disassociate_vpc_actually_removes_vpc(self):
        """After associating and then disassociating a VPC, the zone's vpcs list
        should no longer contain the VPC."""
        zone_id = "Z1234567890"

        # Create a mock Moto zone with vpcs attribute
        mock_zone = MagicMock()
        mock_zone.vpcs = []

        mock_backend = MagicMock()
        mock_backend.get_hosted_zone.return_value = mock_zone

        # get_backend is imported locally inside _handle_associate_vpc and
        # _handle_disassociate_vpc, so we patch at the moto.backends level
        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value = {"123456789012": {"global": mock_backend}}

            # Step 1: Associate VPC
            assoc_body = """<?xml version="1.0" encoding="UTF-8"?>
            <AssociateVPCWithHostedZoneRequest>
                <VPC>
                    <VPCRegion>us-east-1</VPCRegion>
                    <VPCId>vpc-12345</VPCId>
                </VPC>
            </AssociateVPCWithHostedZoneRequest>"""
            req = _make_request(
                "POST",
                f"/2013-04-01/hostedzone/{zone_id}/associatevpc",
                assoc_body,
            )
            resp = await handle_route53_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 200
            assert len(mock_zone.vpcs) == 1  # VPC was added

            # Step 2: Disassociate VPC
            disassoc_body = """<?xml version="1.0" encoding="UTF-8"?>
            <DisassociateVPCFromHostedZoneRequest>
                <VPC>
                    <VPCRegion>us-east-1</VPCRegion>
                    <VPCId>vpc-12345</VPCId>
                </VPC>
            </DisassociateVPCFromHostedZoneRequest>"""
            req = _make_request(
                "POST",
                f"/2013-04-01/hostedzone/{zone_id}/disassociatevpc",
                disassoc_body,
            )
            resp = await handle_route53_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 200

            # Bug: VPC should have been removed but it's still there
            assert len(mock_zone.vpcs) == 0, (
                "DisassociateVPC should remove the VPC from the zone, "
                f"but vpcs list still contains: {mock_zone.vpcs}"
            )


@pytest.mark.asyncio
class TestTestDNSAnswerXMLBug:
    """Bug: TestDNSAnswer produces nested <RecordData> inside <RecordData>.

    AWS returns individual record values inside the RecordData container.
    The provider wraps <RecordData>value</RecordData> elements inside another
    <RecordData> container, producing invalid nesting.
    """

    async def test_record_data_not_nested(self):
        """RecordData should contain individual record data entries, not
        nested RecordData elements."""
        req = _make_request("GET", "/2013-04-01/testdnsanswer")
        req.query_params = {
            "hostedzoneid": "Z1234567890",
            "recordname": "example.com",
            "recordtype": "A",
        }
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        body = resp.body.decode("utf-8")
        parsed = xmltodict.parse(body)
        test_resp = parsed["TestDNSAnswerResponse"]
        record_data = test_resp["RecordData"]

        # RecordData should contain record values directly (as text or list),
        # NOT nested <RecordData> child elements. If record_data is a dict
        # containing a "RecordData" key, the XML has invalid nesting.
        assert "RecordData" not in record_data, (
            "RecordData contains nested RecordData elements. "
            "AWS uses flat RecordData with individual values, not nested elements."
        )


@pytest.mark.asyncio
class TestGetGeoLocationNameBug:
    """Bug: GetGeoLocation echoes continent/country codes as names.

    E.g., ContinentCode=NA produces ContinentName=NA instead of
    ContinentName=North America.
    """

    async def test_continent_name_is_not_continent_code(self):
        """ContinentName should be a human-readable name, not the code."""
        req = _make_request("GET", "/2013-04-01/geolocation")
        req.query_params = {"continentcode": "NA"}
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        parsed = xmltodict.parse(resp.body.decode("utf-8"))
        details = parsed["GetGeoLocationResponse"]["GeoLocationDetails"]
        continent_name = details.get("ContinentName", "")

        # The name should be "North America", not the code "NA"
        assert continent_name != "NA", (
            f"ContinentName is '{continent_name}' which is just the code echoed back. "
            "AWS returns the full name 'North America'."
        )
        assert continent_name == "North America"

    async def test_country_name_is_not_country_code(self):
        """CountryName should be a human-readable name, not the code."""
        req = _make_request("GET", "/2013-04-01/geolocation")
        req.query_params = {"countrycode": "US"}
        resp = await handle_route53_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

        parsed = xmltodict.parse(resp.body.decode("utf-8"))
        details = parsed["GetGeoLocationResponse"]["GeoLocationDetails"]
        country_name = details.get("CountryName", "")

        assert country_name != "US", (
            f"CountryName is '{country_name}' which is just the code echoed back. "
            "AWS returns the full name 'United States'."
        )
        assert country_name == "United States"
