"""Native Route53 provider.

Intercepts operations that Moto has bugs or doesn't implement:
- CreateHostedZone with PrivateZone but no Comment (Moto KeyError bug)
- TestDNSAnswer: Not implemented in Moto
- CreateQueryLoggingConfig: Moto cross-service validation too strict
- AssociateVPCWithHostedZone / DisassociateVPCFromHostedZone
- GetCheckerIpRanges, GetGeoLocation, GetHealthCheckCount,
  GetTrafficPolicyInstanceCount, ListCidrCollections, ListGeoLocations,
  ListTrafficPolicies, ListTrafficPolicyInstances: Missing from Moto's
  flask_paths routing table
"""

import logging
import re
import uuid

import xmltodict
from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_TEST_DNS_RE = re.compile(r"^/2013-04-01/testdnsanswer$")
_QUERY_LOG_RE = re.compile(r"^/2013-04-01/queryloggingconfig$")
_HOSTEDZONE_RE = re.compile(r"^/2013-04-01/hostedzone$")
_ASSOCIATE_VPC_RE = re.compile(r"^/2013-04-01/hostedzone/([^/]+)/associatevpc$")
_DISASSOCIATE_VPC_RE = re.compile(r"^/2013-04-01/hostedzone/([^/]+)/disassociatevpc$")

# Route53 operations missing from Moto's flask_paths
_CHECKER_IP_RE = re.compile(r"^/2013-04-01/checkeripranges$")
_GEO_LOCATION_RE = re.compile(r"^/2013-04-01/geolocation$")
_HEALTH_CHECK_COUNT_RE = re.compile(r"^/2013-04-01/healthcheckcount$")
_TRAFFIC_POLICY_INSTANCE_COUNT_RE = re.compile(r"^/2013-04-01/trafficpolicyinstancecount$")
_CIDR_COLLECTIONS_RE = re.compile(r"^/2013-04-01/cidrcollection$")
_GEO_LOCATIONS_RE = re.compile(r"^/2013-04-01/geolocations$")
_TRAFFIC_POLICIES_RE = re.compile(r"^/2013-04-01/trafficpolicies$")
_TRAFFIC_POLICY_INSTANCES_RE = re.compile(r"^/2013-04-01/trafficpolicyinstances$")

# Store query logging configs
_query_log_configs: dict[str, dict] = {}


logger = logging.getLogger(__name__)


async def handle_route53_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Route53 requests, intercepting buggy operations."""
    path = request.url.path

    # TestDNSAnswer (GET)
    if _TEST_DNS_RE.match(path) and request.method == "GET":
        return _handle_test_dns_answer(request, region, account_id)

    # CreateQueryLoggingConfig (POST)
    if _QUERY_LOG_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _handle_create_query_logging_config(body, region, account_id)

    # CreateHostedZone (POST) — fix Moto's Comment KeyError
    if _HOSTEDZONE_RE.match(path) and request.method == "POST":
        body = await request.body()
        body_str = body.decode("utf-8")
        if "HostedZoneConfig" in body_str and "Comment" not in body_str:
            # Inject an empty Comment to work around Moto's bug
            body_str = body_str.replace(
                "<HostedZoneConfig>", "<HostedZoneConfig><Comment></Comment>"
            ).replace("</HostedZoneConfig>", "</HostedZoneConfig>")
            # Only inject if not already present (double-check)
            if body_str.count("<Comment>") > 1:
                body_str = body.decode("utf-8").replace(
                    "<HostedZoneConfig>", "<HostedZoneConfig><Comment/>"
                )

            # Create modified request with fixed body
            from robotocore.providers.moto_bridge import forward_to_moto_with_body

            return await forward_to_moto_with_body(
                request, "route53", body_str.encode("utf-8"), account_id=account_id
            )

    # AssociateVPCWithHostedZone
    m = _ASSOCIATE_VPC_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        body = await request.body()
        return _handle_associate_vpc(zone_id, body, region, account_id)

    # DisassociateVPCFromHostedZone
    m = _DISASSOCIATE_VPC_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        body = await request.body()
        return _handle_disassociate_vpc(zone_id, body, region, account_id)

    # GetCheckerIpRanges (GET /2013-04-01/checkeripranges)
    if _CHECKER_IP_RE.match(path) and request.method == "GET":
        return _handle_checker_ip_ranges()

    # GetGeoLocation (GET /2013-04-01/geolocation)
    if _GEO_LOCATION_RE.match(path) and request.method == "GET":
        return _handle_get_geo_location(request)

    # GetHealthCheckCount (GET /2013-04-01/healthcheckcount)
    if _HEALTH_CHECK_COUNT_RE.match(path) and request.method == "GET":
        return _handle_get_health_check_count(account_id)

    # GetTrafficPolicyInstanceCount (GET /2013-04-01/trafficpolicyinstancecount)
    if _TRAFFIC_POLICY_INSTANCE_COUNT_RE.match(path) and request.method == "GET":
        return _handle_get_traffic_policy_instance_count()

    # ListCidrCollections (GET /2013-04-01/cidrcollection)
    if _CIDR_COLLECTIONS_RE.match(path) and request.method == "GET":
        return _handle_list_cidr_collections()

    # ListGeoLocations (GET /2013-04-01/geolocations)
    if _GEO_LOCATIONS_RE.match(path) and request.method == "GET":
        return _handle_list_geo_locations()

    # ListTrafficPolicies (GET /2013-04-01/trafficpolicies)
    if _TRAFFIC_POLICIES_RE.match(path) and request.method == "GET":
        return _handle_list_traffic_policies()

    # ListTrafficPolicyInstances (GET /2013-04-01/trafficpolicyinstances)
    if _TRAFFIC_POLICY_INSTANCES_RE.match(path) and request.method == "GET":
        return _handle_list_traffic_policy_instances()

    return await forward_to_moto(request, "route53", account_id=account_id)


def _handle_checker_ip_ranges() -> Response:
    """GetCheckerIpRanges — return AWS health checker IP ranges."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<GetCheckerIpRangesResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CheckerIpRanges>
    <member>54.183.255.128/26</member>
    <member>54.228.16.0/26</member>
    <member>176.34.159.192/26</member>
    <member>54.232.40.64/26</member>
  </CheckerIpRanges>
</GetCheckerIpRangesResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


_CONTINENT_NAMES = {
    "AF": "Africa",
    "AN": "Antarctica",
    "AS": "Asia",
    "EU": "Europe",
    "NA": "North America",
    "OC": "Oceania",
    "SA": "South America",
}

_COUNTRY_NAMES = {
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "JP": "Japan",
    "AU": "Australia",
    "BR": "Brazil",
    "IN": "India",
    "CN": "China",
    "MX": "Mexico",
    "IT": "Italy",
    "ES": "Spain",
    "KR": "Republic of Korea",
    "NL": "Netherlands",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "IE": "Ireland",
    "NZ": "New Zealand",
    "SG": "Singapore",
}


def _handle_get_geo_location(request: Request) -> Response:
    """GetGeoLocation — return geo location details."""
    continent = request.query_params.get("continentcode", "")
    country = request.query_params.get("countrycode", "")
    subdivision = request.query_params.get("subdivisioncode", "")

    parts = []
    if continent:
        continent_name = _CONTINENT_NAMES.get(continent, continent)
        parts.append(f"    <ContinentCode>{continent}</ContinentCode>")
        parts.append(f"    <ContinentName>{continent_name}</ContinentName>")
    if country:
        country_name = _COUNTRY_NAMES.get(country, country)
        parts.append(f"    <CountryCode>{country}</CountryCode>")
        parts.append(f"    <CountryName>{country_name}</CountryName>")
    if subdivision:
        parts.append(f"    <SubdivisionCode>{subdivision}</SubdivisionCode>")
        parts.append(f"    <SubdivisionName>{subdivision}</SubdivisionName>")

    details = "\n".join(parts)
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<GetGeoLocationResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <GeoLocationDetails>
{details}
  </GeoLocationDetails>
</GetGeoLocationResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_get_health_check_count(account_id: str) -> Response:
    """GetHealthCheckCount — return count of health checks."""
    count = 0
    try:
        from moto.backends import get_backend

        backend = get_backend("route53")[account_id]["global"]
        count = len(backend.health_checks.values())
    except Exception as exc:
        logger.debug("_handle_get_health_check_count: len failed (non-fatal): %s", exc)

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<GetHealthCheckCountResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HealthCheckCount>{count}</HealthCheckCount>
</GetHealthCheckCountResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_get_traffic_policy_instance_count() -> Response:
    """GetTrafficPolicyInstanceCount — return count (always 0, not implemented)."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<GetTrafficPolicyInstanceCountResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstanceCount>0</TrafficPolicyInstanceCount>
</GetTrafficPolicyInstanceCountResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_cidr_collections() -> Response:
    """ListCidrCollections — return empty list."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<ListCidrCollectionsResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <CidrCollections/>
</ListCidrCollectionsResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_geo_locations() -> Response:
    """ListGeoLocations — return common geo locations."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<ListGeoLocationsResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <GeoLocationDetailsList>
    <GeoLocationDetails>
      <ContinentCode>AF</ContinentCode>
      <ContinentName>Africa</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>AN</ContinentCode>
      <ContinentName>Antarctica</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>AS</ContinentCode>
      <ContinentName>Asia</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>EU</ContinentCode>
      <ContinentName>Europe</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>NA</ContinentCode>
      <ContinentName>North America</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>OC</ContinentCode>
      <ContinentName>Oceania</ContinentName>
    </GeoLocationDetails>
    <GeoLocationDetails>
      <ContinentCode>SA</ContinentCode>
      <ContinentName>South America</ContinentName>
    </GeoLocationDetails>
  </GeoLocationDetailsList>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
</ListGeoLocationsResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_traffic_policies() -> Response:
    """ListTrafficPolicies — return empty list."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<ListTrafficPoliciesResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicySummaries/>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
</ListTrafficPoliciesResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_traffic_policy_instances() -> Response:
    """ListTrafficPolicyInstances — return empty list."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<ListTrafficPolicyInstancesResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstances/>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
</ListTrafficPolicyInstancesResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_test_dns_answer(request: Request, region: str, account_id: str) -> Response:
    """TestDNSAnswer — return simulated DNS response."""
    record_name = request.query_params.get("recordname", "")
    record_type = request.query_params.get("recordtype", "A")
    hosted_zone_id = request.query_params.get("hostedzoneid", "")

    # Try to look up the actual record
    record_data = []
    try:
        from moto.backends import get_backend

        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(hosted_zone_id)
        if zone:
            for rr_set in zone.rrsets:
                if rr_set.name == record_name + "." or rr_set.name == record_name:
                    if rr_set.type_ == record_type:
                        for record in rr_set.records:
                            val = record.value if hasattr(record, "value") else str(record)
                            record_data.append(val)
    except Exception as exc:
        logger.debug("_handle_test_dns_answer: get_hosted_zone failed (non-fatal): %s", exc)

    if not record_data:
        record_data = ["127.0.0.1"] if record_type == "A" else [record_name]

    records_xml = "".join(f"    <RecordData>{r}</RecordData>\n" for r in record_data)

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<TestDNSAnswerResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Nameserver>ns-1.awsdns-01.com</Nameserver>
  <RecordName>{record_name}</RecordName>
  <RecordType>{record_type}</RecordType>
  <ResponseCode>NOERROR</ResponseCode>
  <Protocol>UDP</Protocol>
{records_xml}</TestDNSAnswerResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_create_query_logging_config(body: bytes, region: str, account_id: str) -> Response:
    """CreateQueryLoggingConfig — skip cross-service validation."""
    parsed = xmltodict.parse(body)
    req = parsed.get("CreateQueryLoggingConfigRequest", {})
    hosted_zone_id = req.get("HostedZoneId", "")
    log_group_arn = req.get("CloudWatchLogsLogGroupArn", "")

    config_id = str(uuid.uuid4())
    config = {
        "Id": config_id,
        "HostedZoneId": hosted_zone_id,
        "CloudWatchLogsLogGroupArn": log_group_arn,
    }
    _query_log_configs[config_id] = config

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <QueryLoggingConfig>
    <Id>{config_id}</Id>
    <HostedZoneId>{hosted_zone_id}</HostedZoneId>
    <CloudWatchLogsLogGroupArn>{log_group_arn}</CloudWatchLogsLogGroupArn>
  </QueryLoggingConfig>
</CreateQueryLoggingConfigResponse>"""
    return Response(
        content=xml,
        status_code=201,
        media_type="text/xml",
        headers={
            "Location": f"https://route53.amazonaws.com/2013-04-01/queryloggingconfig/{config_id}"
        },
    )


def _no_such_hosted_zone_response(zone_id: str) -> Response:
    """Return a 404 NoSuchHostedZone XML error response."""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchHostedZone</Code>
    <Message>No hosted zone found with ID: {zone_id}</Message>
  </Error>
</ErrorResponse>"""
    return Response(content=xml, status_code=404, media_type="text/xml")


def _handle_associate_vpc(zone_id: str, body: bytes, region: str, account_id: str) -> Response:
    """AssociateVPCWithHostedZone — store VPC association."""
    from moto.backends import get_backend

    try:
        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(zone_id)
    except Exception:
        return _no_such_hosted_zone_response(zone_id)

    parsed = xmltodict.parse(body)
    req = parsed.get("AssociateVPCWithHostedZoneRequest", {})
    vpc = req.get("VPC", {})
    vpc_id = vpc.get("VPCId", "")
    vpc_region = vpc.get("VPCRegion", region)
    if not hasattr(zone, "vpcs"):
        zone.vpcs = []
    zone.vpcs.append({"VPCId": vpc_id, "VPCRegion": vpc_region})

    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<AssociateVPCWithHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</AssociateVPCWithHostedZoneResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_disassociate_vpc(zone_id: str, body: bytes, region: str, account_id: str) -> Response:
    """DisassociateVPCFromHostedZone."""
    from moto.backends import get_backend

    try:
        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(zone_id)
    except Exception:
        return _no_such_hosted_zone_response(zone_id)

    if hasattr(zone, "vpcs"):
        parsed = xmltodict.parse(body)
        req = parsed.get("DisassociateVPCFromHostedZoneRequest", {})
        vpc = req.get("VPC", {})
        vpc_id = vpc.get("VPCId", "")
        zone.vpcs = [v for v in zone.vpcs if v.get("VPCId") != vpc_id]

    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DisassociateVPCFromHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</DisassociateVPCFromHostedZoneResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")
