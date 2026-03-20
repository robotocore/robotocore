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
- ChangeCidrCollection: Moto uses <Change> tag but boto3 sends <member>
- TrafficPolicy CRUD: Not implemented in Moto
- KeySigningKey CRUD: Not implemented in Moto
- EnableHostedZoneDNSSEC / DisableHostedZoneDNSSEC: Not implemented in Moto
- UpdateHostedZoneFeatures: Not implemented in Moto
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
_HOSTEDZONE_ENABLE_DNSSEC_RE = re.compile(r"^/2013-04-01/hostedzone/([^/]+)/enable-dnssec$")
_HOSTEDZONE_DISABLE_DNSSEC_RE = re.compile(r"^/2013-04-01/hostedzone/([^/]+)/disable-dnssec$")
_HOSTEDZONE_FEATURES_RE = re.compile(r"^/2013-04-01/hostedzone/([^/]+)/features$")

# Route53 operations missing from Moto's flask_paths
_CHECKER_IP_RE = re.compile(r"^/2013-04-01/checkeripranges$")
_GEO_LOCATION_RE = re.compile(r"^/2013-04-01/geolocation$")
_HEALTH_CHECK_COUNT_RE = re.compile(r"^/2013-04-01/healthcheckcount$")
_TRAFFIC_POLICY_INSTANCE_COUNT_RE = re.compile(r"^/2013-04-01/trafficpolicyinstancecount$")
_CIDR_COLLECTIONS_RE = re.compile(r"^/2013-04-01/cidrcollection$")
_CIDR_COLLECTION_RE = re.compile(r"^/2013-04-01/cidrcollection/([^/]+)$")
_GEO_LOCATIONS_RE = re.compile(r"^/2013-04-01/geolocations$")
# TrafficPolicy list: GET /2013-04-01/trafficpolicies (with 's')
_TRAFFIC_POLICIES_RE = re.compile(r"^/2013-04-01/trafficpolicies$")
# CreateTrafficPolicy: POST /2013-04-01/trafficpolicy (no 's', no version)
_TRAFFIC_POLICY_CREATE_RE = re.compile(r"^/2013-04-01/trafficpolicy$")
# CreateTrafficPolicyVersion: POST /2013-04-01/trafficpolicy/{Id} (no version in path)
# UpdateTrafficPolicyComment: POST /2013-04-01/trafficpolicy/{Id}/{Version}
# GetTrafficPolicy: GET /2013-04-01/trafficpolicy/{Id}/{Version}
# DeleteTrafficPolicy: DELETE /2013-04-01/trafficpolicy/{Id}/{Version}
_TRAFFIC_POLICY_ID_RE = re.compile(r"^/2013-04-01/trafficpolicy/([^/]+)$")
_TRAFFIC_POLICY_VERSION_RE = re.compile(r"^/2013-04-01/trafficpolicy/([^/]+)/([0-9]+)$")
# ListTrafficPolicyVersions: GET /2013-04-01/trafficpolicies/{Id}/versions
_TRAFFIC_POLICY_VERSIONS_RE = re.compile(r"^/2013-04-01/trafficpolicies/([^/]+)/versions$")
# TrafficPolicyInstances
_TRAFFIC_POLICY_INSTANCES_RE = re.compile(r"^/2013-04-01/trafficpolicyinstances$")
# CreateTrafficPolicyInstance: POST /2013-04-01/trafficpolicyinstance (singular)
_TRAFFIC_POLICY_INSTANCE_CREATE_RE = re.compile(r"^/2013-04-01/trafficpolicyinstance$")
_TRAFFIC_POLICY_INSTANCE_RE = re.compile(r"^/2013-04-01/trafficpolicyinstance/([^/]+)$")
_TRAFFIC_POLICY_INSTANCES_BY_HZ_RE = re.compile(r"^/2013-04-01/trafficpolicyinstances/hostedzone$")
_TRAFFIC_POLICY_INSTANCES_BY_POLICY_RE = re.compile(
    r"^/2013-04-01/trafficpolicyinstances/trafficpolicy$"
)
# KeySigningKey: POST /2013-04-01/keysigningkey (create)
_KEY_SIGNING_KEY_CREATE_RE = re.compile(r"^/2013-04-01/keysigningkey$")
# ActivateKeySigningKey: POST /2013-04-01/keysigningkey/{HostedZoneId}/{Name}/activate
_KEY_SIGNING_KEY_ACTIVATE_RE = re.compile(r"^/2013-04-01/keysigningkey/([^/]+)/([^/]+)/activate$")
# DeactivateKeySigningKey: POST /2013-04-01/keysigningkey/{HostedZoneId}/{Name}/deactivate
_KEY_SIGNING_KEY_DEACTIVATE_RE = re.compile(
    r"^/2013-04-01/keysigningkey/([^/]+)/([^/]+)/deactivate$"
)
# DeleteKeySigningKey: DELETE /2013-04-01/keysigningkey/{HostedZoneId}/{Name}
_KEY_SIGNING_KEY_RE = re.compile(r"^/2013-04-01/keysigningkey/([^/]+)/([^/]+)$")

# Store query logging configs
_query_log_configs: dict[str, dict] = {}

# In-memory traffic policy store: {policy_id: {version: policy_doc}}
_traffic_policies: dict[str, dict] = {}
# In-memory traffic policy instances: {instance_id: instance_doc}
_traffic_policy_instances: dict[str, dict] = {}
# In-memory key signing keys: {zone_id: {name: ksk_doc}}
_key_signing_keys: dict[str, dict] = {}


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

    # ChangeCidrCollection (POST /2013-04-01/cidrcollection/{id})
    m = _CIDR_COLLECTION_RE.match(path)
    if m and request.method == "POST":
        collection_id = m.group(1)
        body = await request.body()
        return _handle_change_cidr_collection(collection_id, body)

    # TrafficPolicy CRUD
    # CreateTrafficPolicy: POST /2013-04-01/trafficpolicy
    if _TRAFFIC_POLICY_CREATE_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _handle_create_traffic_policy(body)

    # GetTrafficPolicy / DeleteTrafficPolicy / UpdateTrafficPolicyComment:
    # GET|DELETE|POST /2013-04-01/trafficpolicy/{Id}/{Version}
    m = _TRAFFIC_POLICY_VERSION_RE.match(path)
    if m:
        policy_id, version_str = m.group(1), m.group(2)
        if request.method == "GET":
            return _handle_get_traffic_policy(policy_id, int(version_str))
        if request.method == "DELETE":
            return _handle_delete_traffic_policy(policy_id, int(version_str))
        if request.method == "POST":
            body = await request.body()
            return _handle_update_traffic_policy_comment(policy_id, int(version_str), body)

    # CreateTrafficPolicyVersion: POST /2013-04-01/trafficpolicy/{Id}
    m = _TRAFFIC_POLICY_ID_RE.match(path)
    if m and request.method == "POST":
        policy_id = m.group(1)
        body = await request.body()
        return _handle_create_traffic_policy_version(policy_id, body)

    # ListTrafficPolicyVersions: GET /2013-04-01/trafficpolicies/{Id}/versions
    m = _TRAFFIC_POLICY_VERSIONS_RE.match(path)
    if m and request.method == "GET":
        policy_id = m.group(1)
        return _handle_list_traffic_policy_versions(policy_id)

    # TrafficPolicyInstance CRUD
    # CreateTrafficPolicyInstance: POST /2013-04-01/trafficpolicyinstance
    if _TRAFFIC_POLICY_INSTANCE_CREATE_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _handle_create_traffic_policy_instance(body)

    # GetTrafficPolicyInstance / DeleteTrafficPolicyInstance / UpdateTrafficPolicyInstance:
    # GET|DELETE|POST /2013-04-01/trafficpolicyinstance/{Id}
    m = _TRAFFIC_POLICY_INSTANCE_RE.match(path)
    if m:
        instance_id = m.group(1)
        if request.method == "GET":
            return _handle_get_traffic_policy_instance(instance_id)
        if request.method == "DELETE":
            return _handle_delete_traffic_policy_instance(instance_id)
        if request.method == "POST":
            body = await request.body()
            return _handle_update_traffic_policy_instance(instance_id, body)

    if _TRAFFIC_POLICY_INSTANCES_BY_HZ_RE.match(path) and request.method == "GET":
        return _handle_list_traffic_policy_instances_by_hosted_zone(request)

    if _TRAFFIC_POLICY_INSTANCES_BY_POLICY_RE.match(path) and request.method == "GET":
        return _handle_list_traffic_policy_instances_by_policy(request)

    # KeySigningKey CRUD
    # CreateKeySigningKey: POST /2013-04-01/keysigningkey
    if _KEY_SIGNING_KEY_CREATE_RE.match(path) and request.method == "POST":
        body = await request.body()
        return _handle_create_key_signing_key(body)

    m = _KEY_SIGNING_KEY_ACTIVATE_RE.match(path)
    if m and request.method == "POST":
        zone_id, ksk_name = m.group(1), m.group(2)
        return _handle_activate_key_signing_key(zone_id, ksk_name)

    m = _KEY_SIGNING_KEY_DEACTIVATE_RE.match(path)
    if m and request.method == "POST":
        zone_id, ksk_name = m.group(1), m.group(2)
        return _handle_deactivate_key_signing_key(zone_id, ksk_name)

    m = _KEY_SIGNING_KEY_RE.match(path)
    if m and request.method == "DELETE":
        zone_id, ksk_name = m.group(1), m.group(2)
        return _handle_delete_key_signing_key(zone_id, ksk_name)

    # EnableHostedZoneDNSSEC: POST /2013-04-01/hostedzone/{Id}/enable-dnssec
    m = _HOSTEDZONE_ENABLE_DNSSEC_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        return _handle_enable_hosted_zone_dnssec(zone_id)

    # DisableHostedZoneDNSSEC: POST /2013-04-01/hostedzone/{Id}/disable-dnssec
    m = _HOSTEDZONE_DISABLE_DNSSEC_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        return _handle_disable_hosted_zone_dnssec(zone_id)

    # UpdateHostedZoneFeatures
    m = _HOSTEDZONE_FEATURES_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        body = await request.body()
        return _handle_update_hosted_zone_features(zone_id, body)

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
        from moto.backends import get_backend  # noqa: I001

        backend = get_backend("route53")[account_id]["global"]
        count = len(backend.health_checks.values())
    except Exception as exc:  # noqa: BLE001
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
  <TrafficPolicyIdMarker></TrafficPolicyIdMarker>
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
        from moto.backends import get_backend  # noqa: I001

        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(hosted_zone_id)
        if zone:
            for rr_set in zone.rrsets:
                if rr_set.name == record_name + "." or rr_set.name == record_name:
                    if rr_set.type_ == record_type:
                        for record in rr_set.records:
                            val = record.value if hasattr(record, "value") else str(record)
                            record_data.append(val)
    except Exception as exc:  # noqa: BLE001
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
    from moto.backends import get_backend  # noqa: I001

    try:
        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(zone_id)
    except Exception:  # noqa: BLE001
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
    from moto.backends import get_backend  # noqa: I001

    try:
        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(zone_id)
    except Exception:  # noqa: BLE001
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


def _handle_change_cidr_collection(collection_id: str, body: bytes) -> Response:
    """ChangeCidrCollection — fix boto3 sending <member> instead of <Change>."""
    # boto3 serializes list items as <member>; Moto expects <Change>
    # Normalize by rewriting <member> to <Change> in Changes element
    body_str = body.decode("utf-8")
    body_str = (
        body_str.replace("<Changes><member>", "<Changes><Change>")
        .replace("</member></Changes>", "</Change></Changes>")
        .replace("</member><member>", "</Change><Change>")
    )
    # Forward to Moto with fixed body
    import xmltodict as _xmltodict
    from moto.backends import get_backend  # noqa: I001

    parsed = _xmltodict.parse(body_str)
    req = parsed.get("ChangeCidrCollectionRequest", {})
    changes_elem = req.get("Changes", {})
    changes = changes_elem.get("Change", [])
    if not isinstance(changes, list):
        changes = [changes]

    try:
        backend = get_backend("route53")["123456789012"]["global"]
        # Try any available account
        from moto.backends import get_backend as _gb

        for acct in ["123456789012", "000000000000"]:
            try:
                backend = _gb("route53")[acct]["global"]
                if collection_id in backend.cidr_collections:
                    break
            except Exception:  # noqa: BLE001
                logger.debug(
                    "_handle_change_cidr_collection: backend lookup failed for acct %s", acct
                )
        version = backend.change_cidr_collection(collection_id, changes)
    except Exception as exc:
        logger.debug("_handle_change_cidr_collection: error: %s", exc)
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchCidrCollectionException</Code>
    <Message>The CIDR collection with ID {collection_id} does not exist.</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ChangeCidrCollectionResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Id>{version}</Id>
</ChangeCidrCollectionResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


# ---------------------------------------------------------------------------
# TrafficPolicy CRUD
# ---------------------------------------------------------------------------


def _handle_create_traffic_policy(body: bytes) -> Response:
    """CreateTrafficPolicy — in-memory implementation."""
    parsed = xmltodict.parse(body)
    req = parsed.get("CreateTrafficPolicyRequest", {})
    name = req.get("Name", "")
    document = req.get("Document", "{}")
    comment = req.get("Comment", "")
    policy_id = str(uuid.uuid4())
    policy = {
        "Id": policy_id,
        "Name": name,
        "Version": 1,
        "Document": document,
        "Comment": comment,
        "Type": "A",
    }
    _traffic_policies[policy_id] = {1: policy}

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicy>
    <Id>{policy_id}</Id>
    <Name>{name}</Name>
    <Version>1</Version>
    <Document>{document}</Document>
    <Comment>{comment}</Comment>
  </TrafficPolicy>
  <Location>https://route53.amazonaws.com/2013-04-01/trafficpolicy/{policy_id}/1</Location>
</CreateTrafficPolicyResponse>"""
    return Response(
        content=xml,
        status_code=201,
        media_type="text/xml",
        headers={
            "Location": f"https://route53.amazonaws.com/2013-04-01/trafficpolicy/{policy_id}/1"
        },
    )


def _handle_get_traffic_policy(policy_id: str, version: int) -> Response:
    """GetTrafficPolicy — in-memory implementation."""
    versions = _traffic_policies.get(policy_id)
    if not versions or version not in versions:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicy</Code>
    <Message>No traffic policy found with ID {policy_id} version {version}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    p = versions[version]
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<GetTrafficPolicyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicy>
    <Id>{p["Id"]}</Id>
    <Name>{p["Name"]}</Name>
    <Version>{p["Version"]}</Version>
    <Document>{p["Document"]}</Document>
    <Comment>{p["Comment"]}</Comment>
  </TrafficPolicy>
</GetTrafficPolicyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_delete_traffic_policy(policy_id: str, version: int) -> Response:
    """DeleteTrafficPolicy — in-memory implementation."""
    versions = _traffic_policies.get(policy_id)
    if not versions or version not in versions:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicy</Code>
    <Message>No traffic policy found with ID {policy_id} version {version}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    del versions[version]
    if not versions:
        del _traffic_policies[policy_id]

    return Response(
        content='<?xml version="1.0" encoding="UTF-8"?>'
        '<DeleteTrafficPolicyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/"/>',
        status_code=200,
        media_type="text/xml",
    )


def _handle_update_traffic_policy_comment(policy_id: str, version: int, body: bytes) -> Response:
    """UpdateTrafficPolicyComment — in-memory implementation."""
    versions = _traffic_policies.get(policy_id)
    if not versions or version not in versions:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicy</Code>
    <Message>No traffic policy found with ID {policy_id} version {version}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    parsed = xmltodict.parse(body)
    req = parsed.get("UpdateTrafficPolicyCommentRequest", {})
    comment = req.get("Comment", "")
    versions[version]["Comment"] = comment
    p = versions[version]

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<UpdateTrafficPolicyCommentResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicy>
    <Id>{p["Id"]}</Id>
    <Name>{p["Name"]}</Name>
    <Version>{p["Version"]}</Version>
    <Document>{p["Document"]}</Document>
    <Comment>{comment}</Comment>
  </TrafficPolicy>
</UpdateTrafficPolicyCommentResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_create_traffic_policy_version(policy_id: str, body: bytes) -> Response:
    """CreateTrafficPolicyVersion — in-memory implementation."""
    versions = _traffic_policies.get(policy_id)
    if not versions:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicy</Code>
    <Message>No traffic policy found with ID {policy_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    parsed = xmltodict.parse(body)
    req = parsed.get("CreateTrafficPolicyVersionRequest", {})
    document = req.get("Document", "{}")
    comment = req.get("Comment", "")
    new_version = max(versions.keys()) + 1
    base = list(versions.values())[0]
    new_policy = {
        "Id": policy_id,
        "Name": base["Name"],
        "Version": new_version,
        "Document": document,
        "Comment": comment,
        "Type": base.get("Type", "A"),
    }
    versions[new_version] = new_policy

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicy>
    <Id>{policy_id}</Id>
    <Name>{new_policy["Name"]}</Name>
    <Version>{new_version}</Version>
    <Document>{document}</Document>
    <Comment>{comment}</Comment>
  </TrafficPolicy>
  <Location>https://route53.amazonaws.com/2013-04-01/trafficpolicy/{policy_id}/{new_version}</Location>
</CreateTrafficPolicyVersionResponse>"""
    return Response(
        content=xml,
        status_code=201,
        media_type="text/xml",
        headers={
            "Location": f"https://route53.amazonaws.com/2013-04-01/trafficpolicy/{policy_id}/{new_version}"
        },
    )


def _handle_list_traffic_policy_versions(policy_id: str) -> Response:
    """ListTrafficPolicyVersions — in-memory implementation."""
    versions = _traffic_policies.get(policy_id)
    if versions is None:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicy</Code>
    <Message>No traffic policy found with ID {policy_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    policies_xml = ""
    for v, p in sorted(versions.items()):
        policies_xml += f"""  <TrafficPolicy>
    <Id>{p["Id"]}</Id>
    <Name>{p["Name"]}</Name>
    <Version>{v}</Version>
    <Document>{p["Document"]}</Document>
    <Comment>{p["Comment"]}</Comment>
  </TrafficPolicy>
"""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListTrafficPolicyVersionsResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicies>
{policies_xml}  </TrafficPolicies>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
  <TrafficPolicyVersionMarker></TrafficPolicyVersionMarker>
</ListTrafficPolicyVersionsResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


# ---------------------------------------------------------------------------
# TrafficPolicyInstance CRUD
# ---------------------------------------------------------------------------


def _handle_create_traffic_policy_instance(body: bytes) -> Response:
    """CreateTrafficPolicyInstance — in-memory implementation."""
    parsed = xmltodict.parse(body)
    req = parsed.get("CreateTrafficPolicyInstanceRequest", {})
    hosted_zone_id = req.get("HostedZoneId", "")
    name = req.get("Name", "")
    ttl = req.get("TTL", "60")
    policy_id = req.get("TrafficPolicyId", "")
    policy_version = int(req.get("TrafficPolicyVersion", "1"))

    instance_id = str(uuid.uuid4())
    instance = {
        "Id": instance_id,
        "HostedZoneId": hosted_zone_id,
        "Name": name,
        "TTL": str(ttl),
        "TrafficPolicyId": policy_id,
        "TrafficPolicyVersion": str(policy_version),
        "TrafficPolicyType": "A",
        "State": "Applied",
        "Message": "",
    }
    _traffic_policy_instances[instance_id] = instance

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstance>
    <Id>{instance_id}</Id>
    <HostedZoneId>{hosted_zone_id}</HostedZoneId>
    <Name>{name}</Name>
    <TTL>{ttl}</TTL>
    <TrafficPolicyId>{policy_id}</TrafficPolicyId>
    <TrafficPolicyVersion>{policy_version}</TrafficPolicyVersion>
    <TrafficPolicyType>A</TrafficPolicyType>
    <State>Applied</State>
    <Message></Message>
  </TrafficPolicyInstance>
  <Location>https://route53.amazonaws.com/2013-04-01/trafficpolicyinstance/{instance_id}</Location>
</CreateTrafficPolicyInstanceResponse>"""
    return Response(
        content=xml,
        status_code=201,
        media_type="text/xml",
        headers={
            "Location": f"https://route53.amazonaws.com/2013-04-01/trafficpolicyinstance/{instance_id}"
        },
    )


def _handle_get_traffic_policy_instance(instance_id: str) -> Response:
    """GetTrafficPolicyInstance — in-memory implementation."""
    inst = _traffic_policy_instances.get(instance_id)
    if not inst:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicyInstance</Code>
    <Message>No traffic policy instance found with ID {instance_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<GetTrafficPolicyInstanceResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstance>
    <Id>{inst["Id"]}</Id>
    <HostedZoneId>{inst["HostedZoneId"]}</HostedZoneId>
    <Name>{inst["Name"]}</Name>
    <TTL>{inst["TTL"]}</TTL>
    <TrafficPolicyId>{inst["TrafficPolicyId"]}</TrafficPolicyId>
    <TrafficPolicyVersion>{inst["TrafficPolicyVersion"]}</TrafficPolicyVersion>
    <TrafficPolicyType>{inst["TrafficPolicyType"]}</TrafficPolicyType>
    <State>{inst["State"]}</State>
    <Message>{inst["Message"]}</Message>
  </TrafficPolicyInstance>
</GetTrafficPolicyInstanceResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_delete_traffic_policy_instance(instance_id: str) -> Response:
    """DeleteTrafficPolicyInstance — in-memory implementation."""
    if instance_id not in _traffic_policy_instances:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicyInstance</Code>
    <Message>No traffic policy instance found with ID {instance_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    del _traffic_policy_instances[instance_id]
    return Response(
        content='<?xml version="1.0" encoding="UTF-8"?>'
        '<DeleteTrafficPolicyInstanceResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/"/>',
        status_code=200,
        media_type="text/xml",
    )


def _handle_update_traffic_policy_instance(instance_id: str, body: bytes) -> Response:
    """UpdateTrafficPolicyInstance — in-memory implementation."""
    if instance_id not in _traffic_policy_instances:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchTrafficPolicyInstance</Code>
    <Message>No traffic policy instance found with ID {instance_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    parsed = xmltodict.parse(body)
    req = parsed.get("UpdateTrafficPolicyInstanceRequest", {})
    inst = _traffic_policy_instances[instance_id]
    if "TTL" in req:
        inst["TTL"] = str(req["TTL"])
    if "TrafficPolicyId" in req:
        inst["TrafficPolicyId"] = req["TrafficPolicyId"]
    if "TrafficPolicyVersion" in req:
        inst["TrafficPolicyVersion"] = str(req["TrafficPolicyVersion"])

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<UpdateTrafficPolicyInstanceResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstance>
    <Id>{inst["Id"]}</Id>
    <HostedZoneId>{inst["HostedZoneId"]}</HostedZoneId>
    <Name>{inst["Name"]}</Name>
    <TTL>{inst["TTL"]}</TTL>
    <TrafficPolicyId>{inst["TrafficPolicyId"]}</TrafficPolicyId>
    <TrafficPolicyVersion>{inst["TrafficPolicyVersion"]}</TrafficPolicyVersion>
    <TrafficPolicyType>{inst["TrafficPolicyType"]}</TrafficPolicyType>
    <State>{inst["State"]}</State>
    <Message>{inst["Message"]}</Message>
  </TrafficPolicyInstance>
</UpdateTrafficPolicyInstanceResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_traffic_policy_instances_by_hosted_zone(request: Request) -> Response:
    """ListTrafficPolicyInstancesByHostedZone — in-memory implementation."""
    hz_id = request.query_params.get("id", "")
    instances = [
        inst for inst in _traffic_policy_instances.values() if inst["HostedZoneId"] == hz_id
    ]

    instances_xml = ""
    for inst in instances:
        instances_xml += f"""  <TrafficPolicyInstance>
    <Id>{inst["Id"]}</Id>
    <HostedZoneId>{inst["HostedZoneId"]}</HostedZoneId>
    <Name>{inst["Name"]}</Name>
    <TTL>{inst["TTL"]}</TTL>
    <TrafficPolicyId>{inst["TrafficPolicyId"]}</TrafficPolicyId>
    <TrafficPolicyVersion>{inst["TrafficPolicyVersion"]}</TrafficPolicyVersion>
    <TrafficPolicyType>{inst["TrafficPolicyType"]}</TrafficPolicyType>
    <State>{inst["State"]}</State>
    <Message>{inst["Message"]}</Message>
  </TrafficPolicyInstance>
"""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListTrafficPolicyInstancesByHostedZoneResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstances>
{instances_xml}  </TrafficPolicyInstances>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
</ListTrafficPolicyInstancesByHostedZoneResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_list_traffic_policy_instances_by_policy(request: Request) -> Response:
    """ListTrafficPolicyInstancesByPolicy — in-memory implementation."""
    policy_id = request.query_params.get("id", "")
    policy_version = request.query_params.get("version", "")
    instances = [
        inst
        for inst in _traffic_policy_instances.values()
        if inst["TrafficPolicyId"] == policy_id
        and (not policy_version or inst["TrafficPolicyVersion"] == policy_version)
    ]

    instances_xml = ""
    for inst in instances:
        instances_xml += f"""  <TrafficPolicyInstance>
    <Id>{inst["Id"]}</Id>
    <HostedZoneId>{inst["HostedZoneId"]}</HostedZoneId>
    <Name>{inst["Name"]}</Name>
    <TTL>{inst["TTL"]}</TTL>
    <TrafficPolicyId>{inst["TrafficPolicyId"]}</TrafficPolicyId>
    <TrafficPolicyVersion>{inst["TrafficPolicyVersion"]}</TrafficPolicyVersion>
    <TrafficPolicyType>{inst["TrafficPolicyType"]}</TrafficPolicyType>
    <State>{inst["State"]}</State>
    <Message>{inst["Message"]}</Message>
  </TrafficPolicyInstance>
"""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListTrafficPolicyInstancesByPolicyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <TrafficPolicyInstances>
{instances_xml}  </TrafficPolicyInstances>
  <IsTruncated>false</IsTruncated>
  <MaxItems>100</MaxItems>
</ListTrafficPolicyInstancesByPolicyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


# ---------------------------------------------------------------------------
# KeySigningKey CRUD
# ---------------------------------------------------------------------------


def _handle_create_key_signing_key(body: bytes) -> Response:
    """CreateKeySigningKey — in-memory implementation."""
    parsed = xmltodict.parse(body)
    req = parsed.get("CreateKeySigningKeyRequest", {})
    zone_id = req.get("HostedZoneId", "")
    name = req.get("Name", "")
    kms_arn = req.get("KeyManagementServiceArn", "")
    status = req.get("Status", "INACTIVE")

    if zone_id not in _key_signing_keys:
        _key_signing_keys[zone_id] = {}

    ksk = {
        "Name": name,
        "KmsArn": kms_arn,
        "Status": status,
        "StatusMessage": "",
        "CreatedDate": "2026-01-01T00:00:00Z",
        "LastModifiedDate": "2026-01-01T00:00:00Z",
        "Flag": 257,
        "SigningAlgorithmMnemonic": "ECDSAP256SHA256",
        "SigningAlgorithmType": 13,
        "DigestAlgorithmMnemonic": "SHA-256",
        "DigestAlgorithmType": 2,
        "KeyTag": 1,
        "DigestValue": "abc123",
        "PublicKey": "publickey==",
        "DSRecord": "example DS record",
        "DNSKEYRecord": "example DNSKEY record",
    }
    _key_signing_keys[zone_id][name] = ksk

    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateKeySigningKeyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
  <KeySigningKey>
    <Name>{name}</Name>
    <KmsArn>{kms_arn}</KmsArn>
    <Status>{status}</Status>
    <StatusMessage></StatusMessage>
    <CreatedDate>2026-01-01T00:00:00Z</CreatedDate>
    <LastModifiedDate>2026-01-01T00:00:00Z</LastModifiedDate>
    <Flag>257</Flag>
    <SigningAlgorithmMnemonic>ECDSAP256SHA256</SigningAlgorithmMnemonic>
    <SigningAlgorithmType>13</SigningAlgorithmType>
    <DigestAlgorithmMnemonic>SHA-256</DigestAlgorithmMnemonic>
    <DigestAlgorithmType>2</DigestAlgorithmType>
    <KeyTag>1</KeyTag>
    <DigestValue>abc123</DigestValue>
    <PublicKey>publickey==</PublicKey>
    <DSRecord>example DS record</DSRecord>
    <DNSKEYRecord>example DNSKEY record</DNSKEYRecord>
  </KeySigningKey>
  <Location>https://route53.amazonaws.com/2013-04-01/hostedzone/{zone_id}/keysigningkeys/{name}</Location>
</CreateKeySigningKeyResponse>"""
    return Response(
        content=xml,
        status_code=201,
        media_type="text/xml",
        headers={
            "Location": f"https://route53.amazonaws.com/2013-04-01/hostedzone/{zone_id}/keysigningkeys/{name}"
        },
    )


def _handle_activate_key_signing_key(zone_id: str, ksk_name: str) -> Response:
    """ActivateKeySigningKey — in-memory implementation."""
    ksks = _key_signing_keys.get(zone_id, {})
    ksk = ksks.get(ksk_name)
    if not ksk:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchKeySigningKey</Code>
    <Message>No key signing key found with name {ksk_name} in zone {zone_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    ksk["Status"] = "ACTIVE"
    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ActivateKeySigningKeyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</ActivateKeySigningKeyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_deactivate_key_signing_key(zone_id: str, ksk_name: str) -> Response:
    """DeactivateKeySigningKey — in-memory implementation."""
    ksks = _key_signing_keys.get(zone_id, {})
    ksk = ksks.get(ksk_name)
    if not ksk:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchKeySigningKey</Code>
    <Message>No key signing key found with name {ksk_name} in zone {zone_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    ksk["Status"] = "INACTIVE"
    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DeactivateKeySigningKeyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</DeactivateKeySigningKeyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_delete_key_signing_key(zone_id: str, ksk_name: str) -> Response:
    """DeleteKeySigningKey — in-memory implementation."""
    ksks = _key_signing_keys.get(zone_id, {})
    ksk = ksks.get(ksk_name)
    if not ksk:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Error>
    <Type>Sender</Type>
    <Code>NoSuchKeySigningKey</Code>
    <Message>No key signing key found with name {ksk_name} in zone {zone_id}</Message>
  </Error>
</ErrorResponse>"""
        return Response(content=xml, status_code=404, media_type="text/xml")

    del ksks[ksk_name]
    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DeleteKeySigningKeyResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</DeleteKeySigningKeyResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


# ---------------------------------------------------------------------------
# DNSSEC Enable/Disable
# ---------------------------------------------------------------------------


def _handle_enable_hosted_zone_dnssec(zone_id: str) -> Response:
    """EnableHostedZoneDNSSEC — stub implementation."""
    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<EnableHostedZoneDNSSECResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</EnableHostedZoneDNSSECResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_disable_hosted_zone_dnssec(zone_id: str) -> Response:
    """DisableHostedZoneDNSSEC — stub implementation."""
    change_id = f"/change/{uuid.uuid4().hex[:14].upper()}"
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DisableHostedZoneDNSSECResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <ChangeInfo>
    <Id>{change_id}</Id>
    <Status>INSYNC</Status>
    <SubmittedAt>2026-01-01T00:00:00Z</SubmittedAt>
  </ChangeInfo>
</DisableHostedZoneDNSSECResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _handle_update_hosted_zone_features(zone_id: str, body: bytes) -> Response:
    """UpdateHostedZoneFeatures — stub implementation."""
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<UpdateHostedZoneFeaturesResponse xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>{zone_id}</HostedZoneId>
</UpdateHostedZoneFeaturesResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")
