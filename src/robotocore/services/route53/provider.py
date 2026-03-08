"""Native Route53 provider.

Intercepts operations that Moto has bugs or doesn't implement:
- CreateHostedZone with PrivateZone but no Comment (Moto KeyError bug)
- TestDNSAnswer: Not implemented in Moto
- CreateQueryLoggingConfig: Moto cross-service validation too strict
- AssociateVPCWithHostedZone / DisassociateVPCFromHostedZone
"""

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

# Store query logging configs
_query_log_configs: dict[str, dict] = {}


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

            return await forward_to_moto_with_body(request, "route53", body_str.encode("utf-8"))

    # AssociateVPCWithHostedZone
    m = _ASSOCIATE_VPC_RE.match(path)
    if m and request.method == "POST":
        zone_id = m.group(1)
        body = await request.body()
        return _handle_associate_vpc(zone_id, body, region, account_id)

    # DisassociateVPCFromHostedZone
    m = _DISASSOCIATE_VPC_RE.match(path)
    if m and request.method == "POST":
        return _handle_disassociate_vpc(region, account_id)

    return await forward_to_moto(request, "route53")


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
    except Exception:
        pass

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
  <RecordData>
{records_xml}  </RecordData>
</TestDNSAnswerResponse>"""
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


def _handle_associate_vpc(zone_id: str, body: bytes, region: str, account_id: str) -> Response:
    """AssociateVPCWithHostedZone — store VPC association."""
    from moto.backends import get_backend

    try:
        backend = get_backend("route53")[account_id]["global"]
        zone = backend.get_hosted_zone(zone_id)
        if zone:
            parsed = xmltodict.parse(body)
            req = parsed.get("AssociateVPCWithHostedZoneRequest", {})
            vpc = req.get("VPC", {})
            vpc_id = vpc.get("VPCId", "")
            vpc_region = vpc.get("VPCRegion", region)
            if not hasattr(zone, "vpcs"):
                zone.vpcs = []
            zone.vpcs.append({"VPCId": vpc_id, "VPCRegion": vpc_region})
    except Exception:
        pass

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


def _handle_disassociate_vpc(region: str, account_id: str) -> Response:
    """DisassociateVPCFromHostedZone."""
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
