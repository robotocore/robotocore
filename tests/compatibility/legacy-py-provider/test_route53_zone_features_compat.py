"""Route 53 hosted-zone fields that AWS provider v6.33+ requires."""

import os
import uuid

import boto3

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestHostedZoneFeatures:
    def test_hosted_zone_reports_accelerated_recovery(self):
        r53 = _client("route53")
        zone = r53.create_hosted_zone(
            Name=f"z{uuid.uuid4().hex[:6]}.example.com", CallerReference=uuid.uuid4().hex
        )["HostedZone"]
        assert zone["Features"]["AcceleratedRecoveryStatus"] == "DISABLED"
        got = r53.get_hosted_zone(Id=zone["Id"])["HostedZone"]
        assert got["Features"]["AcceleratedRecoveryStatus"] == "DISABLED"
