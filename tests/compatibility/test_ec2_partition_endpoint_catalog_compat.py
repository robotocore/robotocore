"""EC2 region listing stays in the caller's partition; interface endpoint catalog completeness."""

import os

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


class TestEc2Catalogs:
    def test_interface_endpoint_services_exist(self):
        names = [f"com.amazonaws.us-east-1.{s}" for s in ("oidc-eks", "sts-fips", "eks-fips")]
        got = _client("ec2").describe_vpc_endpoint_services(ServiceNames=names)
        assert sorted(d["ServiceName"] for d in got["ServiceDetails"]) == sorted(names)

    def test_describe_regions_stays_in_partition(self):
        regions = _client("ec2").describe_regions(AllRegions=True)["Regions"]
        names = [r["RegionName"] for r in regions]
        assert "us-east-1" in names
        assert not [n for n in names if n.startswith(("us-gov-", "cn-"))]
