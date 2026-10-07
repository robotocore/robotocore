"""Compatibility: fabricating a resource with a caller-chosen identity (admin plane).

The twin cannot mint a specific id through the AWS wire protocol — the caller only gets
ids back. This admin-plane endpoint is how a seeding tool creates the object *named* by
literal ids in real code (a peer VPC that exists only outside Terraform, for example).
"""

import boto3
import pytest
import requests

from .conftest import ENDPOINT_URL

_ACCOUNT = "555555555555"

_FABRICATED_VPC = "vpc-0feedface00056789"


@pytest.fixture(autouse=True)
def fabricated_vpc():
    resp = requests.post(
        f"{ENDPOINT_URL}/_robotocore/ec2/fabricate",
        json={
            "resources": [
                {
                    "account": _ACCOUNT,
                    "region": "us-east-1",
                    "kind": "vpc",
                    "id": _FABRICATED_VPC,
                    "cidr_block": "10.241.0.0/16",
                    "tags": {"Name": "out-of-band-peer"},
                }
            ]
        },
    )
    if resp.status_code != 200:
        pytest.fail(f"fabricate failed: {resp.status_code} {resp.text[:200]}")
    ec2 = _ec2()
    yield resp.json()
    # The peerings and requester VPCs the tests create come down with the account's
    # fabricated state so repeat runs stay clean.
    for pcx in ec2.describe_vpc_peering_connections()["VpcPeeringConnections"]:
        ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx["VpcPeeringConnectionId"])
    for vpc in ec2.describe_vpcs(Filters=[{"Name": "cidr", "Values": ["10.242.0.0/16"]}])["Vpcs"]:
        if vpc["VpcId"] != _FABRICATED_VPC:
            subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc["VpcId"]]}])
            for subnet in subnets["Subnets"]:
                ec2.delete_subnet(SubnetId=subnet["SubnetId"])
            ec2.delete_vpc(VpcId=vpc["VpcId"])


def _ec2() -> boto3.client:
    return boto3.client(
        "ec2",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id=_ACCOUNT,
        aws_secret_access_key="test",
    )


class TestFabricateCompat:
    def test_describe_vpcs_sees_fabricated_vpc(self):
        r = _ec2().describe_vpcs(VpcIds=[_FABRICATED_VPC])
        vpcs = r["Vpcs"]
        assert len(vpcs) == 1
        assert vpcs[0]["CidrBlock"] == "10.241.0.0/16"
        names = [t["Value"] for t in vpcs[0]["Tags"] if t["Key"] == "Name"]
        assert names == ["out-of-band-peer"]

    def test_second_call_returns_skipped_duplicate(self, fabricated_vpc):
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/ec2/fabricate",
            json={
                "resources": [
                    {"account": _ACCOUNT, "region": "us-east-1", "id": _FABRICATED_VPC}
                ]
            },
        )
        assert resp.status_code == 200
        assert resp.json()["fabricated"] == []
        assert len(resp.json()["skipped"]) == 1

    def test_peering_to_fabricated_vpc_gets_aws_behaviour(self):
        """CreateVpcPeeringConnection validates the peer, as AWS does."""
        ec2 = _ec2()
        # The requester is an ordinary VPC created through the API, so the negative case
        # can only be about the peer: a missing peer id fails even with a real requester.
        requester = ec2.create_vpc(CidrBlock="10.242.0.0/16")["Vpc"]["VpcId"]
        with pytest.raises(ec2.exceptions.ClientError) as exc:
            ec2.create_vpc_peering_connection(
                VpcId=requester, PeerVpcId="vpc-0999999999missing"
            )
        assert "InvalidVpcID.NotFound" in str(exc.value)
        # The accepter is the fabricated one. CIDRs do not overlap, as AWS requires.
        r = ec2.create_vpc_peering_connection(VpcId=requester, PeerVpcId=_FABRICATED_VPC)
        pcx = r["VpcPeeringConnection"]
        assert pcx["AccepterVpcInfo"]["VpcId"] == _FABRICATED_VPC
        assert pcx["Status"]["Code"] in ("initiating-request", "pending-acceptance")

    def test_rejects_malformed_payload(self):
        resp = requests.post(
            f"{ENDPOINT_URL}/_robotocore/ec2/fabricate",
            json={"resources": [{"account": _ACCOUNT, "id": "not-a-vpc"}]},
        )
        assert resp.status_code == 400
        assert ".id" in resp.text

    def test_rejects_bad_cidr_with_400(self):
        for cidr in ("banana", "10.0.0.0/8"):
            resp = requests.post(
                f"{ENDPOINT_URL}/_robotocore/ec2/fabricate",
                json={
                    "resources": [
                        {
                            "account": _ACCOUNT,
                            "region": "us-east-1",
                            "id": "vpc-0feedface09999999",
                            "cidr_block": cidr,
                        }
                    ]
                },
            )
            assert resp.status_code == 400, f"{cidr}: {resp.status_code} {resp.text[:120]}"
            assert "cidr_block" in resp.text
