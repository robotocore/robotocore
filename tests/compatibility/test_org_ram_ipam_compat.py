"""Organization-wide behaviours: cross-account invitations, RAM sharing and a shared IPAM."""

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


class TestOrganizationsMembership:
    def test_invited_account_accepts_and_describes_organization(self):
        mgmt, member = "910000000201", "920000000201"
        org = _client("organizations", mgmt).create_organization(FeatureSet="ALL")["Organization"]
        hs = _client("organizations", mgmt).invite_account_to_organization(
            Target={"Id": member, "Type": "ACCOUNT"}
        )["Handshake"]
        seen = _client("organizations", member).list_handshakes_for_account()["Handshakes"]
        assert hs["Id"] in [h["Id"] for h in seen]
        accepted = _client("organizations", member).accept_handshake(HandshakeId=hs["Id"])
        assert accepted["Handshake"]["State"] == "ACCEPTED"
        described = _client("organizations", member).describe_organization()["Organization"]
        assert described["Id"] == org["Id"]
        assert described["MasterAccountId"] == mgmt
        accounts = _client("organizations", mgmt).list_accounts()["Accounts"]
        assert member in [a["Id"] for a in accounts]


class TestSharedIpamPool:
    """An org-wide IPAM: a network account shares a pool with a nested OU; a member builds a VPC."""

    def test_member_finds_shared_pool_and_allocates_vpc(self):
        mgmt, net, member = "960000000001", "960000000002", "960000000003"
        orgs = _client("organizations", mgmt)
        orgs.create_organization(FeatureSet="ALL")
        root = orgs.list_roots()["Roots"][0]["Id"]
        outer = orgs.create_organizational_unit(ParentId=root, Name="global")["OrganizationalUnit"]
        inner = orgs.create_organizational_unit(ParentId=outer["Id"], Name="prod")[
            "OrganizationalUnit"
        ]
        for account, parent in ((net, outer["Id"]), (member, inner["Id"])):
            hs = orgs.invite_account_to_organization(Target={"Id": account, "Type": "ACCOUNT"})
            _client("organizations", account).accept_handshake(HandshakeId=hs["Handshake"]["Id"])
            orgs.move_account(AccountId=account, SourceParentId=root, DestinationParentId=parent)

        ec2 = _client("ec2", net)
        ipam = ec2.create_ipam(OperatingRegions=[{"RegionName": "us-east-1"}])["Ipam"]
        pool = ec2.create_ipam_pool(
            IpamScopeId=ipam["PrivateDefaultScopeId"],
            AddressFamily="ipv4",
            Locale="us-east-1",
            Description="account-pool-member-us-east-1",
        )["IpamPool"]
        ec2.provision_ipam_pool_cidr(IpamPoolId=pool["IpamPoolId"], Cidr="10.30.0.0/16")
        _client("ram", net).create_resource_share(
            name="ipam", resourceArns=[pool["IpamPoolArn"]], principals=[outer["Arn"]]
        )

        mec2 = _client("ec2", member)
        found = mec2.describe_ipam_pools(
            Filters=[
                {"Name": "locale", "Values": ["us-east-1"]},
                {"Name": "description", "Values": ["account-pool-member-us-east-1"]},
            ]
        )["IpamPools"]
        assert [p["IpamPoolId"] for p in found] == [pool["IpamPoolId"]]
        assert found[0]["OwnerId"] == net
        vpc = mec2.create_vpc(Ipv4IpamPoolId=pool["IpamPoolId"], Ipv4NetmaskLength=20)["Vpc"]
        assert vpc["CidrBlock"] == "10.30.0.0/20"
        shares = _client("ram", member).get_resource_shares(resourceOwner="OTHER-ACCOUNTS")
        assert [s["name"] for s in shares["resourceShares"]] == ["ipam"]
        assert _client("ec2", "960000000099").describe_ipam_pools()["IpamPools"] == []


class TestIpamScopes:
    def test_describe_ipam_scopes_by_id(self):
        ec2 = _client("ec2", "300000000030")
        ipam = ec2.create_ipam(OperatingRegions=[{"RegionName": "us-east-1"}])["Ipam"]
        scope = ipam["PrivateDefaultScopeId"]
        got = ec2.describe_ipam_scopes(IpamScopeIds=[scope])["IpamScopes"]
        assert [s["IpamScopeId"] for s in got] == [scope]
        assert got[0]["IpamScopeArn"].endswith(scope)
