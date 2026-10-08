"""Integration test for `aws_route` CoreNetworkArn round-trip fidelity.

`CreateRoute` and `ReplaceRoute` accept `CoreNetworkArn` (Cloud WAN routing) and the
`Route` shape reports it back; emulators that drop the attribute make IaC tools that
snapshot the attribute after a write see it as null on the next read, which shows up
as a perpetual in-place update on every plan.
"""


class TestRouteCoreNetworkArn:
    def test_create_route_echoes_core_network_arn(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.40.0.0/16")["Vpc"]["VpcId"]
        route_table_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"][
            "RouteTableId"
        ]
        arn = (
            "arn:aws:networkmanager:us-east-1:123456789012:core-network/"
            "core-network-0123456789abcdef0"
        )

        ec2.create_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock="10.41.0.0/16",
            CoreNetworkArn=arn,
        )
        route = next(
            r
            for r in ec2.describe_route_tables(RouteTableIds=[route_table_id])[
                "RouteTables"
            ][0]["Routes"]
            if r.get("DestinationCidrBlock") == "10.41.0.0/16"
        )
        assert route["CoreNetworkArn"] == arn

    def test_replace_route_updates_core_network_arn(self, make_boto_client):
        ec2 = make_boto_client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.42.0.0/16")["Vpc"]["VpcId"]
        route_table_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"][
            "RouteTableId"
        ]
        arn = (
            "arn:aws:networkmanager:us-east-1:123456789012:core-network/"
            "core-network-fedcba9876543210f"
        )
        ec2.create_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock="10.43.0.0/16",
            CoreNetworkArn=arn,
        )
        replaced = arn.replace("fedcba9876543210f", "0123456789abcdef0")
        ec2.replace_route(
            RouteTableId=route_table_id,
            DestinationCidrBlock="10.43.0.0/16",
            CoreNetworkArn=replaced,
        )
        route = next(
            r
            for r in ec2.describe_route_tables(RouteTableIds=[route_table_id])[
                "RouteTables"
            ][0]["Routes"]
            if r.get("DestinationCidrBlock") == "10.43.0.0/16"
        )
        assert route["CoreNetworkArn"] == replaced
