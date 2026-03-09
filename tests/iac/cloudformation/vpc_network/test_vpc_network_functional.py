"""Functional test: deploy VPC network and verify routing, NACLs, and subnets."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestVpcNetworkFunctional:
    """Deploy VPC network and verify routing tables, NACLs, and subnet associations."""

    def test_route_table_has_igw_route(self, deploy_stack):
        """Verify the public route table has a 0.0.0.0/0 route to the IGW."""
        stack = deploy_stack("vpc-func-rt", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        vpc_id = outputs["VpcId"]
        igw_id = outputs["InternetGatewayId"]
        ec2 = make_client("ec2")

        # Find the public route table (non-main, associated with this VPC)
        rt_resp = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        route_tables = rt_resp["RouteTables"]
        assert len(route_tables) >= 1

        # Find a route table with 0.0.0.0/0 -> IGW
        found_igw_route = False
        for rt in route_tables:
            for route in rt.get("Routes", []):
                if (
                    route.get("DestinationCidrBlock") == "0.0.0.0/0"
                    and route.get("GatewayId") == igw_id
                ):
                    found_igw_route = True
                    break
        assert found_igw_route, f"No 0.0.0.0/0 route to IGW {igw_id} found in VPC {vpc_id}"

    def test_nacl_default_allow(self, deploy_stack):
        """Verify the VPC has a NACL with default allow rules."""
        stack = deploy_stack("vpc-func-nacl", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        vpc_id = outputs["VpcId"]
        ec2 = make_client("ec2")

        nacl_resp = ec2.describe_network_acls(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        nacls = nacl_resp["NetworkAcls"]
        assert len(nacls) >= 1

        # Default NACL should have an allow-all entry (rule number 100 or similar)
        default_nacl = next((n for n in nacls if n["IsDefault"]), nacls[0])
        ingress_rules = [e for e in default_nacl["Entries"] if not e["Egress"]]
        allow_rules = [r for r in ingress_rules if r["RuleAction"] == "allow"]
        assert len(allow_rules) >= 1, "Expected at least one allow rule in NACL"

    def test_subnets_associated_with_route_table(self, deploy_stack):
        """Verify both public subnets are associated with the public route table."""
        stack = deploy_stack("vpc-func-sub", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        vpc_id = outputs["VpcId"]
        subnet1_id = outputs["PublicSubnet1Id"]
        subnet2_id = outputs["PublicSubnet2Id"]
        ec2 = make_client("ec2")

        # Get non-main route tables for this VPC
        rt_resp = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])

        # Collect all explicitly associated subnet IDs
        associated_subnets = set()
        for rt in rt_resp["RouteTables"]:
            for assoc in rt.get("Associations", []):
                if assoc.get("SubnetId"):
                    associated_subnets.add(assoc["SubnetId"])

        assert subnet1_id in associated_subnets, (
            f"Subnet {subnet1_id} not associated with any route table"
        )
        assert subnet2_id in associated_subnets, (
            f"Subnet {subnet2_id} not associated with any route table"
        )
