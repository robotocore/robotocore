"""Unit tests for the EC2 fabrication admin plane."""

import pytest

from robotocore.services.ec2.fabricate import (
    FabricateError,
    fabricate_resources,
)


@pytest.fixture(autouse=True)
def isolated_backend():
    """Use a dedicated account so tests never see production helper state."""
    from moto.ec2.models import ec2_backends

    account = "555555555555"
    region = "us-east-1"
    backend = ec2_backends[account][region]
    vpcs = set(backend.vpcs)
    tables = set(backend.route_tables)
    acls = set(backend.network_acls)
    sgs = {vpc: {sid for sid in by_vpc} for vpc, by_vpc in backend.groups.items()}
    tags = set(backend.tags.index()) if hasattr(backend.tags, "index") else set(backend.tags)
    yield account, region
    for vpc_id in set(backend.vpcs) - vpcs:
        backend.vpcs.pop(vpc_id, None)
    for table_id in set(backend.route_tables) - tables:
        backend.route_tables.pop(table_id, None)
    for acl_id in set(backend.network_acls) - acls:
        backend.network_acls.pop(acl_id, None)
    for vpc_id, by_vpc in backend.groups.items():
        for sg_id in set(by_vpc) - sgs.get(vpc_id, set()):
            by_vpc.pop(sg_id, None)
    for resource_id in set(backend.tags) - tags:
        backend.tags.pop(resource_id, None)


class TestFabricateResources:
    def test_fabricates_vpc_with_requested_id(self, isolated_backend):
        account, region = isolated_backend
        made, _ = fabricate_resources(
            [
                {
                    "account": account,
                    "region": region,
                    "id": "vpc-0abc1234def567890",
                    "cidr_block": "10.90.0.0/16",
                }
            ]
        )
        assert made[0]["vpc_id"] == "vpc-0abc1234def567890"
        assert made[0]["cidr_block"] == "10.90.0.0/16"
        from moto.ec2.models import ec2_backends

        vpc = ec2_backends[account][region].vpcs["vpc-0abc1234def567890"]
        assert vpc.cidr_block == "10.90.0.0/16"

    def test_generated_cidr_when_not_given(self, isolated_backend):
        account, region = isolated_backend
        made, _ = fabricate_resources(
            [{"account": account, "region": region, "id": "vpc-0beef000000000001"}]
        )
        assert made[0]["cidr_block"].startswith("10.")
        assert made[0]["cidr_block"].endswith(".0.0/16")

    def test_idempotent_second_call_is_skipped(self, isolated_backend):
        account, region = isolated_backend
        item = {"account": account, "region": region, "id": "vpc-0beef000000000002"}
        fabricate_resources([item])
        made, skipped = fabricate_resources([item])
        assert made == []
        assert len(skipped) == 1
        assert skipped[0]["vpc_id"] == "vpc-0beef000000000002"

    def test_tags_applied(self, isolated_backend):
        account, region = isolated_backend
        fabricate_resources(
            [
                {
                    "account": account,
                    "region": region,
                    "id": "vpc-0beef000000000003",
                    "tags": {"Name": "peer"},
                }
            ]
        )
        from moto.ec2.models import ec2_backends

        vpc = ec2_backends[account][region].vpcs["vpc-0beef000000000003"]
        tags = {t["key"]: t["value"] for t in vpc.get_tags()}
        assert tags == {"Name": "peer"}

    def test_rejects_bad_account(self):
        with pytest.raises(FabricateError) as exc:
            fabricate_resources([{"account": "nonsense", "id": "vpc-0beef000000000004"}])
        assert "account" in str(exc.value)

    def test_rejects_bad_id_shape(self, isolated_backend):
        account, region = isolated_backend
        with pytest.raises(FabricateError) as exc:
            fabricate_resources([{"account": account, "region": region, "id": "not-a-vpc"}])
        assert ".id" in str(exc.value)

    @pytest.mark.parametrize("ident", ["vpc-1a2b3c4d", "vpc-0abc1234def567890"])
    def test_id_shapes_are_fabricated(self, isolated_backend, ident):
        """Both the legacy 8-hex and the extended id shapes fabricate."""
        account, region = isolated_backend
        made, _ = fabricate_resources([{"account": account, "region": region, "id": ident}])
        assert made[0]["vpc_id"] == ident

    def test_global_generator_not_touched(self, isolated_backend):
        """No process-global monkeying: a concurrent CreateVpc must never steal the chosen id."""
        account, region = isolated_backend
        fabricate_resources(
            [
                {
                    "account": account,
                    "region": region,
                    "id": "vpc-0beef000000000005",
                    "cidr_block": "10.91.0.0/16",
                }
            ]
        )
        from moto.ec2.models import ec2_backends
        from moto.ec2.models import vpcs as vpcs_models

        assert vpcs_models.random_vpc_id() != "vpc-0beef000000000005"
        assert "vpc-0beef000000000005" in ec2_backends[account][region].vpcs

    def test_rebind_moves_dependencies(self, isolated_backend):
        """The default route table, network ACL and default SG follow the re-bound id."""
        account, region = isolated_backend
        fabricate_resources(
            [
                {
                    "account": account,
                    "region": region,
                    "id": "vpc-0beef000000000007",
                    "cidr_block": "10.92.0.0/16",
                }
            ]
        )
        from moto.ec2.models import ec2_backends

        backend = ec2_backends[account][region]
        tables = [t for t in backend.route_tables.values() if t.vpc_id == "vpc-0beef000000000007"]
        assert len(tables) >= 1
        acls = [a for a in backend.network_acls.values() if a.vpc_id == "vpc-0beef000000000007"]
        assert len(acls) >= 1
        by_vpc = backend.groups.get("vpc-0beef000000000007", {})
        default_sgs = [g for g in by_vpc.values() if g.name == "default"]
        assert len(default_sgs) == 1

    def test_bad_cidr_is_400_not_500(self, isolated_backend):

        account, region = isolated_backend

        with pytest.raises(FabricateError, match="CIDR"):
            fabricate_resources(
                [
                    {
                        "account": account,
                        "region": region,
                        "id": "vpc-0beef000000000008",
                        "cidr_block": "banana",
                    }
                ]
            )

    def test_bad_prefix_is_400(self, isolated_backend):
        account, region = isolated_backend
        with pytest.raises(FabricateError, match="prefix"):
            fabricate_resources(
                [
                    {
                        "account": account,
                        "region": region,
                        "id": "vpc-0beef000000000009",
                        "cidr_block": "10.0.0.0/8",
                    }
                ]
            )

    def test_null_tag_value_is_400(self, isolated_backend):
        account, region = isolated_backend
        with pytest.raises(FabricateError, match="tags"):
            fabricate_resources(
                [
                    {
                        "account": account,
                        "region": region,
                        "id": "vpc-0beef00000000000a",
                        "tags": {"Name": None},
                    }
                ]
            )

    def test_unknown_region_is_400(self, isolated_backend):
        with pytest.raises(FabricateError, match="region"):
            fabricate_resources(
                [{"account": "555555555555", "region": "banana-1", "id": "vpc-0beef00000000000b"}]
            )
