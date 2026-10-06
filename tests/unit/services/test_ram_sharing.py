"""RAM principals cover accounts directly, via the organization, or via any ancestor OU."""

from moto.organizations.models import organizations_backends

from robotocore.services.organizations.provider import _join_organization
from robotocore.services.ram.sharing import principal_covers


def _tree(mgmt: str, member: str):
    backend = organizations_backends[mgmt]["aws"]
    backend.create_organization(region="us-east-1", FeatureSet="ALL")
    root = backend.org.root_id
    outer = backend.create_organizational_unit(ParentId=root, Name="outer")["OrganizationalUnit"]
    inner = backend.create_organizational_unit(ParentId=outer["Id"], Name="inner")[
        "OrganizationalUnit"
    ]
    _join_organization(backend, member, "aws")
    backend.move_account(AccountId=member, SourceParentId=root, DestinationParentId=inner["Id"])
    return backend, outer, inner


def test_account_id_and_iam_arn():
    assert principal_covers("555500000001", "555500000001")
    assert principal_covers("arn:aws:iam::555500000001:root", "555500000001")
    assert not principal_covers("555500000002", "555500000001")


def test_organization_arn_covers_member():
    backend, _, _ = _tree("940000000001", "555500000011")
    assert principal_covers(backend.org.arn, "555500000011")
    assert not principal_covers(backend.org.arn, "555500000099")


def test_ancestor_ou_covers_nested_member():
    _, outer, inner = _tree("940000000002", "555500000012")
    assert principal_covers(inner["Arn"], "555500000012")
    assert principal_covers(outer["Arn"], "555500000012")


def test_sibling_ou_does_not_cover():
    backend, outer, _ = _tree("940000000003", "555500000013")
    other = backend.create_organizational_unit(ParentId=backend.org.root_id, Name="other")[
        "OrganizationalUnit"
    ]
    assert not principal_covers(other["Arn"], "555500000013")
