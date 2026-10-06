"""Invited accounts see, accept, and join through cross-account handshakes."""

from moto.organizations.models import organizations_backends

from robotocore.services.organizations.provider import _find_handshake, _join_organization


def _org(mgmt: str):
    backend = organizations_backends[mgmt]["aws"]
    backend.create_organization(region="us-east-1", FeatureSet="ALL")
    return backend


def test_find_handshake_across_accounts():
    backend = _org("910000000101")
    hs = backend.invite_account_to_organization(Target={"Id": "920000000101", "Type": "ACCOUNT"})
    found = _find_handshake(hs["Handshake"]["Id"])
    assert found is not None
    assert found[0] is backend


def test_join_makes_member_visible():
    backend = _org("910000000102")
    _join_organization(backend, "920000000102", "aws")
    member = organizations_backends["920000000102"]["aws"]
    assert member.describe_organization()["Organization"]["Id"] == backend.org.id
    joined = [a for a in backend.accounts if a.id == "920000000102"]
    assert joined and joined[0].joined_method == "INVITED"


def test_join_is_idempotent():
    backend = _org("910000000103")
    _join_organization(backend, "920000000103", "aws")
    _join_organization(backend, "920000000103", "aws")
    assert sum(1 for a in backend.accounts if a.id == "920000000103") == 1


def test_register_account_ids_validates():
    import pytest

    from robotocore.services.organizations.provider import register_account_ids

    with pytest.raises(ValueError):
        register_account_ids({"emails": {"a@example.com": "not-an-id"}})
    counts = register_account_ids({"names": {"Unit Name": "980000000001"}})
    assert counts["names"] >= 1


def test_preassigned_id_by_email_is_case_insensitive():
    from robotocore.services.organizations.provider import _preassigned_id, register_account_ids

    register_account_ids({"emails": {"Ops+Unit@Example.com": "980000000002"}})
    assert _preassigned_id({"Email": "ops+unit@example.com", "AccountName": "x"}) == "980000000002"
    assert _preassigned_id({"Email": "nobody@example.com", "AccountName": "nope"}) is None
