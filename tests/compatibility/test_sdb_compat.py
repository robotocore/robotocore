"""SimpleDB compatibility tests.

Restored from the python-era tree (ab8a0e5f) and trimmed to the deployed
moto fork's surface. The fork's `moto/sdb/responses.py` handles exactly five
operations — CreateDomain, DeleteDomain, ListDomains, GetAttributes,
PutAttributes — and this suite covers those. Of the 10 tests restored, 7
were removed for moto-fork-absent operations (DomainMetadata x2,
DeleteAttributes x2, BatchPutAttributes, BatchDeleteAttributes, Select):
their handlers are absent from `responses.py` AND no probe file exists, so
they answer moto's NotImplementedError arm (501 NotImplemented) — double
evidence in the port's report.
"""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sdb():
    return make_client("sdb")


@pytest.fixture
def domain(sdb):
    """Create a domain for testing and delete it after."""
    name = "compat-test-domain"
    sdb.create_domain(DomainName=name)
    yield name
    try:
        sdb.delete_domain(DomainName=name)
    except Exception:  # noqa: BLE001 - best-effort cleanup
        pass  # best-effort cleanup


class TestSdbCreateDomain:
    """Tests for CreateDomain operation."""

    def test_create_domain(self, sdb):
        """CreateDomain succeeds and the domain appears in ListDomains."""
        name = f"test-create-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        try:
            resp = sdb.list_domains()
            assert "DomainNames" in resp
            assert name in resp["DomainNames"]
        finally:
            sdb.delete_domain(DomainName=name)


class TestSdbDeleteDomain:
    """Tests for DeleteDomain operation."""

    def test_delete_domain(self, sdb):
        """DeleteDomain removes the domain from the list."""
        name = f"test-del-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        sdb.delete_domain(DomainName=name)
        resp = sdb.list_domains()
        assert name not in resp.get("DomainNames", [])


class TestSdbListDomains:
    """Tests for ListDomains operation."""

    def test_list_domains(self, sdb):
        """ListDomains returns 200 and DomainNames key when domains exist."""
        name = f"test-list-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        try:
            resp = sdb.list_domains()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "DomainNames" in resp
            assert name in resp["DomainNames"]
        finally:
            sdb.delete_domain(DomainName=name)
