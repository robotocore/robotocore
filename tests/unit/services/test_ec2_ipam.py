"""IPAM pool filters and VPC CIDR allocation from a pool."""

from moto.ec2.models import ec2_backends

from robotocore.services.ec2.ipam import _filters, _matches, allocate


def _pool(account: str):
    backend = ec2_backends[account]["us-east-1"]
    ipam = backend.create_ipam(operating_regions=["us-east-1"])
    pool = backend.create_ipam_pool(
        ipam_scope_id=ipam.private_default_scope.id,
        address_family="ipv4",
        locale="us-east-1",
        description="account-pool-x-us-east-1",
    )
    backend.provision_ipam_pool_cidr(ipam_pool_id=pool.id, cidr="10.20.0.0/16")
    return backend, pool


def test_filter_parsing():
    params = {
        "Filter.1.Name": ["description"],
        "Filter.1.Value.1": ["a"],
        "Filter.2.Name": ["locale"],
        "Filter.2.Value.1": ["us-east-1"],
        "Filter.2.Value.2": ["us-east-2"],
    }
    assert _filters(params) == {"description": ["a"], "locale": ["us-east-1", "us-east-2"]}


def test_matches_description_and_locale():
    backend, pool = _pool("950000000001")
    assert _matches(pool, backend, {"description": ["account-pool-x-us-east-1"]})
    assert _matches(pool, backend, {"locale": ["us-east-1"], "address-family": ["ipv4"]})
    assert not _matches(pool, backend, {"description": ["other"]})


def test_allocations_do_not_overlap():
    backend, pool = _pool("950000000002")
    first = allocate(pool, backend, 20, None, "vpc-1", "950000000002", "us-east-1")
    second = allocate(pool, backend, 20, None, "vpc-2", "950000000002", "us-east-1")
    assert first == "10.20.0.0/20"
    assert second == "10.20.16.0/20"


def test_allocation_fails_when_pool_exhausted():
    backend, pool = _pool("950000000003")
    assert allocate(pool, backend, 16, None, "vpc-1", "950000000003", "us-east-1") == "10.20.0.0/16"
    assert allocate(pool, backend, 20, None, "vpc-2", "950000000003", "us-east-1") is None
