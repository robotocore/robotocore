"""Compatibility tests for Aurora DSQL service."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def dsql():
    return make_client("dsql")


class TestDSQLClusterOperations:
    """Tests for Aurora DSQL cluster operations."""

    def test_create_cluster(self, dsql):
        resp = dsql.create_cluster(deletionProtectionEnabled=False)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "identifier" in resp
        assert "arn" in resp
        # Clean up
        dsql.delete_cluster(identifier=resp["identifier"])

    def test_get_cluster(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        resp = dsql.get_cluster(identifier=cluster_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["identifier"] == cluster_id
        dsql.delete_cluster(identifier=cluster_id)

    def test_delete_cluster(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        resp = dsql.delete_cluster(identifier=create_resp["identifier"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
