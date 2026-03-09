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


class TestDSQLGetClusterErrors:
    """Tests for Aurora DSQL get_cluster error handling."""

    def test_get_cluster_not_found(self, dsql):
        with pytest.raises(dsql.exceptions.ResourceNotFoundException) as exc_info:
            dsql.get_cluster(identifier="nonexistent-cluster-id-12345")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestDSQLVpcEndpoint:
    """Tests for Aurora DSQL VPC endpoint operations."""

    def test_get_vpc_endpoint_service_name(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        try:
            resp = dsql.get_vpc_endpoint_service_name(identifier=cluster_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "serviceName" in resp
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_get_vpc_endpoint_service_name_not_found(self, dsql):
        with pytest.raises(dsql.exceptions.ResourceNotFoundException) as exc_info:
            dsql.get_vpc_endpoint_service_name(identifier="nonexistent-cluster-id-12345")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestDSQLTagsForResource:
    """Tests for Aurora DSQL tag operations."""

    def test_list_tags_for_resource(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_arn = create_resp["arn"]
        try:
            resp = dsql.list_tags_for_resource(resourceArn=cluster_arn)
            assert "tags" in resp
        finally:
            dsql.delete_cluster(identifier=create_resp["identifier"])

    def test_list_tags_for_resource_not_found(self, dsql):
        with pytest.raises(dsql.exceptions.ResourceNotFoundException) as exc_info:
            dsql.list_tags_for_resource(
                resourceArn="arn:aws:dsql:us-east-1:123456789012:cluster/nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
