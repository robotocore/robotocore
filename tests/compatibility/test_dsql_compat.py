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


class TestDSQLClusterDetails:
    """Tests for Aurora DSQL cluster detail fields."""

    def test_create_cluster_returns_status(self, dsql):
        resp = dsql.create_cluster(deletionProtectionEnabled=False)
        assert "status" in resp
        assert "creationTime" in resp
        cluster_id = resp["identifier"]
        dsql.delete_cluster(identifier=cluster_id)

    def test_get_cluster_returns_full_details(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        try:
            resp = dsql.get_cluster(identifier=cluster_id)
            assert resp["identifier"] == cluster_id
            assert "arn" in resp
            assert "status" in resp
            assert "deletionProtectionEnabled" in resp
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_delete_cluster_not_found(self, dsql):
        with pytest.raises(dsql.exceptions.ResourceNotFoundException):
            dsql.delete_cluster(identifier="nonexistent-cluster-id-12345")


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


class TestDSQLNewOps:
    """Tests for newly implemented DSQL operations."""

    def test_list_clusters(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        try:
            resp = dsql.list_clusters()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "clusters" in resp
            ids = [c["identifier"] for c in resp["clusters"]]
            assert cluster_id in ids
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_update_cluster(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        try:
            resp = dsql.update_cluster(identifier=cluster_id, deletionProtectionEnabled=True)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["identifier"] == cluster_id
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_put_and_get_cluster_policy(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        policy = '{"Version":"2012-10-17","Statement":[]}'
        try:
            dsql.put_cluster_policy(identifier=cluster_id, policy=policy)
            resp = dsql.get_cluster_policy(identifier=cluster_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["policy"] == policy
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_delete_cluster_policy(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        try:
            dsql.put_cluster_policy(identifier=cluster_id, policy='{"Version":"2012-10-17"}')
            dsql.delete_cluster_policy(identifier=cluster_id)
            resp = dsql.get_cluster_policy(identifier=cluster_id)
            assert resp["policy"] == ""
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_tag_resource(self, dsql):
        create_resp = dsql.create_cluster(deletionProtectionEnabled=False)
        cluster_id = create_resp["identifier"]
        cluster_arn = create_resp["arn"]
        try:
            dsql.tag_resource(resourceArn=cluster_arn, tags={"NewTag": "newval"})
            resp = dsql.list_tags_for_resource(resourceArn=cluster_arn)
            assert resp["tags"].get("NewTag") == "newval"
        finally:
            dsql.delete_cluster(identifier=cluster_id)

    def test_untag_resource(self, dsql):
        create_resp = dsql.create_cluster(
            deletionProtectionEnabled=False, tags={"ToKeep": "yes", "ToRemove": "no"}
        )
        cluster_id = create_resp["identifier"]
        cluster_arn = create_resp["arn"]
        try:
            dsql.untag_resource(resourceArn=cluster_arn, tagKeys=["ToRemove"])
            resp = dsql.list_tags_for_resource(resourceArn=cluster_arn)
            assert "ToRemove" not in resp["tags"]
            assert resp["tags"].get("ToKeep") == "yes"
        finally:
            dsql.delete_cluster(identifier=cluster_id)
