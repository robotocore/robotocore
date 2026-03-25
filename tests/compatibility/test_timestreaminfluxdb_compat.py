"""Compatibility tests for Timestream for InfluxDB service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def influxdb():
    return make_client("timestream-influxdb")


class TestCreateDbInstance:
    """Tests for CreateDbInstance operation."""

    def test_create_db_instance(self, influxdb):
        name = unique_name("inst")
        resp = influxdb.create_db_instance(
            name=name,
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in resp
        assert "arn" in resp
        assert resp["name"] == name
        influxdb.delete_db_instance(identifier=resp["id"])


class TestCreateDbCluster:
    """Tests for CreateDbCluster operation."""

    def test_create_db_cluster(self, influxdb):
        resp = influxdb.create_db_cluster(
            name=unique_name("cluster"),
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            password="TestPassword123!",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "dbClusterId" in resp
        assert "dbClusterStatus" in resp


class TestDeleteDbInstance:
    """Tests for DeleteDbInstance operation."""

    def test_delete_db_instance_not_found(self, influxdb):
        with pytest.raises(influxdb.exceptions.ResourceNotFoundException) as exc_info:
            influxdb.delete_db_instance(identifier="nonexistent-instance-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_db_instance(self, influxdb):
        create_resp = influxdb.create_db_instance(
            name=unique_name("del-inst"),
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        instance_id = create_resp["id"]
        resp = influxdb.delete_db_instance(identifier=instance_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGetDbCluster:
    """Tests for GetDbCluster operation."""

    def test_get_db_cluster_not_found(self, influxdb):
        with pytest.raises(influxdb.exceptions.ResourceNotFoundException) as exc_info:
            influxdb.get_db_cluster(dbClusterId="nonexistent-cluster-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestGetDbInstance:
    """Tests for GetDbInstance operation."""

    def test_get_db_instance_not_found(self, influxdb):
        with pytest.raises(influxdb.exceptions.ResourceNotFoundException) as exc_info:
            influxdb.get_db_instance(identifier="nonexistent-instance-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_db_instance(self, influxdb):
        create_resp = influxdb.create_db_instance(
            name=unique_name("get-inst"),
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        instance_id = create_resp["id"]
        try:
            resp = influxdb.get_db_instance(identifier=instance_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["id"] == instance_id
            assert "name" in resp
            assert "arn" in resp
        finally:
            influxdb.delete_db_instance(identifier=instance_id)


class TestGetDbParameterGroup:
    """Tests for GetDbParameterGroup operation."""

    def test_get_db_parameter_group_not_found(self, influxdb):
        with pytest.raises(influxdb.exceptions.ResourceNotFoundException) as exc_info:
            influxdb.get_db_parameter_group(identifier="nonexistent-group-id")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestListTagsForResource:
    """Tests for ListTagsForResource operation."""

    def test_list_tags_for_resource(self, influxdb):
        create_resp = influxdb.create_db_instance(
            name=unique_name("tag-list-inst"),
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        instance_id = create_resp["id"]
        instance_arn = create_resp["arn"]
        try:
            resp = influxdb.list_tags_for_resource(resourceArn=instance_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "tags" in resp
        finally:
            influxdb.delete_db_instance(identifier=instance_id)


class TestTagResource:
    """Tests for TagResource operation."""

    def test_tag_resource(self, influxdb):
        create_resp = influxdb.create_db_instance(
            name=unique_name("tag-res-inst"),
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        instance_id = create_resp["id"]
        instance_arn = create_resp["arn"]
        try:
            influxdb.tag_resource(resourceArn=instance_arn, tags={"Environment": "test"})
            resp = influxdb.list_tags_for_resource(resourceArn=instance_arn)
            assert resp["tags"].get("Environment") == "test"
        finally:
            influxdb.delete_db_instance(identifier=instance_id)


class TestUntagResource:
    """Tests for UntagResource operation."""

    def test_untag_resource(self, influxdb):
        create_resp = influxdb.create_db_instance(
            name=unique_name("untag-inst"),
            password="TestPassword123!",
            dbInstanceType="db.influx.medium",
            vpcSubnetIds=["subnet-12345678"],
            vpcSecurityGroupIds=["sg-12345678"],
            allocatedStorage=20,
        )
        instance_id = create_resp["id"]
        instance_arn = create_resp["arn"]
        try:
            influxdb.tag_resource(
                resourceArn=instance_arn, tags={"ToKeep": "yes", "ToRemove": "no"}
            )
            resp = influxdb.untag_resource(resourceArn=instance_arn, tagKeys=["ToRemove"])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            tags_resp = influxdb.list_tags_for_resource(resourceArn=instance_arn)
            assert "ToRemove" not in tags_resp["tags"]
            assert tags_resp["tags"].get("ToKeep") == "yes"
        finally:
            influxdb.delete_db_instance(identifier=instance_id)
