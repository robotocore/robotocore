"""Timestream Write compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def timestream_write():
    return make_client("timestream-write")


class TestTimestreamWriteOperations:
    def test_list_databases_empty(self, timestream_write):
        response = timestream_write.list_databases()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Databases" in response
        assert isinstance(response["Databases"], list)

    def test_describe_endpoints(self, timestream_write):
        response = timestream_write.describe_endpoints()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Endpoints" in response
        assert len(response["Endpoints"]) >= 1
        endpoint = response["Endpoints"][0]
        assert "Address" in endpoint
        assert "CachePeriodInMinutes" in endpoint

    def test_create_list_delete_database(self, timestream_write):
        db_name = f"db-{uuid.uuid4().hex[:8]}"

        # Create
        create_resp = timestream_write.create_database(DatabaseName=db_name)
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert create_resp["Database"]["DatabaseName"] == db_name
        assert "Arn" in create_resp["Database"]

        try:
            # List and verify present
            list_resp = timestream_write.list_databases()
            db_names = [db["DatabaseName"] for db in list_resp["Databases"]]
            assert db_name in db_names
        finally:
            # Delete
            delete_resp = timestream_write.delete_database(DatabaseName=db_name)
            assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify deleted
        list_resp = timestream_write.list_databases()
        db_names = [db["DatabaseName"] for db in list_resp["Databases"]]
        assert db_name not in db_names
