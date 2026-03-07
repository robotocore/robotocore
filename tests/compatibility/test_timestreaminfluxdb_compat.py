"""Timestream InfluxDB compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def timestream_influxdb():
    return make_client("timestream-influxdb")


class TestTimestreamInfluxDBOperations:
    def test_list_db_instances(self, timestream_influxdb):
        response = timestream_influxdb.list_db_instances()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in response
