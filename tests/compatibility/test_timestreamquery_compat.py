"""Timestream Query compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def timestream_query():
    return make_client("timestream-query")


class TestTimestreamQueryOperations:
    def test_describe_endpoints(self, timestream_query):
        response = timestream_query.describe_endpoints()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Endpoints" in response
        assert len(response["Endpoints"]) >= 1
        endpoint = response["Endpoints"][0]
        assert "Address" in endpoint
        assert "CachePeriodInMinutes" in endpoint
