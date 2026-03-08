"""Cost Explorer (CE) compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ce():
    return make_client("ce")


class TestCEOperations:
    def test_get_cost_and_usage(self, ce):
        response = ce.get_cost_and_usage(
            TimePeriod={"Start": "2024-01-01", "End": "2024-01-02"},
            Granularity="DAILY",
            Metrics=["BlendedCost"],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ResultsByTime" in response
        assert isinstance(response["ResultsByTime"], list)
