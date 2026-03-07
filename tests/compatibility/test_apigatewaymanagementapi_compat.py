"""Compatibility tests for API Gateway Management API (@connections)."""

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def apigwmgmt():
    return make_client("apigatewaymanagementapi")


class TestAPIGatewayManagementAPI:
    """Tests for @connections API operations."""

    def test_get_connection_not_found(self, apigwmgmt):
        """Getting a non-existent connection returns 410."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            apigwmgmt.get_connection(ConnectionId="nonexistent-conn-id")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 410
