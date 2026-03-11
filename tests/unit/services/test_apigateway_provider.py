"""Unit tests for the API Gateway native provider."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from robotocore.services.apigateway.provider import handle_apigateway_request


def _make_request(method: str = "GET", path: str = "/"):
    request = MagicMock()
    request.body = AsyncMock(return_value=b"")
    request.headers = {}
    request.method = method
    request.url = MagicMock()
    request.url.path = path
    request.url.query = None
    return request


class TestAPIGatewayProvider:
    @patch("robotocore.services.apigateway.provider.forward_to_moto")
    def test_non_delete_model_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        request = _make_request("GET", "/restapis/abc123/resources")
        asyncio.run(handle_apigateway_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once_with(request, "apigateway", account_id="123456789012")

    @patch("moto.backends.get_backend")
    def test_delete_model_success(self, mock_get_backend):
        mock_rest_api = MagicMock()
        mock_rest_api.models = {"MyModel": MagicMock()}
        mock_backend = MagicMock()
        mock_backend.get_rest_api.return_value = mock_rest_api
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        request = _make_request("DELETE", "/restapis/abc123/models/MyModel")
        response = asyncio.run(handle_apigateway_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 202
        assert "MyModel" not in mock_rest_api.models

    @patch("moto.backends.get_backend")
    def test_delete_model_not_found(self, mock_get_backend):
        mock_rest_api = MagicMock()
        mock_rest_api.models = {}
        mock_backend = MagicMock()
        mock_backend.get_rest_api.return_value = mock_rest_api
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        request = _make_request("DELETE", "/restapis/abc123/models/Missing")
        response = asyncio.run(handle_apigateway_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 404

    @patch("moto.backends.get_backend")
    def test_delete_model_invalid_api(self, mock_get_backend):
        mock_backend = MagicMock()
        mock_backend.get_rest_api.side_effect = Exception("Not found")
        mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

        request = _make_request("DELETE", "/restapis/bad123/models/MyModel")
        response = asyncio.run(handle_apigateway_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 404

    @patch("robotocore.services.apigateway.provider.forward_to_moto")
    def test_delete_non_model_path_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        request = _make_request("DELETE", "/restapis/abc123/stages/test")
        asyncio.run(handle_apigateway_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once()
