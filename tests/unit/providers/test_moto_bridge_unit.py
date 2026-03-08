"""Unit tests for moto_bridge internals."""

from unittest.mock import MagicMock

from robotocore.providers.moto_bridge import (
    _build_werkzeug_request,
    _get_moto_routing_table,
    _RegexConverter,
)


class TestRegexConverter:
    def test_default_regex(self):
        converter = _RegexConverter(MagicMock())
        assert converter.regex == ".*"

    def test_custom_regex(self):
        converter = _RegexConverter(MagicMock(), r"\d+")
        assert converter.regex == r"\d+"

    def test_not_part_isolating(self):
        assert _RegexConverter.part_isolating is False


def _mock_request(method="GET", path="/", query=None, headers=None):
    """Create a mock Starlette Request with proper scope for _build_werkzeug_request."""
    request = MagicMock()
    request.method = method
    request.url.path = path
    request.url.query = query
    request.headers = headers or {}
    # Provide ASGI scope with raw_path for correct percent-encoding handling
    request.scope = {"raw_path": path.encode("latin-1")}
    return request


class TestBuildWerkzeugRequest:
    def test_basic_get(self):
        request = _mock_request("GET", "/test-bucket", "list-type=2", {"Host": "localhost:4566"})
        wz_req = _build_werkzeug_request(request, b"")
        assert wz_req.method == "GET"
        assert wz_req.path == "/test-bucket"

    def test_post_with_body(self):
        request = _mock_request(
            "POST",
            "/",
            "",
            {"Host": "localhost:4566", "Content-Type": "application/x-amz-json-1.0"},
        )
        body = b'{"TableName": "test"}'
        wz_req = _build_werkzeug_request(request, body)
        assert wz_req.method == "POST"
        assert wz_req.get_data() == body

    def test_preserves_content_length(self):
        request = _mock_request(
            "PUT",
            "/bucket/key",
            None,
            {"Host": "localhost:4566", "Content-Length": "0"},
        )
        wz_req = _build_werkzeug_request(request, b"")
        assert wz_req.content_length is not None

    def test_no_query_string(self):
        request = _mock_request("GET", "/", None, {})
        wz_req = _build_werkzeug_request(request, b"")
        assert wz_req.query_string == b""


class TestGetMotoRoutingTable:
    def test_builds_routing_table_for_s3(self):
        url_map = _get_moto_routing_table("s3")
        rules = list(url_map.iter_rules())
        assert len(rules) > 0

    def test_builds_routing_table_for_sqs(self):
        url_map = _get_moto_routing_table("sqs")
        rules = list(url_map.iter_rules())
        assert len(rules) > 0

    def test_builds_routing_table_for_dynamodb(self):
        url_map = _get_moto_routing_table("dynamodb")
        rules = list(url_map.iter_rules())
        assert len(rules) > 0

    def test_caching(self):
        # Second call should return same object (lru_cache)
        map1 = _get_moto_routing_table("iam")
        map2 = _get_moto_routing_table("iam")
        assert map1 is map2
