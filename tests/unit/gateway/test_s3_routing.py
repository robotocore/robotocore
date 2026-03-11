"""Unit tests for S3 virtual-hosted-style routing."""

from robotocore.gateway.s3_routing import (
    get_s3_routing_config,
    is_s3_vhost_request,
    parse_s3_vhost,
    rewrite_vhost_to_path,
)


class TestParseS3Vhost:
    """Tests for parse_s3_vhost host header parsing."""

    def test_default_hostname(self):
        """mybucket.s3.localhost.robotocore.cloud -> bucket=mybucket"""
        result = parse_s3_vhost("mybucket.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_hostname_alias(self):
        """mybucket.s3.localhost.localstack.cloud is accepted as compat alias."""
        result = parse_s3_vhost("mybucket.s3.localhost.localstack.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_aws_region_hostname(self):
        """mybucket.s3.us-east-1.amazonaws.com -> bucket=mybucket, region=us-east-1"""
        result = parse_s3_vhost("mybucket.s3.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"

    def test_aws_global_hostname(self):
        """mybucket.s3.amazonaws.com -> bucket=mybucket"""
        result = parse_s3_vhost("mybucket.s3.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_non_s3_hostname_returns_none(self):
        """Regular hostname (not S3) -> returns None"""
        assert parse_s3_vhost("example.com") is None
        assert parse_s3_vhost("api.example.com") is None
        assert parse_s3_vhost("localhost") is None
        assert parse_s3_vhost("localhost:4566") is None

    def test_empty_host_returns_none(self):
        assert parse_s3_vhost("") is None

    def test_host_with_port(self):
        """Port should be ignored when parsing (robotocore.cloud)."""
        result = parse_s3_vhost("mybucket.s3.localhost.robotocore.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_host_with_port(self):
        """Port should be ignored when parsing (localstack.cloud compat alias)."""
        result = parse_s3_vhost("mybucket.s3.localhost.localstack.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_eu_west_region(self):
        result = parse_s3_vhost("mybucket.s3.eu-west-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "eu-west-1"

    def test_ap_region(self):
        result = parse_s3_vhost("data-bucket.s3.ap-southeast-2.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "data-bucket"
        assert result["region"] == "ap-southeast-2"

    def test_custom_s3_hostname_env(self, monkeypatch):
        """S3_HOSTNAME env var changes the base hostname."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.custom.local")
        # Reset cached pattern
        import robotocore.gateway.s3_routing as mod

        mod._VHOST_CUSTOM_RE = None

        result = parse_s3_vhost("testbucket.s3.custom.local")
        assert result is not None
        assert result["bucket"] == "testbucket"

        # Clean up cached pattern for other tests
        mod._VHOST_CUSTOM_RE = None

    def test_dualstack_hostname(self):
        """mybucket.s3.dualstack.us-east-1.amazonaws.com"""
        result = parse_s3_vhost("mybucket.s3.dualstack.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"


class TestIsS3VhostRequest:
    """Tests for is_s3_vhost_request ASGI scope detection."""

    def _make_scope(self, host: str, path: str = "/") -> dict:
        headers = [(b"host", host.encode("latin-1"))]
        return {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": b"",
            "headers": headers,
        }

    def test_detects_vhost_request(self):
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud")
        assert is_s3_vhost_request(scope) is True

    def test_detects_localstack_alias_vhost_request(self):
        """localstack.cloud alias is also detected as a vhost request."""
        scope = self._make_scope("mybucket.s3.localhost.localstack.cloud")
        assert is_s3_vhost_request(scope) is True

    def test_rejects_normal_request(self):
        scope = self._make_scope("localhost:4566")
        assert is_s3_vhost_request(scope) is False

    def test_rejects_non_http(self):
        scope = {
            "type": "websocket",
            "headers": [(b"host", b"mybucket.s3.localhost.robotocore.cloud")],
        }
        assert is_s3_vhost_request(scope) is False

    def test_rejects_no_host(self):
        scope = {"type": "http", "headers": []}
        assert is_s3_vhost_request(scope) is False


class TestRewriteVhostToPath:
    """Tests for rewrite_vhost_to_path scope rewriting."""

    def _make_scope(self, host: str, path: str = "/", query: bytes = b"") -> dict:
        headers = [(b"host", host.encode("latin-1")), (b"content-type", b"text/plain")]
        return {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": query,
            "headers": headers,
        }

    def test_rewrite_root_path(self):
        """GET / on mybucket.s3.localhost.robotocore.cloud -> /mybucket"""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/")
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket"

    def test_rewrite_root_path_localstack_alias(self):
        """GET / on mybucket.s3.localhost.localstack.cloud -> /mybucket (compat alias)."""
        scope = self._make_scope("mybucket.s3.localhost.localstack.cloud", "/")
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket"

    def test_rewrite_key_path(self):
        """GET /key.txt on mybucket.s3.localhost.robotocore.cloud -> /mybucket/key.txt"""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/key.txt"

    def test_rewrite_nested_key(self):
        """GET /prefix/key.txt -> /mybucket/prefix/key.txt"""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/prefix/key.txt")
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/prefix/key.txt"

    def test_preserves_query_string(self):
        """Query string is preserved in the rewritten scope."""
        scope = self._make_scope(
            "mybucket.s3.localhost.robotocore.cloud", "/key.txt", b"versionId=123"
        )
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/key.txt"
        assert result["query_string"] == b"versionId=123"

    def test_preserves_headers(self):
        """All headers should be preserved after rewrite."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert (b"host", b"mybucket.s3.localhost.robotocore.cloud") in result["headers"]
        assert (b"content-type", b"text/plain") in result["headers"]

    def test_non_s3_host_returns_none(self):
        """Non-S3 host should return None."""
        scope = self._make_scope("example.com", "/key.txt")
        result = rewrite_vhost_to_path(scope)
        assert result is None

    def test_no_host_returns_none(self):
        scope = {"type": "http", "path": "/", "query_string": b"", "headers": []}
        result = rewrite_vhost_to_path(scope)
        assert result is None


class TestGetS3RoutingConfig:
    """Tests for get_s3_routing_config endpoint data."""

    def test_returns_config_dict(self):
        config = get_s3_routing_config()
        assert "s3_hostname" in config
        assert config["virtual_hosted_style"] is True
        assert "supported_patterns" in config
        assert len(config["supported_patterns"]) > 0

    def test_default_hostname_is_robotocore(self):
        """Default hostname should be robotocore.cloud, not localstack.cloud."""
        config = get_s3_routing_config()
        assert config["s3_hostname"] == "s3.localhost.robotocore.cloud"
        assert config["website_hostname"] == "s3-website.s3.localhost.robotocore.cloud"
