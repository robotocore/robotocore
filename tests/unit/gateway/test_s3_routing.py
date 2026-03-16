"""Unit tests for S3 virtual-hosted-style routing."""

import robotocore.gateway.s3_routing as s3_routing


class TestParseS3Vhost:
    """Tests for parse_s3_vhost host header parsing."""

    def test_default_hostname(self):
        """mybucket.s3.localhost.robotocore.cloud -> bucket=mybucket"""
        result = s3_routing.parse_s3_vhost("mybucket.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_hostname_alias(self):
        """mybucket.s3.localhost.localstack.cloud is accepted as compat alias."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.localhost.localstack.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_aws_region_hostname(self):
        """mybucket.s3.us-east-1.amazonaws.com -> bucket=mybucket, region=us-east-1"""
        result = s3_routing.parse_s3_vhost("mybucket.s3.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"

    def test_aws_global_hostname(self):
        """mybucket.s3.amazonaws.com -> bucket=mybucket"""
        result = s3_routing.parse_s3_vhost("mybucket.s3.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_non_s3_hostname_returns_none(self):
        """Regular hostname (not S3) -> returns None"""
        assert s3_routing.parse_s3_vhost("example.com") is None
        assert s3_routing.parse_s3_vhost("api.example.com") is None
        assert s3_routing.parse_s3_vhost("localhost") is None
        assert s3_routing.parse_s3_vhost("localhost:4566") is None

    def test_empty_host_returns_none(self):
        assert s3_routing.parse_s3_vhost("") is None

    def test_none_host_returns_none(self):
        """None input should return None without raising."""
        assert s3_routing.parse_s3_vhost(None) is None

    def test_host_with_port(self):
        """Port should be ignored when parsing (robotocore.cloud)."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.localhost.robotocore.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_host_with_port(self):
        """Port should be ignored when parsing (localstack.cloud compat alias)."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.localhost.localstack.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_s3_express_localhost_host(self):
        """S3 Express directory bucket via boto3 virtual-host on localhost."""
        result = s3_routing.parse_s3_vhost("mybucket--use1-az1--x-s3.localhost:4566")
        assert result is not None
        assert result["bucket"] == "mybucket--use1-az1--x-s3"

    def test_s3_object_lambda_route_token(self):
        """WriteGetObjectResponse route-token host on localhost."""
        result = s3_routing.parse_s3_vhost("my-route-token.localhost:4566")
        assert result is not None
        assert result["bucket"] == "my-route-token"

    def test_s3control_account_id_not_bucket(self):
        """S3 Control uses {AccountId}.localhost:{port} — NOT a bucket vhost."""
        result = s3_routing.parse_s3_vhost("123456789012.localhost:4566")
        assert result is None

    def test_eu_west_region(self):
        result = s3_routing.parse_s3_vhost("mybucket.s3.eu-west-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "eu-west-1"

    def test_ap_region(self):
        result = s3_routing.parse_s3_vhost("data-bucket.s3.ap-southeast-2.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "data-bucket"
        assert result["region"] == "ap-southeast-2"

    def test_sa_region(self):
        """South America region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.sa-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "sa-east-1"

    def test_ca_region(self):
        """Canada region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.ca-central-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "ca-central-1"

    def test_me_region(self):
        """Middle East region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.me-south-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "me-south-1"

    def test_af_region(self):
        """Africa region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.af-south-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "af-south-1"

    def test_il_region(self):
        """Israel region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.il-central-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "il-central-1"

    def test_custom_s3_hostname_env(self, monkeypatch):
        """S3_HOSTNAME env var changes the base hostname."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.custom.local")
        # Reset cached pattern so the new env var is picked up
        s3_routing._VHOST_CUSTOM_CACHE = None

        result = s3_routing.parse_s3_vhost("testbucket.s3.custom.local")
        assert result is not None
        assert result["bucket"] == "testbucket"

        # Clean up cached pattern for other tests
        s3_routing._VHOST_CUSTOM_CACHE = None

    def test_custom_hostname_caching(self, monkeypatch):
        """Custom hostname pattern is cached and reused."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.cached.local")
        s3_routing._VHOST_CUSTOM_CACHE = None

        # First call builds the cache
        pat1, base1 = s3_routing._get_custom_pattern()
        assert base1 == "s3.cached.local"

        # Second call returns same cached pattern
        pat2, base2 = s3_routing._get_custom_pattern()
        assert pat2 is pat1
        assert base2 == base1

        s3_routing._VHOST_CUSTOM_CACHE = None

    def test_custom_hostname_cache_invalidation(self, monkeypatch):
        """Cache is invalidated when S3_HOSTNAME changes."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.first.local")
        s3_routing._VHOST_CUSTOM_CACHE = None
        pat1, base1 = s3_routing._get_custom_pattern()
        assert base1 == "s3.first.local"

        monkeypatch.setenv("S3_HOSTNAME", "s3.second.local")
        pat2, base2 = s3_routing._get_custom_pattern()
        assert base2 == "s3.second.local"
        assert pat2 is not pat1

        s3_routing._VHOST_CUSTOM_CACHE = None

    def test_dualstack_hostname(self):
        """mybucket.s3.dualstack.us-east-1.amazonaws.com"""
        result = s3_routing.parse_s3_vhost("mybucket.s3.dualstack.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"

    def test_bucket_with_dots(self):
        """Bucket name containing dots (e.g., my.bucket.name)."""
        result = s3_routing.parse_s3_vhost("my.bucket.name.s3.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "my.bucket.name"

    def test_bucket_with_hyphens(self):
        """Bucket name with hyphens."""
        result = s3_routing.parse_s3_vhost("my-test-bucket.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "my-test-bucket"

    def test_bucket_all_numeric(self):
        """Bucket name that is all numeric."""
        result = s3_routing.parse_s3_vhost("123456789.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "123456789"

    def test_bucket_min_length(self):
        """S3 bucket names must be at least 3 characters. Regex requires 3+ chars."""
        result = s3_routing.parse_s3_vhost("abc.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "abc"

    def test_bucket_name_with_mixed_case(self):
        """Bucket names can have uppercase in the regex (AWS allows it in some contexts)."""
        result = s3_routing.parse_s3_vhost("MyBucket.s3.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "MyBucket"

    def test_aws_region_with_port(self):
        """AWS-style hostname with port."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.us-west-2.amazonaws.com:443")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-west-2"

    def test_global_hostname_no_region_key(self):
        """mybucket.s3.amazonaws.com should not have a region key."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.amazonaws.com")
        assert result is not None
        assert "region" not in result

    def test_dualstack_eu_region(self):
        """Dualstack with EU region."""
        result = s3_routing.parse_s3_vhost("mybucket.s3.dualstack.eu-central-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "eu-central-1"


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
        assert s3_routing.is_s3_vhost_request(scope) is True

    def test_detects_localstack_alias_vhost_request(self):
        """localstack.cloud alias is also detected as a vhost request."""
        scope = self._make_scope("mybucket.s3.localhost.localstack.cloud")
        assert s3_routing.is_s3_vhost_request(scope) is True

    def test_rejects_normal_request(self):
        scope = self._make_scope("localhost:4566")
        assert s3_routing.is_s3_vhost_request(scope) is False

    def test_rejects_non_http(self):
        scope = {
            "type": "websocket",
            "headers": [(b"host", b"mybucket.s3.localhost.robotocore.cloud")],
        }
        assert s3_routing.is_s3_vhost_request(scope) is False

    def test_rejects_no_host(self):
        scope = {"type": "http", "headers": []}
        assert s3_routing.is_s3_vhost_request(scope) is False

    def test_rejects_empty_host(self):
        """Empty host header should be rejected."""
        scope = {"type": "http", "headers": [(b"host", b"")]}
        assert s3_routing.is_s3_vhost_request(scope) is False

    def test_detects_aws_style_vhost(self):
        """AWS-style regional hostname is detected."""
        scope = self._make_scope("mybucket.s3.us-west-2.amazonaws.com")
        assert s3_routing.is_s3_vhost_request(scope) is True

    def test_missing_headers_key(self):
        """Scope with no headers key at all."""
        scope = {"type": "http"}
        assert s3_routing.is_s3_vhost_request(scope) is False

    def test_host_among_multiple_headers(self):
        """Host header found among other headers."""
        scope = {
            "type": "http",
            "headers": [
                (b"content-type", b"application/json"),
                (b"host", b"mybucket.s3.localhost.robotocore.cloud"),
                (b"accept", b"*/*"),
            ],
        }
        assert s3_routing.is_s3_vhost_request(scope) is True


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
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket"

    def test_rewrite_root_path_localstack_alias(self):
        """GET / on mybucket.s3.localhost.localstack.cloud -> /mybucket (compat alias)."""
        scope = self._make_scope("mybucket.s3.localhost.localstack.cloud", "/")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket"

    def test_rewrite_key_path(self):
        """GET /key.txt on mybucket.s3.localhost.robotocore.cloud -> /mybucket/key.txt"""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/key.txt"

    def test_rewrite_nested_key(self):
        """GET /prefix/key.txt -> /mybucket/prefix/key.txt"""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/prefix/key.txt")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/prefix/key.txt"

    def test_preserves_query_string(self):
        """Query string is preserved in the rewritten scope."""
        scope = self._make_scope(
            "mybucket.s3.localhost.robotocore.cloud", "/key.txt", b"versionId=123"
        )
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/key.txt"
        assert result["query_string"] == b"versionId=123"

    def test_preserves_headers(self):
        """Non-host headers are preserved; host is rewritten to strip bucket prefix."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        # Host is rewritten to strip the bucket-name prefix so Moto sees path-style.
        assert (b"host", b"s3.localhost.robotocore.cloud") in result["headers"]
        assert (b"content-type", b"text/plain") in result["headers"]

    def test_non_s3_host_returns_none(self):
        """Non-S3 host should return None."""
        scope = self._make_scope("example.com", "/key.txt")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is None

    def test_no_host_returns_none(self):
        scope = {"type": "http", "path": "/", "query_string": b"", "headers": []}
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is None

    def test_raw_path_without_query_string(self):
        """raw_path should be set correctly when there is no query string."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt", b"")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["raw_path"] == b"/mybucket/key.txt"

    def test_raw_path_with_query_string(self):
        """raw_path should include query string when present."""
        scope = self._make_scope(
            "mybucket.s3.localhost.robotocore.cloud", "/key.txt", b"acl&versionId=1"
        )
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["raw_path"] == b"/mybucket/key.txt?acl&versionId=1"

    def test_raw_path_root_no_query(self):
        """raw_path for root path with no query string."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/", b"")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["raw_path"] == b"/mybucket"

    def test_original_scope_not_mutated(self):
        """Rewrite should return a new dict, not mutate the original."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        original_path = scope["path"]
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        # Original should be unchanged
        assert scope["path"] == original_path
        assert result["path"] != original_path

    def test_preserves_method(self):
        """HTTP method is preserved in rewritten scope."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/key.txt")
        scope["method"] = "PUT"
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["method"] == "PUT"

    def test_deeply_nested_key(self):
        """Deeply nested object key."""
        scope = self._make_scope("mybucket.s3.localhost.robotocore.cloud", "/a/b/c/d/e/file.json")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/a/b/c/d/e/file.json"

    def test_key_with_special_chars(self):
        """Object key with URL-encoded special characters."""
        scope = self._make_scope(
            "mybucket.s3.localhost.robotocore.cloud", "/path%20with%20spaces/file.txt"
        )
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/path%20with%20spaces/file.txt"

    def test_rewrite_aws_regional_host(self):
        """Rewriting works with AWS regional hostnames too."""
        scope = self._make_scope("mybucket.s3.eu-west-1.amazonaws.com", "/data.csv")
        result = s3_routing.rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket/data.csv"


class TestGetS3RoutingConfig:
    """Tests for get_s3_routing_config endpoint data."""

    def test_returns_config_dict(self):
        config = s3_routing.get_s3_routing_config()
        assert "s3_hostname" in config
        assert config["virtual_hosted_style"] is True
        assert "supported_patterns" in config
        assert len(config["supported_patterns"]) > 0

    def test_default_hostname_is_robotocore(self):
        """Default hostname should be robotocore.cloud, not localstack.cloud."""
        config = s3_routing.get_s3_routing_config()
        assert config["s3_hostname"] == "s3.localhost.robotocore.cloud"
        assert config["website_hostname"] == "s3-website.s3.localhost.robotocore.cloud"

    def test_config_with_custom_hostname(self, monkeypatch):
        """Config reflects custom S3_HOSTNAME env var."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.mycompany.dev")
        s3_routing._VHOST_CUSTOM_CACHE = None

        config = s3_routing.get_s3_routing_config()
        assert config["s3_hostname"] == "s3.mycompany.dev"
        assert config["website_hostname"] == "s3-website.s3.mycompany.dev"

        s3_routing._VHOST_CUSTOM_CACHE = None

    def test_supported_patterns_format(self):
        """Supported patterns should contain placeholder tokens."""
        config = s3_routing.get_s3_routing_config()
        patterns = config["supported_patterns"]
        assert any("<bucket>" in p for p in patterns)
        assert any("<hostname>" in p for p in patterns)
        assert any("<region>" in p for p in patterns)

    def test_config_is_json_serializable(self):
        """Config should be JSON-serializable (all basic types)."""
        import json

        config = s3_routing.get_s3_routing_config()
        serialized = json.dumps(config)
        assert isinstance(serialized, str)
        deserialized = json.loads(serialized)
        assert deserialized == config


class TestGetS3Hostname:
    """Tests for _get_s3_hostname helper."""

    def test_default_value(self):
        """Without env var, returns default robotocore hostname."""
        assert s3_routing._get_s3_hostname() == "s3.localhost.robotocore.cloud"

    def test_custom_value(self, monkeypatch):
        """S3_HOSTNAME env var overrides the default."""
        monkeypatch.setenv("S3_HOSTNAME", "s3.example.local")
        assert s3_routing._get_s3_hostname() == "s3.example.local"


class TestModuleConstants:
    """Tests for module-level constants and patterns."""

    def test_default_hostname_constant(self):
        assert s3_routing.DEFAULT_S3_HOSTNAME == "s3.localhost.robotocore.cloud"

    def test_localstack_hostname_constant(self):
        assert s3_routing.S3_LOCALSTACK_HOSTNAME == "s3.localhost.localstack.cloud"

    def test_vhost_re_is_compiled(self):
        """Verify the main regex is pre-compiled."""
        import re

        assert isinstance(s3_routing._VHOST_RE, re.Pattern)

    def test_localstack_re_is_compiled(self):
        """Verify the localstack alias regex is pre-compiled."""
        import re

        assert isinstance(s3_routing._VHOST_LOCALSTACK_RE, re.Pattern)
