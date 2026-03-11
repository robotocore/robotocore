"""Unit tests for S3 bucket-specific CORS handling."""

from robotocore.gateway.cors import build_s3_cors_headers


class TestS3BucketCORS:
    """Tests for S3 bucket CORS rules applied to responses."""

    def _make_rule(
        self,
        origins: list[str] | None = None,
        methods: list[str] | None = None,
        headers: list[str] | None = None,
        expose: list[str] | None = None,
        max_age: int | None = None,
    ) -> dict:
        rule: dict = {}
        if origins is not None:
            rule["AllowedOrigins"] = origins
        if methods is not None:
            rule["AllowedMethods"] = methods
        if headers is not None:
            rule["AllowedHeaders"] = headers
        if expose is not None:
            rule["ExposeHeaders"] = expose
        if max_age is not None:
            rule["MaxAgeSeconds"] = max_age
        return rule

    def test_allowed_origins_matched(self):
        rules = [self._make_rule(origins=["http://example.com"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert headers["Access-Control-Allow-Origin"] == "http://example.com"

    def test_allowed_origins_not_matched(self):
        rules = [self._make_rule(origins=["http://example.com"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://other.com")
        assert headers == {}

    def test_allowed_methods_matched(self):
        rules = [self._make_rule(origins=["*"], methods=["GET", "PUT"])]
        headers = build_s3_cors_headers(rules, "http://example.com", request_method="GET")
        assert "GET" in headers["Access-Control-Allow-Methods"]
        assert "PUT" in headers["Access-Control-Allow-Methods"]

    def test_allowed_methods_not_matched(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://example.com", request_method="DELETE")
        assert headers == {}

    def test_allowed_headers_reflected(self):
        rules = [
            self._make_rule(
                origins=["*"], methods=["GET"], headers=["X-Custom-Header", "Content-Type"]
            )
        ]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert "X-Custom-Header" in headers["Access-Control-Allow-Headers"]
        assert "Content-Type" in headers["Access-Control-Allow-Headers"]

    def test_expose_headers_set(self):
        rules = [
            self._make_rule(origins=["*"], methods=["GET"], expose=["x-amz-request-id", "ETag"])
        ]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert "x-amz-request-id" in headers["Access-Control-Expose-Headers"]
        assert "ETag" in headers["Access-Control-Expose-Headers"]

    def test_max_age_seconds_set(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"], max_age=3600)]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert headers["Access-Control-Max-Age"] == "3600"

    def test_no_max_age_when_not_specified(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert "Access-Control-Max-Age" not in headers

    def test_vary_origin_always_set(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://example.com")
        assert headers.get("Vary") == "Origin"

    def test_no_origin_returns_empty(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, None)
        assert headers == {}

    def test_wildcard_origin_in_rule(self):
        rules = [self._make_rule(origins=["*"], methods=["GET"])]
        headers = build_s3_cors_headers(rules, "http://anything.com")
        assert headers["Access-Control-Allow-Origin"] == "http://anything.com"

    def test_multiple_rules_first_match_wins(self):
        rules = [
            self._make_rule(origins=["http://first.com"], methods=["GET"], max_age=100),
            self._make_rule(origins=["http://second.com"], methods=["GET"], max_age=200),
        ]
        headers = build_s3_cors_headers(rules, "http://second.com")
        assert headers["Access-Control-Max-Age"] == "200"

    def test_no_cors_config_default_used(self):
        """When bucket has no CORS rules (empty list), no S3 CORS headers are returned."""
        headers = build_s3_cors_headers([], "http://example.com")
        assert headers == {}
