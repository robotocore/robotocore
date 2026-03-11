"""Unit tests for S3 static website hosting."""

from robotocore.services.s3.website import (
    _check_redirect_rules,
    _s3_xml_error,
    guess_content_type,
    is_website_request,
    parse_website_host,
)


class TestParseWebsiteHost:
    """Tests for website host header parsing."""

    def test_default_website_hostname(self):
        """mybucket.s3-website.localhost.robotocore.cloud"""
        result = parse_website_host("mybucket.s3-website.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_website_hostname_alias(self):
        """mybucket.s3-website.localhost.localstack.cloud is accepted as compat alias."""
        result = parse_website_host("mybucket.s3-website.localhost.localstack.cloud")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_aws_website_hostname_dash(self):
        """mybucket.s3-website-us-east-1.amazonaws.com"""
        result = parse_website_host("mybucket.s3-website-us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"

    def test_aws_website_hostname_dot(self):
        """mybucket.s3-website.us-east-1.amazonaws.com"""
        result = parse_website_host("mybucket.s3-website.us-east-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-east-1"

    def test_eu_region(self):
        result = parse_website_host("mybucket.s3-website.eu-west-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "eu-west-1"

    def test_non_website_host_returns_none(self):
        assert parse_website_host("example.com") is None
        assert parse_website_host("mybucket.s3.amazonaws.com") is None
        assert parse_website_host("localhost:4566") is None

    def test_empty_host_returns_none(self):
        assert parse_website_host("") is None

    def test_host_with_port(self):
        result = parse_website_host("mybucket.s3-website.localhost.robotocore.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_host_with_port(self):
        """Port should be ignored for localstack.cloud compat alias."""
        result = parse_website_host("mybucket.s3-website.localhost.localstack.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"


class TestIsWebsiteRequest:
    """Tests for is_website_request ASGI scope detection."""

    def test_detects_website_request(self):
        scope = {
            "type": "http",
            "headers": [(b"host", b"mybucket.s3-website.localhost.robotocore.cloud")],
        }
        assert is_website_request(scope) is True

    def test_detects_localstack_alias_website_request(self):
        """localstack.cloud alias is also detected as a website request."""
        scope = {
            "type": "http",
            "headers": [(b"host", b"mybucket.s3-website.localhost.localstack.cloud")],
        }
        assert is_website_request(scope) is True

    def test_rejects_non_website(self):
        scope = {
            "type": "http",
            "headers": [(b"host", b"mybucket.s3.localhost.robotocore.cloud")],
        }
        assert is_website_request(scope) is False

    def test_rejects_non_http(self):
        scope = {
            "type": "websocket",
            "headers": [(b"host", b"mybucket.s3-website.localhost.robotocore.cloud")],
        }
        assert is_website_request(scope) is False


class TestGuessContentType:
    """Tests for Content-Type detection."""

    def test_html(self):
        assert guess_content_type("index.html") == "text/html"

    def test_htm(self):
        assert guess_content_type("page.htm") == "text/html"

    def test_css(self):
        assert guess_content_type("style.css") == "text/css"

    def test_javascript(self):
        assert guess_content_type("app.js") == "application/javascript"

    def test_json(self):
        assert guess_content_type("data.json") == "application/json"

    def test_png(self):
        assert guess_content_type("logo.png") == "image/png"

    def test_jpg(self):
        assert guess_content_type("photo.jpg") == "image/jpeg"

    def test_svg(self):
        assert guess_content_type("icon.svg") == "image/svg+xml"

    def test_pdf(self):
        assert guess_content_type("doc.pdf") == "application/pdf"

    def test_wasm(self):
        assert guess_content_type("module.wasm") == "application/wasm"

    def test_no_extension(self):
        assert guess_content_type("README") == "application/octet-stream"

    def test_trailing_slash(self):
        assert guess_content_type("dir/") == "text/html"

    def test_empty_key(self):
        assert guess_content_type("") == "text/html"

    def test_nested_path(self):
        assert guess_content_type("assets/css/main.css") == "text/css"


class TestCheckRedirectRules:
    """Tests for redirect rule matching."""

    def test_prefix_redirect(self):
        """Prefix-based redirect: /docs/ -> /documentation/"""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "docs/"},
                "Redirect": {"ReplaceKeyPrefixWith": "documentation/"},
            }
        ]
        result = _check_redirect_rules(rules, "docs/getting-started")
        assert result is not None
        assert result.status_code == 301
        assert "/documentation/getting-started" in result.headers.get("location", "")

    def test_key_redirect(self):
        """Key-based redirect: old-page.html -> new-page.html"""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "old-page.html"},
                "Redirect": {"ReplaceKeyWith": "new-page.html"},
            }
        ]
        result = _check_redirect_rules(rules, "old-page.html")
        assert result is not None
        assert result.status_code == 301
        assert "/new-page.html" in result.headers.get("location", "")

    def test_redirect_with_hostname(self):
        """Redirect to a different host."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "api/"},
                "Redirect": {
                    "HostName": "api.example.com",
                    "Protocol": "https",
                    "ReplaceKeyPrefixWith": "",
                },
            }
        ]
        result = _check_redirect_rules(rules, "api/v1/users")
        assert result is not None
        location = result.headers.get("location", "")
        assert "https://api.example.com/" in location

    def test_redirect_custom_status(self):
        """Redirect with custom HTTP status code."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "temp/"},
                "Redirect": {
                    "ReplaceKeyPrefixWith": "permanent/",
                    "HttpRedirectCode": "302",
                },
            }
        ]
        result = _check_redirect_rules(rules, "temp/file.txt")
        assert result is not None
        assert result.status_code == 302

    def test_error_code_redirect(self):
        """Redirect on 404 error."""
        rules = [
            {
                "Condition": {"HttpErrorCodeReturnedEquals": "404"},
                "Redirect": {"ReplaceKeyWith": "404.html"},
            }
        ]
        result = _check_redirect_rules(rules, "missing.html", http_error_code=404)
        assert result is not None
        assert "/404.html" in result.headers.get("location", "")

    def test_no_matching_rule(self):
        """No matching rule returns None."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "special/"},
                "Redirect": {"ReplaceKeyWith": "other"},
            }
        ]
        result = _check_redirect_rules(rules, "normal/file.txt")
        assert result is None

    def test_empty_rules(self):
        assert _check_redirect_rules([], "any-key") is None

    def test_prefix_and_error_condition(self):
        """Both prefix and error code must match."""
        rules = [
            {
                "Condition": {
                    "KeyPrefixEquals": "docs/",
                    "HttpErrorCodeReturnedEquals": "404",
                },
                "Redirect": {"ReplaceKeyWith": "docs/index.html"},
            }
        ]
        # Prefix matches but no error code
        assert _check_redirect_rules(rules, "docs/missing") is None
        # Both match
        result = _check_redirect_rules(rules, "docs/missing", http_error_code=404)
        assert result is not None


class TestS3XmlError:
    """Tests for XML error response generation."""

    def test_404_error(self):
        resp = _s3_xml_error("NoSuchKey", "Not found")
        assert resp.status_code == 404
        assert b"NoSuchKey" in resp.body
        assert b"Not found" in resp.body

    def test_custom_status(self):
        resp = _s3_xml_error("AccessDenied", "Forbidden", status_code=403)
        assert resp.status_code == 403
