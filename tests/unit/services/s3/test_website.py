"""Unit tests for S3 static website hosting."""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

from robotocore.services.s3.website import (
    _check_redirect_rules,
    _get_website_config,
    _get_website_hostname,
    _parse_routing_rule,
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

    def test_ap_region(self):
        """Asia Pacific region."""
        result = parse_website_host("mybucket.s3-website.ap-northeast-1.amazonaws.com")
        assert result is not None
        assert result["bucket"] == "mybucket"
        assert result["region"] == "ap-northeast-1"

    def test_sa_region(self):
        """South America region."""
        result = parse_website_host("mybucket.s3-website.sa-east-1.amazonaws.com")
        assert result is not None
        assert result["region"] == "sa-east-1"

    def test_non_website_host_returns_none(self):
        assert parse_website_host("example.com") is None
        assert parse_website_host("mybucket.s3.amazonaws.com") is None
        assert parse_website_host("localhost:4566") is None

    def test_empty_host_returns_none(self):
        assert parse_website_host("") is None

    def test_none_host_returns_none(self):
        assert parse_website_host(None) is None

    def test_host_with_port(self):
        result = parse_website_host("mybucket.s3-website.localhost.robotocore.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_localstack_host_with_port(self):
        """Port should be ignored for localstack.cloud compat alias."""
        result = parse_website_host("mybucket.s3-website.localhost.localstack.cloud:4566")
        assert result is not None
        assert result["bucket"] == "mybucket"

    def test_no_region_when_not_aws(self):
        """Non-AWS website hostnames should not have region."""
        result = parse_website_host("mybucket.s3-website.localhost.robotocore.cloud")
        assert result is not None
        assert "region" not in result

    def test_bucket_with_hyphens(self):
        """Bucket name with hyphens in website hostname."""
        result = parse_website_host("my-web-site.s3-website.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "my-web-site"

    def test_bucket_with_dots(self):
        """Bucket name with dots in website hostname."""
        result = parse_website_host("my.site.com.s3-website.localhost.robotocore.cloud")
        assert result is not None
        assert result["bucket"] == "my.site.com"


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

    def test_rejects_no_headers(self):
        """Scope with empty headers list."""
        scope = {"type": "http", "headers": []}
        assert is_website_request(scope) is False

    def test_rejects_missing_headers_key(self):
        """Scope without headers key at all."""
        scope = {"type": "http"}
        assert is_website_request(scope) is False

    def test_rejects_plain_localhost(self):
        """Plain localhost is not a website request."""
        scope = {"type": "http", "headers": [(b"host", b"localhost:4566")]}
        assert is_website_request(scope) is False

    def test_host_among_other_headers(self):
        """Host found among multiple headers."""
        scope = {
            "type": "http",
            "headers": [
                (b"accept", b"text/html"),
                (b"host", b"mybucket.s3-website.localhost.robotocore.cloud"),
                (b"user-agent", b"curl/7.0"),
            ],
        }
        assert is_website_request(scope) is True


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

    def test_mjs(self):
        assert guess_content_type("module.mjs") == "application/javascript"

    def test_json(self):
        assert guess_content_type("data.json") == "application/json"

    def test_png(self):
        assert guess_content_type("logo.png") == "image/png"

    def test_jpg(self):
        assert guess_content_type("photo.jpg") == "image/jpeg"

    def test_jpeg(self):
        assert guess_content_type("photo.jpeg") == "image/jpeg"

    def test_gif(self):
        assert guess_content_type("animation.gif") == "image/gif"

    def test_svg(self):
        assert guess_content_type("icon.svg") == "image/svg+xml"

    def test_ico(self):
        assert guess_content_type("favicon.ico") == "image/x-icon"

    def test_webp(self):
        assert guess_content_type("image.webp") == "image/webp"

    def test_pdf(self):
        assert guess_content_type("doc.pdf") == "application/pdf"

    def test_wasm(self):
        assert guess_content_type("module.wasm") == "application/wasm"

    def test_xml(self):
        assert guess_content_type("config.xml") == "application/xml"

    def test_txt(self):
        assert guess_content_type("readme.txt") == "text/plain"

    def test_csv(self):
        assert guess_content_type("data.csv") == "text/csv"

    def test_zip(self):
        assert guess_content_type("archive.zip") == "application/zip"

    def test_gz(self):
        assert guess_content_type("archive.gz") == "application/gzip"

    def test_tar(self):
        assert guess_content_type("archive.tar") == "application/x-tar"

    def test_mp4(self):
        assert guess_content_type("video.mp4") == "video/mp4"

    def test_mp3(self):
        assert guess_content_type("audio.mp3") == "audio/mpeg"

    def test_woff2(self):
        assert guess_content_type("font.woff2") == "font/woff2"

    def test_woff(self):
        assert guess_content_type("font.woff") == "font/woff"

    def test_ttf(self):
        assert guess_content_type("font.ttf") == "font/ttf"

    def test_otf(self):
        assert guess_content_type("font.otf") == "font/otf"

    def test_eot(self):
        assert guess_content_type("font.eot") == "application/vnd.ms-fontobject"

    def test_no_extension(self):
        assert guess_content_type("README") == "application/octet-stream"

    def test_trailing_slash(self):
        assert guess_content_type("dir/") == "text/html"

    def test_empty_key(self):
        assert guess_content_type("") == "text/html"

    def test_nested_path(self):
        assert guess_content_type("assets/css/main.css") == "text/css"

    def test_case_insensitive_extension(self):
        """Extension matching should be case-insensitive."""
        assert guess_content_type("IMAGE.PNG") == "image/png"
        assert guess_content_type("style.CSS") == "text/css"

    def test_double_extension(self):
        """Only the last extension matters."""
        assert guess_content_type("backup.tar.gz") == "application/gzip"

    def test_dot_only(self):
        """File with just a dot and no extension text."""
        result = guess_content_type("file.")
        # After the dot there's nothing, so rfind returns the last position
        # empty ext after lower -> will try mimetypes which returns None
        assert isinstance(result, str)


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

    def test_redirect_hostname_without_protocol(self):
        """Redirect with hostname but no explicit protocol defaults to http."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "old/"},
                "Redirect": {
                    "HostName": "new.example.com",
                    "ReplaceKeyPrefixWith": "",
                },
            }
        ]
        result = _check_redirect_rules(rules, "old/page")
        assert result is not None
        location = result.headers.get("location", "")
        assert location.startswith("http://new.example.com/")

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

    def test_redirect_307_status(self):
        """Redirect with 307 Temporary Redirect."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "x/"},
                "Redirect": {
                    "ReplaceKeyPrefixWith": "y/",
                    "HttpRedirectCode": "307",
                },
            }
        ]
        result = _check_redirect_rules(rules, "x/data")
        assert result is not None
        assert result.status_code == 307

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

    def test_error_code_redirect_403(self):
        """Redirect on 403 error."""
        rules = [
            {
                "Condition": {"HttpErrorCodeReturnedEquals": "403"},
                "Redirect": {"ReplaceKeyWith": "access-denied.html"},
            }
        ]
        result = _check_redirect_rules(rules, "secret.html", http_error_code=403)
        assert result is not None
        assert "/access-denied.html" in result.headers.get("location", "")

    def test_error_code_no_match_different_code(self):
        """Error code redirect should not match a different error code."""
        rules = [
            {
                "Condition": {"HttpErrorCodeReturnedEquals": "404"},
                "Redirect": {"ReplaceKeyWith": "404.html"},
            }
        ]
        result = _check_redirect_rules(rules, "file.html", http_error_code=500)
        assert result is None

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

    def test_empty_condition_matches_all(self):
        """A rule with empty condition should match any key."""
        rules = [
            {
                "Condition": {},
                "Redirect": {"ReplaceKeyWith": "index.html"},
            }
        ]
        result = _check_redirect_rules(rules, "anything")
        assert result is not None
        assert "/index.html" in result.headers.get("location", "")

    def test_first_matching_rule_wins(self):
        """When multiple rules match, the first one is used."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "a/"},
                "Redirect": {"ReplaceKeyWith": "first.html"},
            },
            {
                "Condition": {"KeyPrefixEquals": "a/"},
                "Redirect": {"ReplaceKeyWith": "second.html"},
            },
        ]
        result = _check_redirect_rules(rules, "a/page")
        assert result is not None
        assert "/first.html" in result.headers.get("location", "")

    def test_redirect_preserves_suffix_after_prefix(self):
        """When replacing prefix, suffix of the key is preserved."""
        rules = [
            {
                "Condition": {"KeyPrefixEquals": "old/"},
                "Redirect": {"ReplaceKeyPrefixWith": "new/"},
            }
        ]
        result = _check_redirect_rules(rules, "old/path/to/file.txt")
        assert result is not None
        location = result.headers.get("location", "")
        assert "/new/path/to/file.txt" in location

    def test_redirect_no_condition_key(self):
        """Rule with missing Condition key still works (defaults to empty dict)."""
        rules = [
            {
                "Redirect": {"ReplaceKeyWith": "fallback.html"},
            }
        ]
        result = _check_redirect_rules(rules, "any-key")
        assert result is not None
        assert "/fallback.html" in result.headers.get("location", "")


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

    def test_xml_structure(self):
        """Response body is valid XML with Error, Code, and Message elements."""
        resp = _s3_xml_error("TestCode", "Test message", status_code=500)
        root = ET.fromstring(resp.body)
        assert root.tag == "Error"
        assert root.find("Code").text == "TestCode"
        assert root.find("Message").text == "Test message"

    def test_content_type_is_xml(self):
        """Response media type should be application/xml."""
        resp = _s3_xml_error("NoSuchKey", "Not found")
        assert resp.media_type == "application/xml"

    def test_xml_declaration_present(self):
        """Response body starts with XML declaration."""
        resp = _s3_xml_error("NoSuchKey", "Not found")
        assert resp.body.startswith(b'<?xml version="1.0" encoding="UTF-8"?>')


class TestGetWebsiteHostname:
    """Tests for _get_website_hostname helper."""

    def test_default_hostname(self):
        hostname = _get_website_hostname()
        assert hostname == "s3-website.s3.localhost.robotocore.cloud"

    def test_custom_hostname(self, monkeypatch):
        monkeypatch.setenv("S3_HOSTNAME", "s3.mydev.local")
        hostname = _get_website_hostname()
        assert hostname == "s3-website.s3.mydev.local"


class TestGetWebsiteConfig:
    """Tests for _get_website_config XML parsing."""

    def test_none_website_configuration(self):
        """Bucket with no website_configuration attribute returns None."""
        bucket = MagicMock()
        bucket.website_configuration = None
        assert _get_website_config(bucket) is None

    def test_empty_string_website_configuration(self):
        """Empty string website_configuration returns None."""
        bucket = MagicMock()
        bucket.website_configuration = ""
        assert _get_website_config(bucket) is None

    def test_empty_bytes_website_configuration(self):
        """Empty bytes website_configuration returns None."""
        bucket = MagicMock()
        bucket.website_configuration = b""
        assert _get_website_config(bucket) is None

    def test_whitespace_only_website_configuration(self):
        """Whitespace-only string returns None."""
        bucket = MagicMock()
        bucket.website_configuration = "   \n\t  "
        assert _get_website_config(bucket) is None

    def test_malformed_xml_returns_none(self):
        """Malformed XML returns None without raising."""
        bucket = MagicMock()
        bucket.website_configuration = b"<not valid xml"
        assert _get_website_config(bucket) is None

    def test_dict_passthrough(self):
        """If website_configuration is already a dict, return it as-is."""
        bucket = MagicMock()
        config = {"IndexDocument": {"Suffix": "index.html"}}
        bucket.website_configuration = config
        assert _get_website_config(bucket) is config

    def test_parse_index_document_namespaced(self):
        """Parse namespaced XML with IndexDocument."""
        ns = "http://s3.amazonaws.com/doc/2006-03-01/"
        xml = (
            f'<WebsiteConfiguration xmlns="{ns}">'
            f"<IndexDocument><Suffix>index.html</Suffix></IndexDocument>"
            f"</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml.encode("utf-8")
        config = _get_website_config(bucket)
        assert config is not None
        assert config["IndexDocument"]["Suffix"] == "index.html"

    def test_parse_index_document_non_namespaced(self):
        """Parse non-namespaced XML with IndexDocument."""
        xml = (
            "<WebsiteConfiguration>"
            "<IndexDocument><Suffix>default.html</Suffix></IndexDocument>"
            "</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert config["IndexDocument"]["Suffix"] == "default.html"

    def test_parse_error_document(self):
        """Parse XML with ErrorDocument."""
        xml = (
            "<WebsiteConfiguration>"
            "<IndexDocument><Suffix>index.html</Suffix></IndexDocument>"
            "<ErrorDocument><Key>error.html</Key></ErrorDocument>"
            "</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert config["ErrorDocument"]["Key"] == "error.html"

    def test_parse_routing_rules(self):
        """Parse XML with RoutingRules."""
        xml = (
            "<WebsiteConfiguration>"
            "<IndexDocument><Suffix>index.html</Suffix></IndexDocument>"
            "<RoutingRules>"
            "<RoutingRule>"
            "<Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>"
            "<Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect>"
            "</RoutingRule>"
            "</RoutingRules>"
            "</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert len(config["RoutingRules"]) == 1
        rule = config["RoutingRules"][0]
        assert rule["Condition"]["KeyPrefixEquals"] == "docs/"
        assert rule["Redirect"]["ReplaceKeyPrefixWith"] == "documents/"

    def test_parse_multiple_routing_rules(self):
        """Parse XML with multiple RoutingRules."""
        xml = (
            "<WebsiteConfiguration>"
            "<IndexDocument><Suffix>index.html</Suffix></IndexDocument>"
            "<RoutingRules>"
            "<RoutingRule>"
            "<Condition><KeyPrefixEquals>a/</KeyPrefixEquals></Condition>"
            "<Redirect><ReplaceKeyPrefixWith>b/</ReplaceKeyPrefixWith></Redirect>"
            "</RoutingRule>"
            "<RoutingRule>"
            "<Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>"
            "<Redirect><ReplaceKeyWith>404.html</ReplaceKeyWith></Redirect>"
            "</RoutingRule>"
            "</RoutingRules>"
            "</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert len(config["RoutingRules"]) == 2

    def test_no_index_document_in_xml(self):
        """XML with no IndexDocument returns empty dict for it."""
        xml = "<WebsiteConfiguration></WebsiteConfiguration>"
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert config["IndexDocument"] == {}
        assert config["ErrorDocument"] == {}
        assert config["RoutingRules"] == []

    def test_missing_attribute_returns_none(self):
        """Bucket object without website_configuration attribute returns None."""
        bucket = MagicMock(spec=[])  # no attributes
        assert _get_website_config(bucket) is None

    def test_str_xml_input(self):
        """String (not bytes) XML is handled correctly."""
        xml = (
            "<WebsiteConfiguration>"
            "<IndexDocument><Suffix>home.html</Suffix></IndexDocument>"
            "</WebsiteConfiguration>"
        )
        bucket = MagicMock()
        bucket.website_configuration = xml
        config = _get_website_config(bucket)
        assert config is not None
        assert config["IndexDocument"]["Suffix"] == "home.html"


class TestParseRoutingRule:
    """Tests for _parse_routing_rule XML element parsing."""

    def test_parse_condition_and_redirect(self):
        """Parse a complete routing rule element."""
        xml = (
            "<RoutingRule>"
            "<Condition><KeyPrefixEquals>old/</KeyPrefixEquals></Condition>"
            "<Redirect><ReplaceKeyPrefixWith>new/</ReplaceKeyPrefixWith></Redirect>"
            "</RoutingRule>"
        )
        el = ET.fromstring(xml)
        rule = _parse_routing_rule(el)
        assert rule["Condition"]["KeyPrefixEquals"] == "old/"
        assert rule["Redirect"]["ReplaceKeyPrefixWith"] == "new/"

    def test_parse_error_condition(self):
        """Parse rule with HttpErrorCodeReturnedEquals."""
        xml = (
            "<RoutingRule>"
            "<Condition>"
            "<HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>"
            "</Condition>"
            "<Redirect><ReplaceKeyWith>error.html</ReplaceKeyWith></Redirect>"
            "</RoutingRule>"
        )
        el = ET.fromstring(xml)
        rule = _parse_routing_rule(el)
        assert rule["Condition"]["HttpErrorCodeReturnedEquals"] == "404"
        assert rule["Redirect"]["ReplaceKeyWith"] == "error.html"

    def test_parse_redirect_with_all_fields(self):
        """Parse redirect with Protocol, HostName, HttpRedirectCode."""
        xml = (
            "<RoutingRule>"
            "<Condition><KeyPrefixEquals>api/</KeyPrefixEquals></Condition>"
            "<Redirect>"
            "<Protocol>https</Protocol>"
            "<HostName>api.example.com</HostName>"
            "<ReplaceKeyPrefixWith>v2/</ReplaceKeyPrefixWith>"
            "<HttpRedirectCode>302</HttpRedirectCode>"
            "</Redirect>"
            "</RoutingRule>"
        )
        el = ET.fromstring(xml)
        rule = _parse_routing_rule(el)
        assert rule["Redirect"]["Protocol"] == "https"
        assert rule["Redirect"]["HostName"] == "api.example.com"
        assert rule["Redirect"]["ReplaceKeyPrefixWith"] == "v2/"
        assert rule["Redirect"]["HttpRedirectCode"] == "302"

    def test_parse_empty_rule(self):
        """Parse rule with no Condition or Redirect children."""
        xml = "<RoutingRule></RoutingRule>"
        el = ET.fromstring(xml)
        rule = _parse_routing_rule(el)
        assert rule["Condition"] == {}
        assert rule["Redirect"] == {}

    def test_parse_namespaced_rule(self):
        """Parse rule with S3 namespace."""
        ns = "http://s3.amazonaws.com/doc/2006-03-01/"
        xml = (
            f'<RoutingRule xmlns="{ns}">'
            f"<Condition><KeyPrefixEquals>x/</KeyPrefixEquals></Condition>"
            f"<Redirect><ReplaceKeyPrefixWith>y/</ReplaceKeyPrefixWith></Redirect>"
            f"</RoutingRule>"
        )
        el = ET.fromstring(xml)
        rule = _parse_routing_rule(el)
        assert rule["Condition"]["KeyPrefixEquals"] == "x/"
        assert rule["Redirect"]["ReplaceKeyPrefixWith"] == "y/"
