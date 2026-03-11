"""Comprehensive unit tests for robotocore.dns.resolver.

Tests cover: hostname matching, config parsing, A/AAAA/CNAME resolution,
edge cases, case sensitivity, and environment variable handling.
"""

import os
from unittest.mock import patch

from robotocore.dns.resolver import (
    _DEFAULT_LOCAL_PATTERNS,
    DEFAULT_TTL,
    get_config,
    resolve_a_record,
    resolve_aaaa_record,
    resolve_cname_record,
    should_resolve_locally,
)

# ---------------------------------------------------------------------------
# should_resolve_locally — pattern matching
# ---------------------------------------------------------------------------


class TestShouldResolveLocally:
    """Hostname-to-local resolution decisions."""

    def test_s3_subdomain(self) -> None:
        config = get_config()
        assert should_resolve_locally("s3.us-east-1.amazonaws.com", config) is True

    def test_deep_subdomain(self) -> None:
        config = get_config()
        assert should_resolve_locally("vpce-abc.s3.us-east-1.amazonaws.com", config) is True

    def test_case_insensitive_matching(self) -> None:
        config = get_config()
        assert should_resolve_locally("S3.US-EAST-1.AMAZONAWS.COM", config) is True

    def test_mixed_case_matching(self) -> None:
        config = get_config()
        assert should_resolve_locally("DynamoDB.Us-West-2.Amazonaws.Com", config) is True

    def test_trailing_dot_stripped(self) -> None:
        config = get_config()
        assert should_resolve_locally("s3.amazonaws.com.", config) is True

    def test_multiple_trailing_dots_stripped(self) -> None:
        config = get_config()
        # rstrip(".") removes all trailing dots
        assert should_resolve_locally("s3.amazonaws.com...", config) is True

    def test_non_aws_domain_returns_false(self) -> None:
        config = get_config()
        assert should_resolve_locally("google.com", config) is False

    def test_partial_amazonaws_not_matched(self) -> None:
        """A domain like 'fakeamazonaws.com' should NOT match."""
        config = get_config()
        # The pattern is .*\.amazonaws\.com$ — requires a dot before amazonaws
        assert should_resolve_locally("fakeamazonaws.com", config) is False

    def test_amazonaws_in_subdomain_not_matched(self) -> None:
        config = get_config()
        assert should_resolve_locally("amazonaws.com.evil.com", config) is False

    def test_empty_hostname(self) -> None:
        config = get_config()
        assert should_resolve_locally("", config) is False

    def test_bare_amazonaws_com(self) -> None:
        config = get_config()
        assert should_resolve_locally("amazonaws.com", config) is True

    def test_bare_aws_amazon_com(self) -> None:
        config = get_config()
        assert should_resolve_locally("aws.amazon.com", config) is True

    def test_subdomain_of_aws_amazon_com(self) -> None:
        config = get_config()
        assert should_resolve_locally("console.aws.amazon.com", config) is True

    def test_upstream_pattern_takes_priority(self) -> None:
        """Upstream bypass patterns are checked before local patterns."""
        config = get_config()
        config["upstream_patterns"] = [r".*\.amazonaws\.com"]
        assert should_resolve_locally("s3.amazonaws.com", config) is False

    def test_upstream_pattern_partial_match(self) -> None:
        """Upstream patterns use re.search, so partial matches work."""
        config = get_config()
        config["upstream_patterns"] = [r"us-gov"]
        assert should_resolve_locally("s3.us-gov-west-1.amazonaws.com", config) is False
        assert should_resolve_locally("s3.us-east-1.amazonaws.com", config) is True

    def test_empty_local_patterns_resolves_nothing(self) -> None:
        config = get_config()
        config["local_patterns"] = []
        config["upstream_patterns"] = []
        assert should_resolve_locally("s3.amazonaws.com", config) is False

    def test_uses_default_config_when_none(self) -> None:
        """When config=None, should_resolve_locally calls get_config()."""
        result = should_resolve_locally("s3.amazonaws.com")
        assert result is True

    def test_custom_local_pattern_only(self) -> None:
        config = {
            "local_patterns": [r".*\.mytest\.local$"],
            "upstream_patterns": [],
        }
        assert should_resolve_locally("api.mytest.local", config) is True
        assert should_resolve_locally("s3.amazonaws.com", config) is False


# ---------------------------------------------------------------------------
# get_config — environment variable parsing
# ---------------------------------------------------------------------------


class TestGetConfig:
    """Config from environment variables."""

    def _clean_env(self) -> dict[str, str]:
        """Return env dict with all DNS_ vars removed."""
        return {k: v for k, v in os.environ.items() if not k.startswith("DNS_")}

    def test_defaults_with_no_env(self) -> None:
        with patch.dict(os.environ, self._clean_env(), clear=True):
            config = get_config()
            assert config["resolve_ip"] == "127.0.0.1"
            assert config["port"] == 53
            assert config["address"] == "0.0.0.0"
            assert config["disabled"] is False
            assert config["upstream_server"] == ""
            assert config["upstream_patterns"] == []
            assert config["ttl"] == 300
            assert len(config["local_patterns"]) == len(_DEFAULT_LOCAL_PATTERNS)

    def test_dns_resolve_ip(self) -> None:
        with patch.dict(os.environ, {"DNS_RESOLVE_IP": "192.168.1.100"}):
            config = get_config()
            assert config["resolve_ip"] == "192.168.1.100"

    def test_dns_port_parsed_as_int(self) -> None:
        with patch.dict(os.environ, {"DNS_PORT": "5353"}):
            config = get_config()
            assert config["port"] == 5353
            assert isinstance(config["port"], int)

    def test_dns_disabled_only_when_1(self) -> None:
        with patch.dict(os.environ, {"DNS_DISABLED": "0"}):
            assert get_config()["disabled"] is False
        with patch.dict(os.environ, {"DNS_DISABLED": "true"}):
            assert get_config()["disabled"] is False  # Only "1" disables
        with patch.dict(os.environ, {"DNS_DISABLED": "1"}):
            assert get_config()["disabled"] is True

    def test_upstream_patterns_comma_separated(self) -> None:
        env = {"DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM": r"foo\.com , bar\.net"}
        with patch.dict(os.environ, env):
            config = get_config()
            assert config["upstream_patterns"] == [r"foo\.com", r"bar\.net"]

    def test_upstream_patterns_empty_string(self) -> None:
        env = {"DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM": ""}
        with patch.dict(os.environ, env):
            config = get_config()
            assert config["upstream_patterns"] == []

    def test_upstream_patterns_trailing_comma(self) -> None:
        env = {"DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM": r"foo\.com,"}
        with patch.dict(os.environ, env):
            config = get_config()
            assert config["upstream_patterns"] == [r"foo\.com"]

    def test_local_patterns_appended_to_defaults(self) -> None:
        env = {"DNS_LOCAL_NAME_PATTERNS": r".*\.custom\.dev"}
        with patch.dict(os.environ, env):
            config = get_config()
            assert len(config["local_patterns"]) == len(_DEFAULT_LOCAL_PATTERNS) + 1
            assert config["local_patterns"][-1] == r".*\.custom\.dev"

    def test_local_patterns_multiple(self) -> None:
        env = {"DNS_LOCAL_NAME_PATTERNS": r".*\.a,.*\.b,.*\.c"}
        with patch.dict(os.environ, env):
            config = get_config()
            assert len(config["local_patterns"]) == len(_DEFAULT_LOCAL_PATTERNS) + 3


# ---------------------------------------------------------------------------
# resolve_a_record
# ---------------------------------------------------------------------------


class TestResolveARecord:
    """A record resolution."""

    def test_aws_hostname_returns_configured_ip(self) -> None:
        config = get_config()
        config["resolve_ip"] = "10.0.0.42"
        assert resolve_a_record("sqs.amazonaws.com", config) == "10.0.0.42"

    def test_non_aws_returns_none(self) -> None:
        config = get_config()
        assert resolve_a_record("example.org", config) is None

    def test_uses_default_config_when_none(self) -> None:
        result = resolve_a_record("s3.amazonaws.com")
        assert result == "127.0.0.1"

    def test_returns_string_type(self) -> None:
        config = get_config()
        result = resolve_a_record("s3.amazonaws.com", config)
        assert isinstance(result, str)

    def test_ipv6_resolve_ip_still_returned_for_a_record(self) -> None:
        """If resolve_ip is IPv6, A record still returns it as-is."""
        config = get_config()
        config["resolve_ip"] = "::1"
        result = resolve_a_record("s3.amazonaws.com", config)
        assert result == "::1"


# ---------------------------------------------------------------------------
# resolve_aaaa_record
# ---------------------------------------------------------------------------


class TestResolveAAAARecord:
    """AAAA (IPv6) record resolution."""

    def test_ipv4_mapped_to_ipv6_format(self) -> None:
        config = get_config()
        config["resolve_ip"] = "127.0.0.1"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result is not None
        # IPv4-mapped IPv6: ::ffff:127.0.0.1 -> 0000:0000:0000:0000:0000:ffff:7f00:0001
        assert "ffff" in result.lower()
        # Verify it's colon-separated hex groups (no dotted quad)
        parts = result.split(":")
        assert len(parts) == 8
        for part in parts:
            assert len(part) == 4, f"Expected 4-char hex group, got '{part}'"
            int(part, 16)  # Must be valid hex

    def test_ipv4_mapped_specific_value(self) -> None:
        """Verify the exact mapped address for 127.0.0.1."""
        config = get_config()
        config["resolve_ip"] = "127.0.0.1"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result is not None
        # ::ffff:127.0.0.1 = ::ffff:7f00:0001
        assert result.endswith("ffff:7f00:0001")

    def test_ipv4_mapped_10_0_0_1(self) -> None:
        config = get_config()
        config["resolve_ip"] = "10.0.0.1"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result is not None
        # 10.0.0.1 -> 0a00:0001
        assert result.endswith("ffff:0a00:0001")

    def test_native_ipv6_returned_directly(self) -> None:
        config = get_config()
        config["resolve_ip"] = "::1"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result == "::1"

    def test_native_ipv6_full_address(self) -> None:
        config = get_config()
        config["resolve_ip"] = "fe80::1"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result == "fe80::1"

    def test_invalid_ip_returns_none(self) -> None:
        config = get_config()
        config["resolve_ip"] = "not-a-valid-ip"
        result = resolve_aaaa_record("s3.amazonaws.com", config)
        assert result is None

    def test_non_aws_hostname_returns_none(self) -> None:
        config = get_config()
        result = resolve_aaaa_record("google.com", config)
        assert result is None

    def test_uses_default_config_when_none(self) -> None:
        result = resolve_aaaa_record("s3.amazonaws.com")
        assert result is not None
        assert "ffff" in result.lower()


# ---------------------------------------------------------------------------
# resolve_cname_record
# ---------------------------------------------------------------------------


class TestResolveCNAMERecord:
    """CNAME record resolution."""

    def test_aws_hostname_returns_with_trailing_dot(self) -> None:
        config = get_config()
        result = resolve_cname_record("s3.amazonaws.com", config)
        assert result == "s3.amazonaws.com."

    def test_trailing_dot_input_normalized(self) -> None:
        config = get_config()
        result = resolve_cname_record("s3.amazonaws.com.", config)
        # rstrip(".") removes trailing dot, then "." is appended
        assert result == "s3.amazonaws.com."

    def test_non_aws_returns_none(self) -> None:
        config = get_config()
        result = resolve_cname_record("github.com", config)
        assert result is None

    def test_uses_default_config_when_none(self) -> None:
        result = resolve_cname_record("s3.amazonaws.com")
        assert result is not None
        assert result.endswith(".")

    def test_subdomain_preserved(self) -> None:
        config = get_config()
        result = resolve_cname_record("sqs.us-east-1.amazonaws.com", config)
        assert result == "sqs.us-east-1.amazonaws.com."


# ---------------------------------------------------------------------------
# DEFAULT_TTL constant
# ---------------------------------------------------------------------------


class TestDefaults:
    """Verify module-level defaults."""

    def test_default_ttl_is_300(self) -> None:
        assert DEFAULT_TTL == 300

    def test_default_local_patterns_count(self) -> None:
        assert len(_DEFAULT_LOCAL_PATTERNS) == 4

    def test_default_local_patterns_contain_amazonaws(self) -> None:
        assert any("amazonaws" in p for p in _DEFAULT_LOCAL_PATTERNS)

    def test_default_local_patterns_contain_aws_amazon(self) -> None:
        assert any("aws" in p and "amazon" in p for p in _DEFAULT_LOCAL_PATTERNS)
