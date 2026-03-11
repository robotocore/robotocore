"""Unit tests for the DNS server and resolver."""

import os
import socket
import threading
from unittest.mock import patch

from robotocore.dns.resolver import (
    DEFAULT_TTL,
    get_config,
    resolve_a_record,
    resolve_aaaa_record,
    resolve_cname_record,
    should_resolve_locally,
)
from robotocore.dns.server import DNSServer, start_dns_server

# ---------------------------------------------------------------------------
# Resolver: local resolution of AWS hostnames
# ---------------------------------------------------------------------------


class TestResolverLocalResolution:
    """Test that AWS hostnames resolve to the configured local IP."""

    def test_sqs_resolves_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("sqs.us-east-1.amazonaws.com", config)
        assert ip == "127.0.0.1"

    def test_s3_resolves_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("s3.amazonaws.com", config)
        assert ip == "127.0.0.1"

    def test_dynamodb_resolves_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("dynamodb.us-west-2.amazonaws.com", config)
        assert ip == "127.0.0.1"

    def test_bare_amazonaws_resolves_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("amazonaws.com", config)
        assert ip == "127.0.0.1"

    def test_aws_amazon_com_resolves_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("console.aws.amazon.com", config)
        assert ip == "127.0.0.1"

    def test_trailing_dot_handled(self) -> None:
        config = get_config()
        ip = resolve_a_record("s3.amazonaws.com.", config)
        assert ip == "127.0.0.1"


class TestResolverUpstreamForwarding:
    """Test that non-AWS hostnames are forwarded upstream."""

    def test_github_not_resolved_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("github.com", config)
        assert ip is None

    def test_google_not_resolved_locally(self) -> None:
        config = get_config()
        ip = resolve_a_record("google.com", config)
        assert ip is None

    def test_random_domain_not_resolved_locally(self) -> None:
        config = get_config()
        assert should_resolve_locally("example.org", config) is False


class TestResolverUpstreamPatterns:
    """Test DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM."""

    def test_upstream_pattern_overrides_local(self) -> None:
        config = get_config()
        config["upstream_patterns"] = [r"s3\.amazonaws\.com"]
        # s3.amazonaws.com should be forwarded upstream, not resolved locally
        assert should_resolve_locally("s3.amazonaws.com", config) is False

    def test_upstream_pattern_does_not_affect_other_services(self) -> None:
        config = get_config()
        config["upstream_patterns"] = [r"s3\.amazonaws\.com"]
        # SQS should still resolve locally
        assert should_resolve_locally("sqs.us-east-1.amazonaws.com", config) is True

    def test_upstream_pattern_regex(self) -> None:
        config = get_config()
        config["upstream_patterns"] = [r".*\.us-gov-.*\.amazonaws\.com"]
        assert should_resolve_locally("s3.us-gov-west-1.amazonaws.com", config) is False
        assert should_resolve_locally("s3.us-east-1.amazonaws.com", config) is True


class TestResolverLocalPatterns:
    """Test DNS_LOCAL_NAME_PATTERNS for custom domains."""

    def test_custom_local_pattern(self) -> None:
        config = get_config()
        config["local_patterns"] = config["local_patterns"] + [r".*\.mycompany\.internal$"]
        assert should_resolve_locally("api.mycompany.internal", config) is True

    def test_custom_local_pattern_coexists_with_aws(self) -> None:
        config = get_config()
        config["local_patterns"] = config["local_patterns"] + [r".*\.local$"]
        assert should_resolve_locally("sqs.us-east-1.amazonaws.com", config) is True
        assert should_resolve_locally("myapp.local", config) is True


class TestARecord:
    """Test A record response format."""

    def test_a_record_returns_ip_string(self) -> None:
        config = get_config()
        ip = resolve_a_record("s3.amazonaws.com", config)
        assert isinstance(ip, str)
        assert ip == "127.0.0.1"

    def test_a_record_custom_ip(self) -> None:
        config = get_config()
        config["resolve_ip"] = "10.0.0.1"
        ip = resolve_a_record("s3.amazonaws.com", config)
        assert ip == "10.0.0.1"

    def test_a_record_none_for_upstream(self) -> None:
        config = get_config()
        ip = resolve_a_record("github.com", config)
        assert ip is None


class TestAAAARecord:
    """Test AAAA record response."""

    def test_aaaa_maps_ipv4_to_ipv6(self) -> None:
        config = get_config()
        config["resolve_ip"] = "127.0.0.1"
        ip6 = resolve_aaaa_record("s3.amazonaws.com", config)
        assert ip6 is not None
        assert "ffff" in ip6.lower()
        # Should be a valid IPv6 address (pure hex groups, no dotted-quad)
        assert ":" in ip6

    def test_aaaa_native_ipv6(self) -> None:
        config = get_config()
        config["resolve_ip"] = "::1"
        ip6 = resolve_aaaa_record("s3.amazonaws.com", config)
        assert ip6 == "::1"

    def test_aaaa_none_for_upstream(self) -> None:
        config = get_config()
        ip6 = resolve_aaaa_record("github.com", config)
        assert ip6 is None

    def test_aaaa_empty_for_invalid_ip(self) -> None:
        config = get_config()
        config["resolve_ip"] = "not-an-ip"
        ip6 = resolve_aaaa_record("s3.amazonaws.com", config)
        assert ip6 is None


class TestCNAMERecord:
    """Test CNAME record resolution."""

    def test_cname_returns_hostname_with_dot(self) -> None:
        config = get_config()
        target = resolve_cname_record("s3.amazonaws.com", config)
        assert target is not None
        assert target.endswith(".")
        assert "s3.amazonaws.com" in target

    def test_cname_none_for_upstream(self) -> None:
        config = get_config()
        target = resolve_cname_record("github.com", config)
        assert target is None


# ---------------------------------------------------------------------------
# DNS Server lifecycle
# ---------------------------------------------------------------------------


class TestDNSServerLifecycle:
    """Test DNS server start/stop behavior."""

    def test_server_starts_and_stops_cleanly(self) -> None:
        server = DNSServer(address="127.0.0.1", port=0, config=get_config())
        # Port 0 = OS assigns a free port; but DNSServer binds to specific port.
        # Use a high port to avoid conflicts.
        server = DNSServer(address="127.0.0.1", port=15353, config=get_config())
        server.start()
        assert server.is_running is True
        server.stop()
        assert server.is_running is False

    def test_server_handles_concurrent_queries(self) -> None:
        """Start server, send multiple queries concurrently."""
        from dnslib import DNSRecord

        config = get_config()
        config["resolve_ip"] = "127.0.0.1"
        server = DNSServer(address="127.0.0.1", port=15354, config=config)
        server.start()

        errors: list[str] = []

        def query_dns(hostname: str) -> None:
            try:
                q = DNSRecord.question(hostname)
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(3.0)
                try:
                    sock.sendto(q.pack(), ("127.0.0.1", 15354))
                    data, _ = sock.recvfrom(4096)
                    response = DNSRecord.parse(data)
                    if len(response.rr) == 0:
                        errors.append(f"No answers for {hostname}")
                finally:
                    sock.close()
            except Exception as e:
                errors.append(f"Error querying {hostname}: {e}")

        threads = []
        for host in [
            "s3.amazonaws.com",
            "sqs.us-east-1.amazonaws.com",
            "dynamodb.us-west-2.amazonaws.com",
            "ec2.amazonaws.com",
            "lambda.us-east-1.amazonaws.com",
        ]:
            t = threading.Thread(target=query_dns, args=(host,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=5)

        server.stop()
        assert errors == [], f"DNS query errors: {errors}"

    def test_server_with_custom_port(self) -> None:
        config = get_config()
        server = DNSServer(address="127.0.0.1", port=15355, config=config)
        server.start()
        assert server.is_running is True
        assert server.port == 15355
        server.stop()

    def test_double_start_is_idempotent(self) -> None:
        config = get_config()
        server = DNSServer(address="127.0.0.1", port=15356, config=config)
        server.start()
        server.start()  # Should not raise
        assert server.is_running is True
        server.stop()

    def test_double_stop_is_idempotent(self) -> None:
        config = get_config()
        server = DNSServer(address="127.0.0.1", port=15357, config=config)
        server.start()
        server.stop()
        server.stop()  # Should not raise
        assert server.is_running is False


class TestDNSDisabled:
    """Test DNS disabled via env var."""

    def test_dns_disabled_returns_none(self) -> None:
        with patch.dict(os.environ, {"DNS_DISABLED": "1"}):
            result = start_dns_server()
            assert result is None

    def test_dns_disabled_config(self) -> None:
        with patch.dict(os.environ, {"DNS_DISABLED": "1"}):
            config = get_config()
            assert config["disabled"] is True

    def test_dns_enabled_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            # Remove DNS_DISABLED if set
            env = os.environ.copy()
            env.pop("DNS_DISABLED", None)
            with patch.dict(os.environ, env, clear=True):
                config = get_config()
                assert config["disabled"] is False


class TestDefaultConfig:
    """Test default configuration values."""

    def test_default_resolve_ip(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            env = {k: v for k, v in os.environ.items() if not k.startswith("DNS_")}
            with patch.dict(os.environ, env, clear=True):
                config = get_config()
                assert config["resolve_ip"] == "127.0.0.1"

    def test_default_port(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            env = {k: v for k, v in os.environ.items() if not k.startswith("DNS_")}
            with patch.dict(os.environ, env, clear=True):
                config = get_config()
                assert config["port"] == 53

    def test_default_address(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            env = {k: v for k, v in os.environ.items() if not k.startswith("DNS_")}
            with patch.dict(os.environ, env, clear=True):
                config = get_config()
                assert config["address"] == "0.0.0.0"

    def test_default_ttl(self) -> None:
        config = get_config()
        assert config["ttl"] == DEFAULT_TTL
        assert config["ttl"] == 300

    def test_custom_env_vars(self) -> None:
        env = {
            "DNS_RESOLVE_IP": "10.0.0.5",
            "DNS_PORT": "5353",
            "DNS_ADDRESS": "192.168.1.1",
            "DNS_SERVER": "8.8.4.4",
        }
        with patch.dict(os.environ, env):
            config = get_config()
            assert config["resolve_ip"] == "10.0.0.5"
            assert config["port"] == 5353
            assert config["address"] == "192.168.1.1"
            assert config["upstream_server"] == "8.8.4.4"

    def test_local_patterns_env_var(self) -> None:
        env = {"DNS_LOCAL_NAME_PATTERNS": r".*\.local,.*\.internal"}
        with patch.dict(os.environ, env):
            config = get_config()
            # Should include default patterns plus custom ones
            assert any("amazonaws" in p for p in config["local_patterns"])
            assert r".*\.local" in config["local_patterns"]
            assert r".*\.internal" in config["local_patterns"]

    def test_upstream_patterns_env_var(self) -> None:
        env = {"DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM": r"github\.com,gitlab\.com"}
        with patch.dict(os.environ, env):
            config = get_config()
            assert r"github\.com" in config["upstream_patterns"]
            assert r"gitlab\.com" in config["upstream_patterns"]
