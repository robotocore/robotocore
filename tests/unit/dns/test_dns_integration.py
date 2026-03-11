"""Integration-style tests for the DNS server.

These tests start a real UDP DNS server and query it via sockets,
verifying end-to-end behavior including packet parsing and serialization.
"""

import socket

import pytest
from dnslib import DNSRecord

from robotocore.dns.resolver import DEFAULT_TTL, get_config
from robotocore.dns.server import DNSServer


@pytest.fixture()
def dns_server():
    """Start a DNS server on a test port and yield it, then stop."""
    config = get_config()
    config["resolve_ip"] = "127.0.0.1"
    server = DNSServer(address="127.0.0.1", port=15360, config=config)
    server.start()
    yield server
    server.stop()


def _query(hostname: str, qtype: str = "A", port: int = 15360) -> DNSRecord:
    """Send a DNS query and return the parsed response."""
    q = DNSRecord.question(hostname, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3.0)
    try:
        sock.sendto(q.pack(), ("127.0.0.1", port))
        data, _ = sock.recvfrom(4096)
        return DNSRecord.parse(data)
    finally:
        sock.close()


class TestDNSQueryViaSocket:
    """Test actual DNS queries via UDP socket."""

    def test_a_query_returns_correct_ip(self, dns_server: DNSServer) -> None:
        response = _query("s3.amazonaws.com", "A")
        assert len(response.rr) == 1
        rr = response.rr[0]
        assert str(rr.rdata) == "127.0.0.1"

    def test_aaaa_query_returns_mapped_ipv6(self, dns_server: DNSServer) -> None:
        response = _query("s3.amazonaws.com", "AAAA")
        assert len(response.rr) == 1
        rr = response.rr[0]
        ip6 = str(rr.rdata)
        # Should contain "ffff" from the IPv4-mapped IPv6 address
        assert "ffff" in ip6.lower()

    def test_cname_query(self, dns_server: DNSServer) -> None:
        response = _query("sqs.amazonaws.com", "CNAME")
        assert len(response.rr) == 1
        rr = response.rr[0]
        assert "sqs.amazonaws.com" in str(rr.rdata)

    def test_non_aws_domain_forwarded(self, dns_server: DNSServer) -> None:
        """Query a non-AWS domain. The server should attempt upstream forwarding.

        We don't assert a specific IP (upstream may fail in CI), but verify
        we get a valid DNS response (not a crash).
        """
        response = _query("example.com", "A")
        # Should get a valid DNS response (might be SERVFAIL if upstream is unreachable)
        assert response.header.id is not None
        assert response.header.qr == 1  # It's a response

    def test_ttl_value_in_response(self, dns_server: DNSServer) -> None:
        response = _query("dynamodb.us-east-1.amazonaws.com", "A")
        assert len(response.rr) >= 1
        rr = response.rr[0]
        assert rr.ttl == DEFAULT_TTL

    def test_multiple_services(self, dns_server: DNSServer) -> None:
        """Query multiple AWS service hostnames."""
        for hostname in [
            "sqs.us-east-1.amazonaws.com",
            "kinesis.eu-west-1.amazonaws.com",
            "lambda.us-west-2.amazonaws.com",
        ]:
            response = _query(hostname, "A")
            assert len(response.rr) == 1, f"No answer for {hostname}"
            assert str(response.rr[0].rdata) == "127.0.0.1"


class TestMalformedPackets:
    """Test graceful handling of malformed DNS packets."""

    def test_garbage_data(self, dns_server: DNSServer) -> None:
        """Send garbage bytes -- server should not crash."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(3.0)
        try:
            sock.sendto(b"\x00\x01garbage data here", ("127.0.0.1", 15360))
            # Server may send a SERVFAIL or nothing at all
            try:
                data, _ = sock.recvfrom(4096)
                # If we get a response, it should be parseable
                response = DNSRecord.parse(data)
                assert response.header.rcode in (0, 2)  # NOERROR or SERVFAIL
            except TimeoutError:
                # No response is also acceptable for garbage input
                pass
        finally:
            sock.close()

    def test_empty_packet(self, dns_server: DNSServer) -> None:
        """Send an empty packet -- server should not crash."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2.0)
        try:
            sock.sendto(b"", ("127.0.0.1", 15360))
            try:
                data, _ = sock.recvfrom(4096)
            except TimeoutError:
                pass  # No response is fine
        finally:
            sock.close()

    def test_truncated_header(self, dns_server: DNSServer) -> None:
        """Send a truncated DNS header (less than 12 bytes)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2.0)
        try:
            sock.sendto(b"\x00\x01\x00\x00", ("127.0.0.1", 15360))
            try:
                data, _ = sock.recvfrom(4096)
            except TimeoutError:
                pass
        finally:
            sock.close()


class TestDNSConfigEndpoint:
    """Test the /_robotocore/dns/config management endpoint."""

    @pytest.fixture()
    def client(self):
        from starlette.testclient import TestClient

        from robotocore.gateway.app import app

        return TestClient(app, raise_server_exceptions=False)

    def test_dns_config_returns_200(self, client) -> None:
        response = client.get("/_robotocore/dns/config")
        assert response.status_code == 200

    def test_dns_config_has_dns_key(self, client) -> None:
        response = client.get("/_robotocore/dns/config")
        data = response.json()
        assert "dns" in data

    def test_dns_config_fields(self, client) -> None:
        response = client.get("/_robotocore/dns/config")
        dns = response.json()["dns"]
        assert "disabled" in dns
        assert "address" in dns
        assert "port" in dns
        assert "resolve_ip" in dns
        assert "upstream_server" in dns
        assert "local_patterns" in dns
        assert "upstream_patterns" in dns
        assert "ttl" in dns

    def test_dns_config_default_values(self, client) -> None:
        response = client.get("/_robotocore/dns/config")
        dns = response.json()["dns"]
        assert dns["resolve_ip"] == "127.0.0.1"
        assert dns["ttl"] == 300
        assert isinstance(dns["local_patterns"], list)
        assert any("amazonaws" in p for p in dns["local_patterns"])
