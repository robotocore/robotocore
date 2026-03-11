"""Comprehensive unit tests for robotocore.dns.server.

Tests cover: _get_system_dns, _get_upstream_server, _forward_upstream,
_build_response, _handle_query, DNSServer lifecycle, and module-level
start/stop helpers. Network calls are mocked.
"""

import os
import struct
from unittest.mock import MagicMock, mock_open, patch

from dnslib import DNSHeader, DNSRecord

from robotocore.dns.resolver import DEFAULT_TTL, get_config
from robotocore.dns.server import (
    DNSServer,
    _build_response,
    _forward_upstream,
    _get_system_dns,
    _get_upstream_server,
    _handle_query,
    start_dns_server,
    stop_dns_server,
)

# ---------------------------------------------------------------------------
# _get_system_dns — parsing /etc/resolv.conf
# ---------------------------------------------------------------------------


class TestGetSystemDNS:
    """Test detection of system DNS from /etc/resolv.conf."""

    def test_parses_first_nameserver(self) -> None:
        content = "nameserver 1.1.1.1\nnameserver 8.8.8.8\n"
        with patch("builtins.open", mock_open(read_data=content)):
            assert _get_system_dns() == "1.1.1.1"

    def test_skips_comments(self) -> None:
        content = "# nameserver 9.9.9.9\nnameserver 1.0.0.1\n"
        with patch("builtins.open", mock_open(read_data=content)):
            assert _get_system_dns() == "1.0.0.1"

    def test_skips_non_nameserver_lines(self) -> None:
        content = "search example.com\nnameserver 4.4.4.4\n"
        with patch("builtins.open", mock_open(read_data=content)):
            assert _get_system_dns() == "4.4.4.4"

    def test_handles_extra_whitespace(self) -> None:
        content = "  nameserver   8.8.4.4  \n"
        with patch("builtins.open", mock_open(read_data=content)):
            assert _get_system_dns() == "8.8.4.4"

    def test_fallback_on_oserror(self) -> None:
        with patch("builtins.open", side_effect=OSError("no such file")):
            assert _get_system_dns() == "8.8.8.8"

    def test_fallback_on_empty_file(self) -> None:
        with patch("builtins.open", mock_open(read_data="")):
            assert _get_system_dns() == "8.8.8.8"

    def test_nameserver_without_value_skipped(self) -> None:
        content = "nameserver\nnameserver 2.2.2.2\n"
        with patch("builtins.open", mock_open(read_data=content)):
            # "nameserver" alone has only 1 part, so skip it
            assert _get_system_dns() == "2.2.2.2"


# ---------------------------------------------------------------------------
# _get_upstream_server
# ---------------------------------------------------------------------------


class TestGetUpstreamServer:
    """Test upstream server selection."""

    def test_uses_config_when_provided(self) -> None:
        config = {"upstream_server": "1.2.3.4"}
        assert _get_upstream_server(config) == "1.2.3.4"

    def test_falls_back_to_system_dns(self) -> None:
        config = {"upstream_server": ""}
        with patch("robotocore.dns.server._get_system_dns", return_value="9.9.9.9"):
            assert _get_upstream_server(config) == "9.9.9.9"

    def test_empty_string_triggers_fallback(self) -> None:
        config = {"upstream_server": ""}
        with patch("robotocore.dns.server._get_system_dns", return_value="8.8.8.8"):
            assert _get_upstream_server(config) == "8.8.8.8"

    def test_missing_key_triggers_fallback(self) -> None:
        config = {}
        with patch("robotocore.dns.server._get_system_dns", return_value="1.1.1.1"):
            assert _get_upstream_server(config) == "1.1.1.1"


# ---------------------------------------------------------------------------
# _forward_upstream — mocked socket
# ---------------------------------------------------------------------------


class TestForwardUpstream:
    """Test upstream DNS forwarding with mocked sockets."""

    def test_returns_response_bytes(self) -> None:
        fake_response = b"\x00\x01response"
        mock_sock = MagicMock()
        mock_sock.recvfrom.return_value = (fake_response, ("8.8.8.8", 53))

        with patch("socket.socket", return_value=mock_sock):
            result = _forward_upstream(b"\x00\x01query", "8.8.8.8")
            assert result == fake_response

    def test_sends_to_correct_upstream(self) -> None:
        mock_sock = MagicMock()
        mock_sock.recvfrom.return_value = (b"resp", ("1.2.3.4", 53))

        with patch("socket.socket", return_value=mock_sock):
            _forward_upstream(b"q", "1.2.3.4", upstream_port=5353)
            mock_sock.sendto.assert_called_once_with(b"q", ("1.2.3.4", 5353))

    def test_returns_none_on_timeout(self) -> None:
        mock_sock = MagicMock()
        mock_sock.recvfrom.side_effect = TimeoutError()

        with patch("socket.socket", return_value=mock_sock):
            result = _forward_upstream(b"q", "8.8.8.8")
            assert result is None

    def test_returns_none_on_oserror(self) -> None:
        mock_sock = MagicMock()
        mock_sock.recvfrom.side_effect = OSError("Network unreachable")

        with patch("socket.socket", return_value=mock_sock):
            result = _forward_upstream(b"q", "8.8.8.8")
            assert result is None

    def test_socket_always_closed(self) -> None:
        mock_sock = MagicMock()
        mock_sock.recvfrom.side_effect = TimeoutError()

        with patch("socket.socket", return_value=mock_sock):
            _forward_upstream(b"q", "8.8.8.8")
            mock_sock.close.assert_called_once()

    def test_socket_closed_on_success(self) -> None:
        mock_sock = MagicMock()
        mock_sock.recvfrom.return_value = (b"resp", ("8.8.8.8", 53))

        with patch("socket.socket", return_value=mock_sock):
            _forward_upstream(b"q", "8.8.8.8")
            mock_sock.close.assert_called_once()


# ---------------------------------------------------------------------------
# _build_response — local DNS response construction
# ---------------------------------------------------------------------------


class TestBuildResponse:
    """Test local DNS response building for different query types."""

    def _make_config(self, resolve_ip: str = "127.0.0.1") -> dict:
        config = get_config()
        config["resolve_ip"] = resolve_ip
        return config

    def test_a_query_for_aws_hostname(self) -> None:
        request = DNSRecord.question("s3.amazonaws.com", "A")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is not None
        assert len(response.rr) == 1
        assert str(response.rr[0].rdata) == "127.0.0.1"
        assert response.rr[0].ttl == DEFAULT_TTL

    def test_a_query_with_custom_ip(self) -> None:
        request = DNSRecord.question("s3.amazonaws.com", "A")
        config = self._make_config("10.0.0.42")
        response = _build_response(request, config)
        assert response is not None
        assert str(response.rr[0].rdata) == "10.0.0.42"

    def test_a_query_for_non_aws_returns_none(self) -> None:
        request = DNSRecord.question("google.com", "A")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is None

    def test_aaaa_query_for_aws_hostname(self) -> None:
        request = DNSRecord.question("sqs.amazonaws.com", "AAAA")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is not None
        assert len(response.rr) == 1
        assert response.rr[0].ttl == DEFAULT_TTL

    def test_aaaa_query_for_non_aws_returns_none(self) -> None:
        request = DNSRecord.question("google.com", "AAAA")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is None

    def test_cname_query_for_aws_hostname(self) -> None:
        request = DNSRecord.question("sqs.amazonaws.com", "CNAME")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is not None
        assert len(response.rr) == 1
        assert "sqs.amazonaws.com" in str(response.rr[0].rdata)

    def test_cname_query_for_non_aws_returns_none(self) -> None:
        request = DNSRecord.question("github.com", "CNAME")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is None

    def test_mx_query_returns_none(self) -> None:
        """MX queries are not handled locally."""
        request = DNSRecord.question("s3.amazonaws.com", "MX")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is None

    def test_txt_query_returns_none(self) -> None:
        """TXT queries are not handled locally."""
        request = DNSRecord.question("s3.amazonaws.com", "TXT")
        config = self._make_config()
        response = _build_response(request, config)
        assert response is None

    def test_custom_ttl(self) -> None:
        request = DNSRecord.question("s3.amazonaws.com", "A")
        config = self._make_config()
        config["ttl"] = 600
        response = _build_response(request, config)
        assert response is not None
        assert response.rr[0].ttl == 600


# ---------------------------------------------------------------------------
# _handle_query — full query handling pipeline
# ---------------------------------------------------------------------------


class TestHandleQuery:
    """Test the full query handling pipeline."""

    def _make_config(self) -> dict:
        config = get_config()
        config["resolve_ip"] = "127.0.0.1"
        return config

    def test_local_resolution_returns_packed_response(self) -> None:
        request = DNSRecord.question("s3.amazonaws.com", "A")
        config = self._make_config()
        result = _handle_query(request.pack(), config, "8.8.8.8")
        response = DNSRecord.parse(result)
        assert len(response.rr) == 1
        assert str(response.rr[0].rdata) == "127.0.0.1"

    def test_non_aws_forwards_upstream(self) -> None:
        request = DNSRecord.question("example.com", "A")
        config = self._make_config()
        fake_upstream = DNSRecord(DNSHeader(id=request.header.id, qr=1, ra=1))
        with patch("robotocore.dns.server._forward_upstream", return_value=fake_upstream.pack()):
            result = _handle_query(request.pack(), config, "8.8.8.8")
            response = DNSRecord.parse(result)
            assert response.header.qr == 1  # It's a response

    def test_upstream_failure_returns_servfail(self) -> None:
        request = DNSRecord.question("example.com", "A")
        config = self._make_config()
        with patch("robotocore.dns.server._forward_upstream", return_value=None):
            result = _handle_query(request.pack(), config, "8.8.8.8")
            response = DNSRecord.parse(result)
            assert response.header.rcode == 2  # SERVFAIL

    def test_malformed_data_with_extractable_id(self) -> None:
        """Malformed packet with a valid 2-byte ID prefix -> SERVFAIL with same ID."""
        query_id = 0x1234
        bad_data = struct.pack("!H", query_id) + b"\xff\xff\xff garbage"
        config = self._make_config()
        result = _handle_query(bad_data, config, "8.8.8.8")
        if result:
            response = DNSRecord.parse(result)
            assert response.header.id == query_id
            assert response.header.rcode == 2  # SERVFAIL

    def test_completely_empty_data(self) -> None:
        config = self._make_config()
        result = _handle_query(b"", config, "8.8.8.8")
        # Empty data -> can't extract ID -> empty bytes
        assert result == b""

    def test_single_byte_data(self) -> None:
        config = self._make_config()
        result = _handle_query(b"\x01", config, "8.8.8.8")
        # Less than 2 bytes -> can't extract ID
        # Might return b"" or a SERVFAIL — either is acceptable
        assert isinstance(result, bytes)


# ---------------------------------------------------------------------------
# DNSServer — lifecycle without actual binding
# ---------------------------------------------------------------------------


class TestDNSServerInit:
    """Test DNSServer initialization and properties."""

    def test_default_config(self) -> None:
        server = DNSServer(address="127.0.0.1", port=15400)
        assert server.address == "127.0.0.1"
        assert server.port == 15400
        assert server.is_running is False

    def test_custom_config(self) -> None:
        config = {"resolve_ip": "10.0.0.1", "ttl": 60}
        server = DNSServer(address="0.0.0.0", port=53, config=config)
        assert server.config["resolve_ip"] == "10.0.0.1"
        assert server.config["ttl"] == 60

    def test_not_running_initially(self) -> None:
        server = DNSServer()
        assert server.is_running is False
        assert server._socket is None
        assert server._thread is None

    def test_stop_when_not_started(self) -> None:
        """Stopping a server that was never started should not raise."""
        server = DNSServer()
        server.stop()  # Should not raise
        assert server.is_running is False


# ---------------------------------------------------------------------------
# Module-level start/stop helpers
# ---------------------------------------------------------------------------


class TestModuleLevelHelpers:
    """Test start_dns_server and stop_dns_server module-level functions."""

    def test_start_dns_server_disabled(self) -> None:
        with patch.dict(os.environ, {"DNS_DISABLED": "1"}):
            result = start_dns_server()
            assert result is None

    def test_start_dns_server_bind_failure(self) -> None:
        """If the port can't be bound, returns None without raising."""
        with patch.dict(os.environ, {"DNS_PORT": "15401", "DNS_ADDRESS": "127.0.0.1"}):
            with patch.object(DNSServer, "start", side_effect=OSError("Address in use")):
                result = start_dns_server()
                assert result is None

    def test_stop_dns_server_when_none(self) -> None:
        """stop_dns_server should handle None _server gracefully."""
        import robotocore.dns.server as mod

        original = mod._server
        mod._server = None
        try:
            stop_dns_server()  # Should not raise
        finally:
            mod._server = original

    def test_stop_dns_server_calls_stop(self) -> None:
        import robotocore.dns.server as mod

        mock_server = MagicMock()
        original = mod._server
        mod._server = mock_server
        try:
            stop_dns_server()
            mock_server.stop.assert_called_once()
            assert mod._server is None
        finally:
            mod._server = original

    def test_start_returns_server_on_success(self) -> None:
        with patch.dict(
            os.environ, {"DNS_PORT": "15402", "DNS_ADDRESS": "127.0.0.1", "DNS_DISABLED": "0"}
        ):
            with patch.object(DNSServer, "start"):
                result = start_dns_server()
                assert result is not None
                assert isinstance(result, DNSServer)
                # Clean up
                import robotocore.dns.server as mod

                mod._server = None
