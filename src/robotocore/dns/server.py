"""UDP DNS server using dnslib.

Listens for DNS queries, resolves *.amazonaws.com to the local IP,
and forwards all other queries to an upstream DNS server.
"""

import logging
import socket
import struct
import threading

from dnslib import AAAA, CNAME, QTYPE, RR, A, DNSHeader, DNSRecord

from robotocore.dns.resolver import (
    DEFAULT_TTL,
    get_config,
    resolve_a_record,
    resolve_aaaa_record,
    resolve_cname_record,
)

logger = logging.getLogger(__name__)

# Buffer size for UDP packets
_UDP_BUFFER_SIZE = 4096

# Timeout for upstream DNS queries (seconds)
_UPSTREAM_TIMEOUT = 5.0


def _get_system_dns() -> str:
    """Detect the system DNS server from /etc/resolv.conf."""
    try:
        with open("/etc/resolv.conf") as f:
            for line in f:
                line = line.strip()
                if line.startswith("nameserver"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return parts[1]
    except OSError as exc:
        logger.debug("_get_system_dns: open failed (non-fatal): %s", exc)
    return "8.8.8.8"


def _get_upstream_server(config: dict) -> str:
    """Get the upstream DNS server address."""
    upstream = config.get("upstream_server", "")
    if upstream:
        return upstream
    return _get_system_dns()


def _forward_upstream(data: bytes, upstream: str, upstream_port: int = 53) -> bytes | None:
    """Forward a raw DNS query to an upstream server and return the response."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(_UPSTREAM_TIMEOUT)
        try:
            sock.sendto(data, (upstream, upstream_port))
            response, _ = sock.recvfrom(_UDP_BUFFER_SIZE)
            return response
        finally:
            sock.close()
    except (TimeoutError, OSError):
        logger.debug("Upstream DNS query to %s failed", upstream)
        return None


def _build_response(request: DNSRecord, config: dict) -> DNSRecord | None:
    """Build a local DNS response for the request, or return None to forward upstream."""
    reply = request.reply()
    qname = str(request.q.qname)
    qtype = request.q.qtype
    ttl = config.get("ttl", DEFAULT_TTL)

    handled = False

    if qtype == QTYPE.A:
        ip = resolve_a_record(qname, config)
        if ip is not None:
            reply.add_answer(RR(qname, QTYPE.A, rdata=A(ip), ttl=ttl))
            handled = True

    elif qtype == QTYPE.AAAA:
        ip6 = resolve_aaaa_record(qname, config)
        if ip6 is not None:
            reply.add_answer(RR(qname, QTYPE.AAAA, rdata=AAAA(ip6), ttl=ttl))
            handled = True

    elif qtype == QTYPE.CNAME:
        target = resolve_cname_record(qname, config)
        if target is not None:
            reply.add_answer(RR(qname, QTYPE.CNAME, rdata=CNAME(target), ttl=ttl))
            handled = True

    if handled:
        return reply
    return None


def _handle_query(data: bytes, config: dict, upstream: str) -> bytes:
    """Handle a single DNS query: resolve locally or forward upstream."""
    try:
        request = DNSRecord.parse(data)
    except Exception:
        logger.debug("Failed to parse DNS query, dropping")
        # Return SERVFAIL for malformed packets
        try:
            # Try to extract the query ID from the raw data
            if len(data) >= 2:
                qid = struct.unpack("!H", data[:2])[0]
                error_reply = DNSRecord(DNSHeader(id=qid, qr=1, ra=1, rcode=2))
                return error_reply.pack()
        except Exception as exc:
            logger.debug("_handle_query: len failed (non-fatal): %s", exc)
        return b""

    # Try local resolution
    response = _build_response(request, config)
    if response is not None:
        return response.pack()

    # Forward upstream
    upstream_response = _forward_upstream(data, upstream)
    if upstream_response is not None:
        return upstream_response

    # Return SERVFAIL if upstream also fails
    reply = request.reply()
    reply.header.rcode = 2  # SERVFAIL
    return reply.pack()


class DNSServer:
    """UDP DNS server that runs in a background thread."""

    def __init__(
        self,
        address: str = "0.0.0.0",
        port: int = 53,
        config: dict | None = None,
    ):
        self.address = address
        self.port = port
        self.config = config or get_config()
        self._socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        """Start the DNS server in a background daemon thread."""
        if self._running:
            return

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.settimeout(1.0)  # Allow periodic shutdown checks
        self._socket.bind((self.address, self.port))
        self._running = True

        self._thread = threading.Thread(target=self._serve, daemon=True, name="dns-server")
        self._thread.start()
        logger.info("DNS server started on %s:%d", self.address, self.port)

    def stop(self) -> None:
        """Stop the DNS server."""
        self._running = False
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError as exc:
                logger.debug("stop: close failed (non-fatal): %s", exc)
            self._socket = None
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("DNS server stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def _serve(self) -> None:
        """Main server loop: receive queries, handle them, send responses."""
        upstream = _get_upstream_server(self.config)
        logger.info("DNS upstream server: %s", upstream)

        while self._running:
            try:
                data, addr = self._socket.recvfrom(_UDP_BUFFER_SIZE)
            except TimeoutError:
                continue
            except OSError:
                if self._running:
                    logger.debug("DNS socket error, stopping")
                break

            try:
                response = _handle_query(data, self.config, upstream)
                if response:
                    self._socket.sendto(response, addr)
            except OSError:
                if self._running:
                    logger.debug("DNS send error for %s", addr)


# ---------------------------------------------------------------------------
# Module-level singleton for start/stop from main.py
# ---------------------------------------------------------------------------

_server: DNSServer | None = None


def start_dns_server() -> DNSServer | None:
    """Start the DNS server using environment configuration.

    Returns the DNSServer instance, or None if disabled or if the port
    cannot be bound (e.g. port 53 requires root privileges).
    """
    global _server

    config = get_config()
    if config["disabled"]:
        logger.info("DNS server disabled via DNS_DISABLED=1")
        return None

    _server = DNSServer(
        address=config["address"],
        port=config["port"],
        config=config,
    )
    try:
        _server.start()
    except OSError as e:
        logger.warning(
            "DNS server failed to start on %s:%d: %s", config["address"], config["port"], e
        )
        _server = None
        return None
    return _server


def stop_dns_server() -> None:
    """Stop the DNS server if running."""
    global _server
    if _server is not None:
        _server.stop()
        _server = None
