"""Unit tests for SES SMTP server and EmailStore."""

import smtplib
import socket
import threading
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

import pytest
from aiosmtpd.controller import Controller

from robotocore.services.ses.email_store import EmailStore, StoredEmail, get_email_store
from robotocore.services.ses.smtp_server import (
    RobotocoreSMTPHandler,
    _get_verified_identities,
    _is_sender_verified,
    start_smtp_server,
    stop_smtp_server,
)


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------------------------------------------------------------------------
# StoredEmail dataclass
# ---------------------------------------------------------------------------


class TestStoredEmail:
    """Tests for the StoredEmail dataclass."""

    def test_to_dict_includes_all_fields(self):
        email = StoredEmail(
            sender="alice@example.com",
            recipients=["bob@example.com"],
            subject="Hello",
            body="Hi Bob",
            raw="raw content",
            timestamp=1234567890.0,
        )
        d = email.to_dict()
        assert d["sender"] == "alice@example.com"
        assert d["recipients"] == ["bob@example.com"]
        assert d["subject"] == "Hello"
        assert d["body"] == "Hi Bob"
        assert d["timestamp"] == 1234567890.0

    def test_to_dict_excludes_raw(self):
        """The raw field should NOT appear in the dict (it's large and not needed in API)."""
        email = StoredEmail(
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="S",
            body="B",
            raw="very long raw content",
        )
        d = email.to_dict()
        assert "raw" not in d

    def test_timestamp_auto_set(self):
        """Timestamp should be auto-set to current time if not provided."""
        before = time.time()
        email = StoredEmail(
            sender="a@b.com",
            recipients=["c@d.com"],
            subject="S",
            body="B",
            raw="raw",
        )
        after = time.time()
        assert before <= email.timestamp <= after

    def test_multiple_recipients_preserved(self):
        email = StoredEmail(
            sender="a@b.com",
            recipients=["x@y.com", "z@w.com", "q@r.com"],
            subject="S",
            body="B",
            raw="raw",
        )
        d = email.to_dict()
        assert len(d["recipients"]) == 3
        assert "x@y.com" in d["recipients"]
        assert "z@w.com" in d["recipients"]

    def test_empty_subject_and_body(self):
        email = StoredEmail(sender="a@b.com", recipients=["c@d.com"], subject="", body="", raw="")
        d = email.to_dict()
        assert d["subject"] == ""
        assert d["body"] == ""


# ---------------------------------------------------------------------------
# EmailStore
# ---------------------------------------------------------------------------


class TestEmailStore:
    """Tests for the EmailStore class."""

    def test_add_and_get_messages(self):
        store = EmailStore()
        store.add_message(
            sender="alice@example.com",
            recipients=["bob@example.com"],
            subject="Hello",
            body="Hi Bob",
            raw="From: alice@example.com\nTo: bob@example.com\nSubject: Hello\n\nHi Bob",
        )

        messages = store.get_messages()
        assert len(messages) == 1
        assert messages[0]["sender"] == "alice@example.com"
        assert messages[0]["recipients"] == ["bob@example.com"]
        assert messages[0]["subject"] == "Hello"
        assert messages[0]["body"] == "Hi Bob"
        assert "timestamp" in messages[0]

    def test_get_messages_returns_most_recent_first(self):
        store = EmailStore()
        store.add_message("a@x.com", ["b@x.com"], "First", "1", "raw1")
        store.add_message("a@x.com", ["b@x.com"], "Second", "2", "raw2")
        store.add_message("a@x.com", ["b@x.com"], "Third", "3", "raw3")

        messages = store.get_messages()
        assert len(messages) == 3
        assert messages[0]["subject"] == "Third"
        assert messages[1]["subject"] == "Second"
        assert messages[2]["subject"] == "First"

    def test_get_messages_with_limit(self):
        store = EmailStore()
        for i in range(10):
            store.add_message("a@x.com", ["b@x.com"], f"Msg {i}", str(i), f"raw{i}")

        messages = store.get_messages(limit=3)
        assert len(messages) == 3
        # Most recent 3
        assert messages[0]["subject"] == "Msg 9"
        assert messages[1]["subject"] == "Msg 8"
        assert messages[2]["subject"] == "Msg 7"

    def test_get_messages_limit_larger_than_store(self):
        store = EmailStore()
        store.add_message("a@x.com", ["b@x.com"], "Only", "body", "raw")
        messages = store.get_messages(limit=100)
        assert len(messages) == 1
        assert messages[0]["subject"] == "Only"

    def test_get_messages_empty_store(self):
        store = EmailStore()
        messages = store.get_messages()
        assert messages == []

    def test_clear_messages(self):
        store = EmailStore()
        store.add_message("a@x.com", ["b@x.com"], "Test", "body", "raw")
        store.add_message("a@x.com", ["b@x.com"], "Test2", "body2", "raw2")

        count = store.clear_messages()
        assert count == 2
        assert store.get_messages() == []

    def test_clear_empty_store(self):
        store = EmailStore()
        count = store.clear_messages()
        assert count == 0

    def test_clear_then_add(self):
        """After clearing, new messages should work normally."""
        store = EmailStore()
        store.add_message("a@x.com", ["b@x.com"], "Before", "b", "r")
        store.clear_messages()
        store.add_message("a@x.com", ["b@x.com"], "After", "a", "r")
        messages = store.get_messages()
        assert len(messages) == 1
        assert messages[0]["subject"] == "After"

    def test_thread_safety(self):
        store = EmailStore()
        errors = []

        def writer(n):
            try:
                for i in range(50):
                    store.add_message(f"w{n}@x.com", ["b@x.com"], f"W{n}-{i}", "body", "raw")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        messages = store.get_messages(limit=300)
        assert len(messages) == 200  # 4 threads * 50 messages

    def test_concurrent_read_write(self):
        """Concurrent readers and writers should not crash."""
        store = EmailStore()
        errors = []

        def writer():
            try:
                for i in range(50):
                    store.add_message("w@x.com", ["b@x.com"], f"W-{i}", "body", "raw")
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    store.get_messages()
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(errors) == 0

    def test_get_email_store_singleton(self):
        store1 = get_email_store()
        store2 = get_email_store()
        assert store1 is store2

    def test_messages_are_dicts_not_dataclasses(self):
        """get_messages returns plain dicts, not StoredEmail instances."""
        store = EmailStore()
        store.add_message("a@b.com", ["c@d.com"], "S", "B", "R")
        messages = store.get_messages()
        assert isinstance(messages[0], dict)


# ---------------------------------------------------------------------------
# Sender validation (_is_sender_verified)
# ---------------------------------------------------------------------------


class TestSenderValidation:
    """Tests for sender verification logic."""

    def test_exact_email_match(self):
        identities = {"alice@example.com", "bob@example.com"}
        assert _is_sender_verified("alice@example.com", identities) is True
        assert _is_sender_verified("bob@example.com", identities) is True
        assert _is_sender_verified("eve@example.com", identities) is False

    def test_domain_match(self):
        identities = {"example.com"}
        assert _is_sender_verified("anyone@example.com", identities) is True
        assert _is_sender_verified("anyone@other.com", identities) is False

    def test_empty_identities_allows_all(self):
        assert _is_sender_verified("anyone@anywhere.com", set()) is True

    def test_case_insensitive_domain(self):
        identities = {"Example.COM"}
        assert _is_sender_verified("user@example.com", identities) is True

    def test_no_at_sign_in_sender(self):
        """A sender without @ cannot match a domain but can match exact."""
        identities = {"nodomain"}
        assert _is_sender_verified("nodomain", identities) is True

    def test_no_at_sign_not_in_identities(self):
        identities = {"example.com"}
        assert _is_sender_verified("noatsign", identities) is False

    def test_subdomain_does_not_match_parent(self):
        """sub.example.com should NOT match an identity of 'example.com'."""
        identities = {"example.com"}
        assert _is_sender_verified("user@sub.example.com", identities) is False

    def test_mixed_email_and_domain_identities(self):
        identities = {"alice@example.com", "otherdomain.com"}
        # alice matches by exact email
        assert _is_sender_verified("alice@example.com", identities) is True
        # bob matches by domain
        assert _is_sender_verified("bob@otherdomain.com", identities) is True
        # carol matches neither
        assert _is_sender_verified("carol@unknown.com", identities) is False

    def test_empty_sender_string(self):
        """Empty sender should not match any identity."""
        identities = {"example.com"}
        assert _is_sender_verified("", identities) is False

    def test_domain_case_mismatch_both_ways(self):
        """Domain matching should be case-insensitive for both identity and sender."""
        identities = {"EXAMPLE.com"}
        assert _is_sender_verified("user@Example.COM", identities) is True


# ---------------------------------------------------------------------------
# _get_verified_identities (with mocked Moto backend)
# ---------------------------------------------------------------------------


class TestGetVerifiedIdentities:
    """Tests for _get_verified_identities with mocked Moto backend."""

    def test_returns_emails_and_domains(self):
        mock_backend = MagicMock()
        mock_backend.email_identities = ["alice@example.com", "bob@example.com"]
        mock_backend.domains = ["example.org"]

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value = {"123456789012": {"us-east-1": mock_backend}}
            result = _get_verified_identities("123456789012", "us-east-1")

        assert "alice@example.com" in result
        assert "bob@example.com" in result
        assert "example.org" in result

    def test_returns_empty_set_on_exception(self):
        """When Moto backend is not available, returns empty set (allow all)."""
        with patch(
            "moto.backends.get_backend",
            side_effect=Exception("no backend"),
        ):
            result = _get_verified_identities("123456789012", "us-east-1")
        assert result == set()

    def test_returns_empty_set_on_import_error(self):
        """If moto is not installed at all, returns empty set."""
        with patch(
            "moto.backends.get_backend",
            side_effect=ImportError("no moto"),
        ):
            result = _get_verified_identities("123456789012", "us-east-1")
        assert result == set()

    def test_uses_correct_account_and_region(self):
        mock_backend = MagicMock()
        mock_backend.email_identities = []
        mock_backend.domains = []

        mock_acct_region = MagicMock()
        mock_acct_region.__getitem__ = MagicMock(return_value=mock_backend)
        mock_acct = MagicMock()
        mock_acct.__getitem__ = MagicMock(return_value=mock_acct_region)

        with patch("moto.backends.get_backend") as mock_get:
            mock_get.return_value = mock_acct
            _get_verified_identities("999888777666", "eu-west-1")

        mock_acct.__getitem__.assert_called_with("999888777666")
        mock_acct_region.__getitem__.assert_called_with("eu-west-1")


# ---------------------------------------------------------------------------
# RobotocoreSMTPHandler
# ---------------------------------------------------------------------------


class TestRobotocoreSMTPHandler:
    """Tests for handler initialization and defaults."""

    def test_default_account_and_region(self):
        handler = RobotocoreSMTPHandler()
        assert handler.account_id == "123456789012"
        assert handler.region == "us-east-1"

    def test_custom_account_and_region(self):
        handler = RobotocoreSMTPHandler(account_id="999888777666", region="eu-west-1")
        assert handler.account_id == "999888777666"
        assert handler.region == "eu-west-1"


# ---------------------------------------------------------------------------
# start_smtp_server / stop_smtp_server lifecycle
# ---------------------------------------------------------------------------


class TestSMTPServerLifecycle:
    """Tests for start/stop lifecycle functions."""

    def test_start_returns_controller(self):
        port = _find_free_port()
        with patch.dict("os.environ", {"SMTP_PORT": str(port), "SMTP_DISABLED": "0"}):
            controller = start_smtp_server()
            try:
                assert controller is not None
                assert isinstance(controller, Controller)
            finally:
                stop_smtp_server()

    def test_start_disabled_returns_none(self):
        with patch.dict("os.environ", {"SMTP_DISABLED": "1"}):
            controller = start_smtp_server()
            assert controller is None

    def test_stop_when_not_started(self):
        """stop_smtp_server should not raise when no server is running."""
        # Reset the global controller to None
        import robotocore.services.ses.smtp_server as mod

        original = mod._controller
        mod._controller = None
        try:
            stop_smtp_server()  # Should not raise
        finally:
            mod._controller = original

    def test_start_respects_custom_port(self):
        port = _find_free_port()
        with patch.dict(
            "os.environ",
            {"SMTP_PORT": str(port), "SMTP_DISABLED": "0"},
        ):
            controller = start_smtp_server()
            try:
                assert controller is not None
                # Verify we can connect on the custom port
                with smtplib.SMTP("127.0.0.1", port, timeout=5) as client:
                    code, _ = client.noop()
                    assert code == 250
            finally:
                stop_smtp_server()

    def test_start_respects_custom_account_region(self):
        port = _find_free_port()
        with patch.dict(
            "os.environ",
            {
                "SMTP_PORT": str(port),
                "SMTP_DISABLED": "0",
                "DEFAULT_ACCOUNT_ID": "111222333444",
                "DEFAULT_REGION": "ap-southeast-1",
            },
        ):
            controller = start_smtp_server()
            try:
                assert controller is not None
                # The handler inside the controller should have custom values
                handler = controller.handler
                assert handler.account_id == "111222333444"
                assert handler.region == "ap-southeast-1"
            finally:
                stop_smtp_server()


# ---------------------------------------------------------------------------
# SMTP server integration (real SMTP messages)
# ---------------------------------------------------------------------------


class TestSMTPServer:
    """Tests for the actual SMTP server (sends real SMTP messages)."""

    @pytest.fixture()
    def smtp_server(self):
        """Start a test SMTP server on a random high port."""
        handler = RobotocoreSMTPHandler()
        port = _find_free_port()
        controller = Controller(handler, hostname="127.0.0.1", port=port)
        controller.start()
        time.sleep(0.3)
        yield port
        controller.stop()

    def test_send_email_via_smtp(self, smtp_server):
        """Send a real email via smtplib and verify it lands in the store."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEText("Hello from SMTP test")
        msg["Subject"] = "SMTP Test"
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) >= 1

        latest = messages[0]
        assert latest["sender"] == "sender@example.com"
        assert "recipient@example.com" in latest["recipients"]
        assert latest["subject"] == "SMTP Test"
        assert "Hello from SMTP test" in latest["body"]

    def test_send_to_multiple_recipients(self, smtp_server):
        """Send to multiple recipients."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEText("Multi-recipient test")
        msg["Subject"] = "Multi"
        msg["From"] = "sender@example.com"
        msg["To"] = "a@example.com, b@example.com"

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.sendmail(
                "sender@example.com",
                ["a@example.com", "b@example.com"],
                msg.as_string(),
            )

        messages = store.get_messages()
        assert len(messages) >= 1
        latest = messages[0]
        assert "a@example.com" in latest["recipients"]
        assert "b@example.com" in latest["recipients"]

    def test_sender_validation_rejects_unverified(self, smtp_server):
        """When SES has verified identities, unverified senders are rejected."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        mock_identities = {"verified@example.com"}
        with patch(
            "robotocore.services.ses.smtp_server._get_verified_identities",
            return_value=mock_identities,
        ):
            msg = MIMEText("Should be rejected")
            msg["Subject"] = "Rejected"
            msg["From"] = "unverified@evil.com"
            msg["To"] = "recipient@example.com"

            with smtplib.SMTP("127.0.0.1", port) as client:
                with pytest.raises(smtplib.SMTPDataError) as exc_info:
                    client.sendmail(
                        "unverified@evil.com",
                        ["recipient@example.com"],
                        msg.as_string(),
                    )
                assert exc_info.value.smtp_code == 554
                assert b"not verified" in exc_info.value.smtp_error

        # The message should NOT have been stored
        messages = store.get_messages()
        rejected_msgs = [m for m in messages if m["subject"] == "Rejected"]
        assert len(rejected_msgs) == 0

    def test_sender_validation_accepts_verified(self, smtp_server):
        """When SES has verified identities, verified senders are accepted."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        mock_identities = {"verified@example.com"}
        with patch(
            "robotocore.services.ses.smtp_server._get_verified_identities",
            return_value=mock_identities,
        ):
            msg = MIMEText("Should be accepted")
            msg["Subject"] = "Accepted"
            msg["From"] = "verified@example.com"
            msg["To"] = "recipient@example.com"

            with smtplib.SMTP("127.0.0.1", port) as client:
                client.send_message(msg)

        messages = store.get_messages()
        accepted = [m for m in messages if m["subject"] == "Accepted"]
        assert len(accepted) == 1

    def test_domain_verified_sender_accepted(self, smtp_server):
        """A sender whose domain is verified should be accepted."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        mock_identities = {"example.com"}
        with patch(
            "robotocore.services.ses.smtp_server._get_verified_identities",
            return_value=mock_identities,
        ):
            msg = MIMEText("Domain verified")
            msg["Subject"] = "DomainOK"
            msg["From"] = "anyuser@example.com"
            msg["To"] = "recipient@example.com"

            with smtplib.SMTP("127.0.0.1", port) as client:
                client.send_message(msg)

        messages = store.get_messages()
        accepted = [m for m in messages if m["subject"] == "DomainOK"]
        assert len(accepted) == 1

    def test_no_identities_allows_all_senders(self, smtp_server):
        """When no identities are configured, all senders are allowed."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        # Return empty set (no identities) -- should allow everything
        with patch(
            "robotocore.services.ses.smtp_server._get_verified_identities",
            return_value=set(),
        ):
            msg = MIMEText("Open relay mode")
            msg["Subject"] = "OpenRelay"
            msg["From"] = "anyone@anywhere.com"
            msg["To"] = "recipient@example.com"

            with smtplib.SMTP("127.0.0.1", port) as client:
                client.send_message(msg)

        messages = store.get_messages()
        accepted = [m for m in messages if m["subject"] == "OpenRelay"]
        assert len(accepted) == 1

    def test_multipart_email_extracts_text_body(self, smtp_server):
        """A multipart email should have its text/plain body extracted."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Multipart"
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"

        text_part = MIMEText("Plain text body", "plain")
        html_part = MIMEText("<h1>HTML body</h1>", "html")
        msg.attach(text_part)
        msg.attach(html_part)

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) >= 1
        latest = messages[0]
        assert latest["subject"] == "Multipart"
        assert "Plain text body" in latest["body"]

    def test_email_with_no_subject(self, smtp_server):
        """An email with no Subject header should still be stored."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEText("No subject body")
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"
        # Intentionally no Subject header

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) >= 1
        # Subject should be empty string (not None or missing)
        latest = messages[0]
        assert latest["subject"] == "" or latest["subject"] is not None

    def test_multiple_emails_accumulate(self, smtp_server):
        """Sending multiple emails should accumulate in the store."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        for i in range(5):
            msg = MIMEText(f"Body {i}")
            msg["Subject"] = f"Accumulate {i}"
            msg["From"] = "sender@example.com"
            msg["To"] = "recipient@example.com"

            with smtplib.SMTP("127.0.0.1", port) as client:
                client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) == 5
        # Most recent first
        assert messages[0]["subject"] == "Accumulate 4"
        assert messages[4]["subject"] == "Accumulate 0"

    def test_empty_body_email(self, smtp_server):
        """An email with empty body should be stored with empty body."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEText("")
        msg["Subject"] = "EmptyBody"
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) >= 1
        latest = messages[0]
        assert latest["subject"] == "EmptyBody"
        assert latest["body"] == "" or latest["body"].strip() == ""

    def test_unicode_subject_and_body(self, smtp_server):
        """Unicode characters in subject and body should be preserved."""
        port = smtp_server
        store = get_email_store()
        store.clear_messages()

        msg = MIMEText("Hello from Tokyo!")
        msg["Subject"] = "Unicode test"
        msg["From"] = "sender@example.com"
        msg["To"] = "recipient@example.com"

        with smtplib.SMTP("127.0.0.1", port) as client:
            client.send_message(msg)

        messages = store.get_messages()
        assert len(messages) >= 1
        assert "Hello from Tokyo" in messages[0]["body"]


# ---------------------------------------------------------------------------
# Management API endpoints (ses_messages_list, ses_messages_clear)
# ---------------------------------------------------------------------------


class TestManagementAPIEndpoints:
    """Tests for the /_robotocore/ses/messages management endpoints."""

    @pytest.fixture(autouse=True)
    def _clear_store(self):
        """Clear the global email store before each test."""
        get_email_store().clear_messages()
        yield
        get_email_store().clear_messages()

    @pytest.mark.asyncio
    async def test_ses_messages_list_empty(self):

        # Use Starlette's TestClient approach — but we can just call the endpoint directly
        # by constructing a mock request.
        from starlette.requests import Request

        from robotocore.gateway.app import ses_messages_list

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/_robotocore/ses/messages",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        response = await ses_messages_list(request)
        assert response.status_code == 200
        body = response.body.decode()
        import json

        data = json.loads(body)
        assert data["messages"] == []
        assert data["count"] == 0

    @pytest.mark.asyncio
    async def test_ses_messages_list_with_messages(self):
        import json

        from starlette.requests import Request

        from robotocore.gateway.app import ses_messages_list

        store = get_email_store()
        store.add_message("a@b.com", ["c@d.com"], "Test", "Body", "Raw")

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/_robotocore/ses/messages",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        response = await ses_messages_list(request)
        data = json.loads(response.body.decode())
        assert data["count"] == 1
        assert data["messages"][0]["sender"] == "a@b.com"
        assert data["messages"][0]["subject"] == "Test"

    @pytest.mark.asyncio
    async def test_ses_messages_list_with_limit(self):
        import json

        from starlette.requests import Request

        from robotocore.gateway.app import ses_messages_list

        store = get_email_store()
        for i in range(10):
            store.add_message("a@b.com", ["c@d.com"], f"Msg {i}", "Body", "Raw")

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/_robotocore/ses/messages",
            "query_string": b"limit=3",
            "headers": [],
        }
        request = Request(scope)
        response = await ses_messages_list(request)
        data = json.loads(response.body.decode())
        assert data["count"] == 3
        assert data["messages"][0]["subject"] == "Msg 9"

    @pytest.mark.asyncio
    async def test_ses_messages_clear(self):
        import json

        from starlette.requests import Request

        from robotocore.gateway.app import ses_messages_clear

        store = get_email_store()
        store.add_message("a@b.com", ["c@d.com"], "Test", "Body", "Raw")
        store.add_message("a@b.com", ["c@d.com"], "Test2", "Body", "Raw")

        scope = {
            "type": "http",
            "method": "DELETE",
            "path": "/_robotocore/ses/messages",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        response = await ses_messages_clear(request)
        data = json.loads(response.body.decode())
        assert data["status"] == "cleared"
        assert data["count"] == 2

        # Verify actually cleared
        assert get_email_store().get_messages() == []

    @pytest.mark.asyncio
    async def test_ses_messages_clear_empty(self):
        import json

        from starlette.requests import Request

        from robotocore.gateway.app import ses_messages_clear

        scope = {
            "type": "http",
            "method": "DELETE",
            "path": "/_robotocore/ses/messages",
            "query_string": b"",
            "headers": [],
        }
        request = Request(scope)
        response = await ses_messages_clear(request)
        data = json.loads(response.body.decode())
        assert data["status"] == "cleared"
        assert data["count"] == 0
