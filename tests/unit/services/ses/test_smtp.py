"""Unit tests for SES SMTP server and EmailStore."""

import smtplib
import socket
import threading
import time
from email.mime.text import MIMEText
from unittest.mock import patch

import pytest
from aiosmtpd.controller import Controller

from robotocore.services.ses.email_store import EmailStore, get_email_store
from robotocore.services.ses.smtp_server import RobotocoreSMTPHandler, _is_sender_verified


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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

    def test_get_email_store_singleton(self):
        store1 = get_email_store()
        store2 = get_email_store()
        assert store1 is store2


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

        # Mock the SES backend to have verified identities
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
