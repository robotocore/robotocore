"""SMTP server for SES email delivery.

Runs an aiosmtpd SMTP server on port 1025 (configurable via SMTP_PORT env var).
Validates senders against SES verified identities and stores messages in the EmailStore.
"""

import asyncio
import email
import logging
import os
from email.policy import default as default_policy

from aiosmtpd.controller import Controller
from aiosmtpd.smtp import SMTP, Envelope, Session

from robotocore.services.ses.email_store import get_email_store

logger = logging.getLogger(__name__)

# Default account/region for SMTP-delivered emails
_DEFAULT_ACCOUNT_ID = "123456789012"
_DEFAULT_REGION = "us-east-1"


def _get_verified_identities(account_id: str, region: str) -> set[str]:
    """Get verified email addresses and domains from the Moto SES backend."""
    try:
        from moto.backends import get_backend

        backend = get_backend("ses")[account_id][region]
        identities: set[str] = set()
        identities.update(backend.email_identities)
        identities.update(backend.domains)
        return identities
    except Exception:  # noqa: BLE001
        logger.debug("Could not load SES identities, allowing all senders")
        return set()


def _is_sender_verified(sender: str, identities: set[str]) -> bool:
    """Check if the sender is verified (by exact email or domain match)."""
    if not identities:
        # If no identities are configured, allow everything (dev convenience)
        return True

    # Exact email match
    if sender in identities:
        return True

    # Domain match: extract domain from sender address
    if "@" in sender:
        domain = sender.split("@", 1)[1].lower()
        for identity in identities:
            if identity.lower() == domain:
                return True

    return False


class RobotocoreSMTPHandler:
    """aiosmtpd handler that validates senders and stores messages."""

    def __init__(self, account_id: str = _DEFAULT_ACCOUNT_ID, region: str = _DEFAULT_REGION):
        self.account_id = account_id
        self.region = region

    async def handle_RCPT(  # noqa: N802 — aiosmtpd requires this name
        self,
        server: SMTP,
        session: Session,
        envelope: Envelope,
        address: str,
        rcpt_options: list[str],
    ) -> str:
        """Accept all recipients."""
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(  # noqa: N802 — aiosmtpd requires this name
        self, server: SMTP, session: Session, envelope: Envelope
    ) -> str:
        """Process incoming email: validate sender and store."""
        sender = envelope.mail_from or ""

        # Validate sender against verified identities
        identities = _get_verified_identities(self.account_id, self.region)
        if identities and not _is_sender_verified(sender, identities):
            logger.warning("SMTP rejected: sender %s not verified", sender)
            return "554 Message rejected: Email address is not verified"

        # Parse the email message
        if isinstance(envelope.content, str):
            raw_data = envelope.content
        else:
            raw_data = envelope.content.decode("utf-8", errors="replace")
        msg = email.message_from_string(raw_data, policy=default_policy)

        subject = msg.get("Subject", "")
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    payload = part.get_content()
                    if isinstance(payload, str):
                        body = payload
                    break
        else:
            payload = msg.get_content()
            if isinstance(payload, str):
                body = payload

        recipients = list(envelope.rcpt_tos)
        store = get_email_store()
        store.add_message(
            sender=sender,
            recipients=recipients,
            subject=subject,
            body=body,
            raw=raw_data,
        )

        logger.info("SMTP received: from=%s to=%s subject=%s", sender, recipients, subject)
        return "250 OK"


# Global controller reference for shutdown
_controller: Controller | None = None


def start_smtp_server() -> Controller | None:
    """Start the SMTP server in a background thread.

    Returns the Controller instance (or None if disabled).
    The server runs in a daemon thread and will be stopped when the process exits.
    """
    global _controller

    port = int(os.environ.get("SMTP_PORT", "1025"))
    if os.environ.get("SMTP_DISABLED", "0") == "1":
        logger.info("SMTP server disabled via SMTP_DISABLED=1")
        return None

    account_id = os.environ.get("DEFAULT_ACCOUNT_ID", _DEFAULT_ACCOUNT_ID)
    region = os.environ.get("DEFAULT_REGION", _DEFAULT_REGION)

    handler = RobotocoreSMTPHandler(account_id=account_id, region=region)
    _controller = Controller(handler, hostname="0.0.0.0", port=port)
    _controller.start()
    logger.info("SMTP server started on port %d", port)
    return _controller


def stop_smtp_server() -> None:
    """Stop the SMTP server if running."""
    global _controller
    if _controller is not None:
        _controller.stop()
        _controller = None
        logger.info("SMTP server stopped")


async def start_smtp_server_async() -> None:
    """Start SMTP server from an async context (for use in Starlette on_startup)."""
    await asyncio.to_thread(start_smtp_server)
