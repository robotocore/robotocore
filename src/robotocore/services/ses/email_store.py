"""In-memory store for emails sent via the SMTP server."""

import threading
import time
from dataclasses import dataclass, field


@dataclass
class StoredEmail:
    """A single email stored from SMTP delivery."""

    sender: str
    recipients: list[str]
    subject: str
    body: str
    raw: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "sender": self.sender,
            "recipients": self.recipients,
            "subject": self.subject,
            "body": self.body,
            "timestamp": self.timestamp,
        }


class EmailStore:
    """Thread-safe singleton store for SMTP-delivered emails."""

    def __init__(self) -> None:
        self._messages: list[StoredEmail] = []
        self._lock = threading.Lock()

    def add_message(
        self,
        sender: str,
        recipients: list[str],
        subject: str,
        body: str,
        raw: str,
    ) -> None:
        """Store a new email message."""
        msg = StoredEmail(
            sender=sender,
            recipients=recipients,
            subject=subject,
            body=body,
            raw=raw,
        )
        with self._lock:
            self._messages.append(msg)

    def get_messages(self, limit: int = 100) -> list[dict]:
        """Return stored messages as dicts, most recent first."""
        with self._lock:
            msgs = list(reversed(self._messages[-limit:]))
        return [m.to_dict() for m in msgs]

    def clear_messages(self) -> int:
        """Clear all stored messages. Returns count of cleared messages."""
        with self._lock:
            count = len(self._messages)
            self._messages.clear()
        return count


# Module-level singleton
_store: EmailStore | None = None
_store_lock = threading.Lock()


def get_email_store() -> EmailStore:
    """Get the global email store singleton."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = EmailStore()
    return _store
