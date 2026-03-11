"""Per-request context for correlating chaos, audit, and observability events."""

import contextvars
import time
import uuid
from dataclasses import dataclass, field

_current_request: contextvars.ContextVar["RequestContext | None"] = contextvars.ContextVar(
    "request_context", default=None
)


@dataclass
class RequestContext:
    """Tracks observability state for a single request across middleware layers."""

    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    service: str = ""
    operation: str = ""
    start_time: float = field(default_factory=time.monotonic)
    chaos_applied: list[dict] = field(default_factory=list)

    @property
    def elapsed_ms(self) -> float:
        """Milliseconds since request started."""
        return (time.monotonic() - self.start_time) * 1000


def get_current_context() -> "RequestContext | None":
    """Return the current request's observability context, or None."""
    return _current_request.get()


def set_current_context(ctx: "RequestContext") -> None:
    """Set the observability context for the current request."""
    _current_request.set(ctx)
