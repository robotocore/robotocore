"""Handler chain for processing AWS requests through a pipeline.

Handler chain pattern:
- Request handlers run in order, can modify the request context
- Response handlers run after the service provider returns
- Exception handlers catch errors during processing
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

log = logging.getLogger(__name__)


@dataclass
class RequestContext:
    """Carries state through the handler chain for a single request."""

    request: Request
    service_name: str
    operation: str | None = None
    parsed_request: dict[str, Any] = field(default_factory=dict)
    account_id: str = "123456789012"
    region: str = "us-east-1"
    protocol: str | None = None
    response: Response | None = None


HandlerFn = Callable[[RequestContext], None]
ExceptionHandlerFn = Callable[[RequestContext, Exception], None]


class HandlerChain:
    """Executes a sequence of handlers on a request context."""

    def __init__(self) -> None:
        self.request_handlers: list[HandlerFn] = []
        self.response_handlers: list[HandlerFn] = []
        self.exception_handlers: list[ExceptionHandlerFn] = []

    def handle(self, context: RequestContext) -> None:
        stopped = False
        try:
            for handler in self.request_handlers:
                if stopped:
                    break
                handler(context)
                if context.response is not None:
                    stopped = True
        except Exception as exc:  # noqa: BLE001
            self._handle_exception(context, exc)

    def _handle_exception(self, context: RequestContext, exc: Exception) -> None:
        for eh in self.exception_handlers:
            try:
                eh(context, exc)
            except Exception:
                log.exception("Error in exception handler")
        if context.response is None:
            raise exc
