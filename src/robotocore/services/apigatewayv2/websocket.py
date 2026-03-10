"""WebSocket transport for API Gateway V2 WebSocket APIs.

Handles actual WebSocket connections over ASGI, bridging them to the
existing executor layer (connect/message/disconnect lifecycle) and
enabling server-to-client pushes via the @connections POST API.
"""

import asyncio
import logging
import re
import uuid

logger = logging.getLogger(__name__)

# Maps (api_id, connection_id) -> asyncio.Queue for outbound messages.
# The queue is consumed by the per-connection ASGI send loop.
_send_queues: dict[tuple[str, str], asyncio.Queue] = {}

# Sentinel used to signal the send loop to close the WebSocket.
_CLOSE_SENTINEL = object()


def get_send_queue(api_id: str, connection_id: str) -> asyncio.Queue | None:
    """Return the outbound queue for a live connection, or None."""
    return _send_queues.get((api_id, connection_id))


async def push_to_connection(api_id: str, connection_id: str, data: bytes | str) -> bool:
    """Push a message to a live WebSocket connection.

    Called by the @connections POST handler to deliver server-to-client
    messages.  Returns True if the connection is alive and the message was
    enqueued.
    """
    queue = _send_queues.get((api_id, connection_id))
    if queue is None:
        return False
    await queue.put(data)
    return True


def _resolve_ws_api(path: str, region: str) -> tuple[str, str] | None:
    """Match a WebSocket upgrade path to (api_id, stage).

    Convention: ``/ws-exec/{api_id}/{stage}``
    """
    match = re.match(r"^/ws-exec/([^/]+)/([^/]+)", path)
    if match:
        return match.group(1), match.group(2)
    return None


async def handle_websocket(scope: dict, receive, send) -> None:
    """Full ASGI WebSocket handler for API Gateway V2 WebSocket APIs.

    Lifecycle:
      1. Accept the WebSocket handshake
      2. Generate a connection ID, register it
      3. Invoke the $connect route integration
      4. Loop: receive messages and invoke the matched route integration
      5. On close, invoke $disconnect and clean up
    """
    from robotocore.services.apigatewayv2.executor import (
        execute_websocket_connect,
        execute_websocket_disconnect,
        execute_websocket_message,
    )
    from robotocore.services.apigatewayv2.provider import (
        create_connection,
        delete_connection,
        get_api_store,
    )

    path = scope.get("path", "")
    # ASGI headers are list of (name_bytes, value_bytes) pairs.
    str_headers = {}
    for k, v in scope.get("headers") or []:
        name = k.decode("latin-1") if isinstance(k, bytes) else k
        val = v.decode("latin-1") if isinstance(v, bytes) else v
        str_headers[name] = val

    query_string = scope.get("query_string", b"").decode("latin-1")
    query_params: dict[str, str] = {}
    if query_string:
        for pair in query_string.split("&"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                query_params[k] = v

    # Determine region from headers (fallback us-east-1)
    region = "us-east-1"
    account_id = "123456789012"

    resolved = _resolve_ws_api(path, region)
    if resolved is None:
        # Not a recognised WebSocket path -- reject.
        await send({"type": "websocket.close", "code": 4000})
        return

    api_id, stage = resolved

    # Verify the API exists and is a WEBSOCKET type
    apis = get_api_store(region)
    api = apis.get(api_id)
    if not api or api.get("ProtocolType", "").upper() != "WEBSOCKET":
        await send({"type": "websocket.close", "code": 4000})
        return

    # Accept the WebSocket handshake
    await send({"type": "websocket.accept"})

    connection_id = uuid.uuid4().hex[:12]
    create_connection(api_id, connection_id)

    # Set up the outbound queue for server-to-client pushes
    queue: asyncio.Queue = asyncio.Queue()
    _send_queues[(api_id, connection_id)] = queue

    try:
        # Invoke $connect integration
        status, _, _ = execute_websocket_connect(
            api_id=api_id,
            connection_id=connection_id,
            headers=str_headers,
            query_params=query_params,
            region=region,
            account_id=account_id,
        )
        if status >= 400:
            # $connect rejected -- close
            await send({"type": "websocket.close", "code": 4001})
            return

        # Spin up a task that drains the outbound queue
        async def _send_loop():
            while True:
                item = await queue.get()
                if item is _CLOSE_SENTINEL:
                    break
                payload = item if isinstance(item, (bytes, bytearray)) else str(item)
                if isinstance(payload, bytes):
                    await send({"type": "websocket.send", "bytes": payload})
                else:
                    await send({"type": "websocket.send", "text": payload})

        send_task = asyncio.create_task(_send_loop())

        # Main receive loop
        try:
            while True:
                message = await receive()
                msg_type = message.get("type", "")

                if msg_type == "websocket.receive":
                    data = message.get("text") or message.get("bytes", b"")
                    execute_websocket_message(
                        api_id=api_id,
                        connection_id=connection_id,
                        message=data,
                        region=region,
                        account_id=account_id,
                    )
                elif msg_type == "websocket.disconnect":
                    break
                else:
                    # Unknown message type -- ignore
                    continue
        finally:
            # Signal the send loop to stop
            await queue.put(_CLOSE_SENTINEL)
            await send_task

        # Invoke $disconnect
        execute_websocket_disconnect(
            api_id=api_id,
            connection_id=connection_id,
            region=region,
            account_id=account_id,
        )
    finally:
        _send_queues.pop((api_id, connection_id), None)
        delete_connection(api_id, connection_id)


def reset() -> None:
    """Clear all send queues (used by tests / state reset)."""
    _send_queues.clear()
