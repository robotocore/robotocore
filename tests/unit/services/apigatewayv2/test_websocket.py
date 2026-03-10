"""Unit tests for API Gateway V2 WebSocket transport."""

import asyncio

import pytest

from robotocore.services.apigatewayv2.provider import (
    create_connection,
    delete_connection,
    get_api_store,
    get_connection,
    post_to_connection,
)
from robotocore.services.apigatewayv2.websocket import (
    _resolve_ws_api,
    _send_queues,
    get_send_queue,
    handle_websocket,
    push_to_connection,
    reset,
)

# ---------------------------------------------------------------------------
# _resolve_ws_api
# ---------------------------------------------------------------------------


class TestResolveWsApi:
    def test_valid_path(self):
        result = _resolve_ws_api("/ws-exec/abc123/prod", "us-east-1")
        assert result == ("abc123", "prod")

    def test_default_stage(self):
        result = _resolve_ws_api("/ws-exec/myapi/$default", "us-east-1")
        assert result == ("myapi", "$default")

    def test_no_match(self):
        assert _resolve_ws_api("/v2-exec/abc/stage", "us-east-1") is None
        assert _resolve_ws_api("/foo", "us-east-1") is None
        assert _resolve_ws_api("/ws-exec/", "us-east-1") is None


# ---------------------------------------------------------------------------
# get_send_queue / push_to_connection
# ---------------------------------------------------------------------------


class TestSendQueue:
    def setup_method(self):
        reset()

    def teardown_method(self):
        reset()

    def test_get_send_queue_missing(self):
        assert get_send_queue("api1", "conn1") is None

    def test_get_send_queue_present(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[("api1", "conn1")] = q
        assert get_send_queue("api1", "conn1") is q

    @pytest.mark.asyncio
    async def test_push_to_connection_no_queue(self):
        result = await push_to_connection("api1", "conn-missing", b"hello")
        assert result is False

    @pytest.mark.asyncio
    async def test_push_to_connection_with_queue(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[("api1", "conn1")] = q
        result = await push_to_connection("api1", "conn1", b"hello")
        assert result is True
        assert q.get_nowait() == b"hello"


# ---------------------------------------------------------------------------
# post_to_connection integration with send queue
# ---------------------------------------------------------------------------


class TestPostToConnectionWithQueue:
    def setup_method(self):
        reset()
        # Register a connection in the provider store
        self.api_id = "ws-api-1"
        self.conn_id = create_connection(self.api_id, "conn-abc")

    def teardown_method(self):
        reset()
        delete_connection(self.api_id, self.conn_id)

    def test_post_to_connection_enqueues(self):
        """post_to_connection should put the message on the send queue."""
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[(self.api_id, self.conn_id)] = q

        result = post_to_connection(self.api_id, self.conn_id, b"pushed")
        assert result is True
        assert q.get_nowait() == b"pushed"

        # Also stored in connection info
        conn = get_connection(self.api_id, self.conn_id)
        assert conn is not None
        assert conn["lastMessage"] == "pushed"

    def test_post_to_connection_no_queue_still_stores(self):
        """Without a live WebSocket, message is still recorded."""
        result = post_to_connection(self.api_id, self.conn_id, b"stored")
        assert result is True
        conn = get_connection(self.api_id, self.conn_id)
        assert conn is not None
        assert conn["lastMessage"] == "stored"


# ---------------------------------------------------------------------------
# handle_websocket -- full lifecycle with mock ASGI
# ---------------------------------------------------------------------------


def _make_scope(path: str, headers: list | None = None, query_string: bytes = b""):
    return {
        "type": "websocket",
        "path": path,
        "headers": headers or [],
        "query_string": query_string,
    }


class TestHandleWebsocket:
    """Integration-style tests using mock ASGI receive/send callables."""

    def setup_method(self):
        reset()

    def teardown_method(self):
        reset()

    @pytest.mark.asyncio
    async def test_rejects_unknown_path(self):
        """Non ws-exec paths should be closed with code 4000."""
        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/bad-path"), receive, send)
        assert any(m.get("type") == "websocket.close" and m.get("code") == 4000 for m in sent)

    @pytest.mark.asyncio
    async def test_rejects_nonexistent_api(self):
        """An api_id that doesn't exist should be rejected."""
        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/noapi/prod"), receive, send)
        assert any(m.get("type") == "websocket.close" and m.get("code") == 4000 for m in sent)

    @pytest.mark.asyncio
    async def test_rejects_http_api(self):
        """An HTTP (not WEBSOCKET) API should be rejected."""
        apis = get_api_store("us-east-1")
        apis["http-api"] = {
            "ApiId": "http-api",
            "ProtocolType": "HTTP",
            "Name": "http",
        }

        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/http-api/prod"), receive, send)
        assert any(m.get("type") == "websocket.close" and m.get("code") == 4000 for m in sent)
        # cleanup
        del apis["http-api"]

    @pytest.mark.asyncio
    async def test_connect_message_disconnect_lifecycle(self):
        """Full lifecycle: connect, send message, disconnect."""
        # Register a WEBSOCKET API (no routes, so $connect returns 200 by default)
        apis = get_api_store("us-east-1")
        apis["ws-test"] = {
            "ApiId": "ws-test",
            "ProtocolType": "WEBSOCKET",
            "Name": "test-ws",
            "RouteSelectionExpression": "$request.body.action",
        }

        sent: list[dict] = []
        messages = [
            {"type": "websocket.receive", "text": '{"action":"echo","data":"hi"}'},
            {"type": "websocket.disconnect"},
        ]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/ws-test/$default"), receive, send)

        # Should have accepted the websocket
        assert sent[0] == {"type": "websocket.accept"}

        # Connection should be cleaned up
        assert ("ws-test",) not in _send_queues or all(k[0] != "ws-test" for k in _send_queues)

        # cleanup
        del apis["ws-test"]

    @pytest.mark.asyncio
    async def test_server_push_during_connection(self):
        """Verify that push_to_connection delivers messages through the WebSocket."""
        apis = get_api_store("us-east-1")
        apis["ws-push"] = {
            "ApiId": "ws-push",
            "ProtocolType": "WEBSOCKET",
            "Name": "push-test",
            "RouteSelectionExpression": "$request.body.action",
        }

        sent: list[dict] = []
        connected = asyncio.Event()
        can_disconnect = asyncio.Event()

        async def receive():
            # Wait until the test has had a chance to push a message
            if not connected.is_set():
                connected.set()
                await can_disconnect.wait()
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        async def run_ws():
            await handle_websocket(_make_scope("/ws-exec/ws-push/$default"), receive, send)

        ws_task = asyncio.create_task(run_ws())

        # Wait for the connection to be established
        await asyncio.wait_for(connected.wait(), timeout=2.0)

        # Find the connection_id
        matching = [k for k in _send_queues if k[0] == "ws-push"]
        assert len(matching) == 1, f"Expected 1 connection, got {matching}"
        api_id, conn_id = matching[0]

        # Push a message
        ok = await push_to_connection(api_id, conn_id, "hello from server")
        assert ok is True

        # Allow the websocket to disconnect
        can_disconnect.set()
        await asyncio.wait_for(ws_task, timeout=2.0)

        # Verify the push message was sent
        text_msgs = [m for m in sent if m.get("type") == "websocket.send"]
        assert len(text_msgs) == 1
        assert text_msgs[0].get("text") == "hello from server"

        # cleanup
        del apis["ws-push"]

    @pytest.mark.asyncio
    async def test_reset_clears_queues(self):
        """reset() should clear all send queues."""
        _send_queues[("a", "b")] = asyncio.Queue()
        reset()
        assert len(_send_queues) == 0
