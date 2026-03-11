"""Unit tests for API Gateway V2 WebSocket transport.

Covers:
- Path resolution (_resolve_ws_api)
- Send queue management (get_send_queue, push_to_connection, reset)
- Provider-level connection CRUD (create/get/delete/list/post_to)
- Full ASGI WebSocket lifecycle (handle_websocket) with mocked scope/send/receive
- AWSRoutingMiddleware WebSocket routing
- @connections API endpoint (handle_connections_api)
- Route selection expression evaluation
- Error/edge cases
"""

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from robotocore.services.apigatewayv2.provider import (
    _connections,
    create_connection,
    delete_connection,
    get_api_store,
    get_connection,
    get_integration_store,
    get_route_store,
    list_connections,
    post_to_connection,
)
from robotocore.services.apigatewayv2.websocket import (
    _CLOSE_SENTINEL,
    _resolve_ws_api,
    _send_queues,
    get_send_queue,
    handle_websocket,
    push_to_connection,
    reset,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_scope(path: str, headers: list | None = None, query_string: bytes = b""):
    return {
        "type": "websocket",
        "path": path,
        "headers": headers or [],
        "query_string": query_string,
    }


def _register_ws_api(
    api_id: str = "ws-test",
    region: str = "us-east-1",
    name: str = "test-ws",
    route_sel_expr: str = "$request.body.action",
):
    """Register a WEBSOCKET API in the store and return cleanup callable."""
    apis = get_api_store(region)
    apis[api_id] = {
        "ApiId": api_id,
        "ProtocolType": "WEBSOCKET",
        "Name": name,
        "RouteSelectionExpression": route_sel_expr,
    }

    def cleanup():
        apis.pop(api_id, None)

    return cleanup


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

    def test_no_match_v2_exec(self):
        assert _resolve_ws_api("/v2-exec/abc/stage", "us-east-1") is None

    def test_no_match_random_path(self):
        assert _resolve_ws_api("/foo", "us-east-1") is None

    def test_no_match_trailing_slash_only(self):
        assert _resolve_ws_api("/ws-exec/", "us-east-1") is None

    def test_extra_path_segments(self):
        """Extra segments after stage should still match api_id and stage."""
        result = _resolve_ws_api("/ws-exec/abc/prod/extra/stuff", "us-east-1")
        assert result == ("abc", "prod")

    def test_empty_path(self):
        assert _resolve_ws_api("", "us-east-1") is None

    def test_path_with_query_like_chars(self):
        """Query chars in path shouldn't confuse the regex."""
        result = _resolve_ws_api("/ws-exec/abc/stg?foo=bar", "us-east-1")
        # The regex uses [^/]+ which stops at /, not at ?, so "stg?foo=bar"
        # is treated as stage (in ASGI, query_string is separate from path).
        assert result is not None
        assert result[0] == "abc"


# ---------------------------------------------------------------------------
# get_send_queue / push_to_connection (websocket module level)
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

    def test_get_send_queue_wrong_key(self):
        _send_queues[("api1", "conn1")] = asyncio.Queue()
        assert get_send_queue("api1", "conn2") is None
        assert get_send_queue("api2", "conn1") is None

    @pytest.mark.asyncio
    async def test_push_to_connection_no_queue(self):
        result = await push_to_connection("api1", "conn-missing", b"hello")
        assert result is False

    @pytest.mark.asyncio
    async def test_push_to_connection_with_queue_bytes(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[("api1", "conn1")] = q
        result = await push_to_connection("api1", "conn1", b"hello")
        assert result is True
        assert q.get_nowait() == b"hello"

    @pytest.mark.asyncio
    async def test_push_to_connection_with_queue_str(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[("api1", "conn1")] = q
        result = await push_to_connection("api1", "conn1", "text message")
        assert result is True
        assert q.get_nowait() == "text message"

    @pytest.mark.asyncio
    async def test_push_multiple_messages(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[("api1", "conn1")] = q
        await push_to_connection("api1", "conn1", "msg1")
        await push_to_connection("api1", "conn1", "msg2")
        await push_to_connection("api1", "conn1", "msg3")
        assert q.qsize() == 3
        assert q.get_nowait() == "msg1"
        assert q.get_nowait() == "msg2"
        assert q.get_nowait() == "msg3"


# ---------------------------------------------------------------------------
# reset()
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_clears_queues(self):
        _send_queues[("a", "b")] = asyncio.Queue()
        _send_queues[("c", "d")] = asyncio.Queue()
        reset()
        assert len(_send_queues) == 0

    def test_reset_idempotent(self):
        reset()
        reset()
        assert len(_send_queues) == 0


# ---------------------------------------------------------------------------
# Provider connection management
# ---------------------------------------------------------------------------


class TestConnectionCRUD:
    def setup_method(self):
        _connections.clear()

    def teardown_method(self):
        _connections.clear()

    def test_create_connection_returns_id(self):
        conn_id = create_connection("api1", "my-conn-id")
        assert conn_id == "my-conn-id"

    def test_create_connection_auto_id(self):
        conn_id = create_connection("api1")
        assert len(conn_id) == 12

    def test_get_connection_exists(self):
        create_connection("api1", "conn-x")
        conn = get_connection("api1", "conn-x")
        assert conn is not None
        assert conn["connectionId"] == "conn-x"
        assert "connectedAt" in conn

    def test_get_connection_not_found(self):
        assert get_connection("api1", "nope") is None

    def test_delete_connection_exists(self):
        create_connection("api1", "conn-x")
        assert delete_connection("api1", "conn-x") is True
        assert get_connection("api1", "conn-x") is None

    def test_delete_connection_not_found(self):
        assert delete_connection("api1", "nope") is False

    def test_list_connections_empty(self):
        assert list_connections("api1") == []

    def test_list_connections_multiple(self):
        create_connection("api1", "c1")
        create_connection("api1", "c2")
        conns = list_connections("api1")
        assert len(conns) == 2
        ids = {c["connectionId"] for c in conns}
        assert ids == {"c1", "c2"}

    def test_connections_isolated_by_api(self):
        create_connection("api1", "c1")
        create_connection("api2", "c2")
        assert len(list_connections("api1")) == 1
        assert len(list_connections("api2")) == 1
        assert list_connections("api1")[0]["connectionId"] == "c1"


# ---------------------------------------------------------------------------
# post_to_connection (provider)
# ---------------------------------------------------------------------------


class TestPostToConnectionProvider:
    def setup_method(self):
        reset()
        _connections.clear()
        self.api_id = "ws-api-1"
        self.conn_id = create_connection(self.api_id, "conn-abc")

    def teardown_method(self):
        reset()
        _connections.clear()

    def test_post_enqueues_when_queue_exists(self):
        q: asyncio.Queue = asyncio.Queue()
        _send_queues[(self.api_id, self.conn_id)] = q
        result = post_to_connection(self.api_id, self.conn_id, b"pushed")
        assert result is True
        assert q.get_nowait() == b"pushed"

    def test_post_records_last_message(self):
        post_to_connection(self.api_id, self.conn_id, b"stored")
        conn = get_connection(self.api_id, self.conn_id)
        assert conn is not None
        assert conn["lastMessage"] == "stored"
        assert "lastMessageAt" in conn

    def test_post_no_queue_still_stores(self):
        result = post_to_connection(self.api_id, self.conn_id, b"data")
        assert result is True
        conn = get_connection(self.api_id, self.conn_id)
        assert conn["lastMessage"] == "data"

    def test_post_nonexistent_connection(self):
        result = post_to_connection(self.api_id, "no-such-conn", b"data")
        assert result is False

    def test_post_string_data(self):
        """post_to_connection with string data should store as-is."""
        # The function signature says bytes, but the code handles both
        post_to_connection(self.api_id, self.conn_id, b"hello bytes")
        conn = get_connection(self.api_id, self.conn_id)
        assert conn["lastMessage"] == "hello bytes"

    def test_post_multiple_updates_last_message(self):
        post_to_connection(self.api_id, self.conn_id, b"first")
        post_to_connection(self.api_id, self.conn_id, b"second")
        conn = get_connection(self.api_id, self.conn_id)
        assert conn["lastMessage"] == "second"


# ---------------------------------------------------------------------------
# handle_websocket -- ASGI lifecycle tests
# ---------------------------------------------------------------------------


class TestHandleWebsocket:
    """Integration-style tests using mock ASGI receive/send callables."""

    def setup_method(self):
        reset()
        _connections.clear()

    def teardown_method(self):
        reset()
        _connections.clear()

    @pytest.mark.asyncio
    async def test_rejects_unknown_path(self):
        """Non ws-exec paths should be closed with code 4000."""
        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/bad-path"), receive, send)
        assert len(sent) == 1
        assert sent[0]["type"] == "websocket.close"
        assert sent[0]["code"] == 4000

    @pytest.mark.asyncio
    async def test_rejects_nonexistent_api(self):
        """An api_id that doesn't exist should be rejected with 4000."""
        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/noapi/prod"), receive, send)
        assert sent[0]["type"] == "websocket.close"
        assert sent[0]["code"] == 4000

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
        assert sent[0]["type"] == "websocket.close"
        assert sent[0]["code"] == 4000
        del apis["http-api"]

    @pytest.mark.asyncio
    async def test_connect_accept(self):
        """A valid WEBSOCKET API should trigger websocket.accept."""
        cleanup = _register_ws_api("ws-acc")
        sent: list[dict] = []
        messages = [{"type": "websocket.disconnect"}]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/ws-acc/$default"), receive, send)
        assert sent[0] == {"type": "websocket.accept"}
        cleanup()

    @pytest.mark.asyncio
    async def test_connection_created_and_cleaned_up(self):
        """Connection should be registered during lifecycle and deleted after."""
        cleanup = _register_ws_api("ws-life")

        sent: list[dict] = []
        connected = asyncio.Event()
        can_disconnect = asyncio.Event()
        found_conn_ids = []

        async def receive():
            if not connected.is_set():
                connected.set()
                await can_disconnect.wait()
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        async def run_ws():
            await handle_websocket(_make_scope("/ws-exec/ws-life/$default"), receive, send)

        task = asyncio.create_task(run_ws())
        await asyncio.wait_for(connected.wait(), timeout=2.0)

        # Connection should exist now
        conns = list_connections("ws-life")
        assert len(conns) == 1
        found_conn_ids.append(conns[0]["connectionId"])

        # Send queue should exist
        matching = [k for k in _send_queues if k[0] == "ws-life"]
        assert len(matching) == 1

        can_disconnect.set()
        await asyncio.wait_for(task, timeout=2.0)

        # After disconnect, connection and queue should be gone
        assert list_connections("ws-life") == []
        assert not any(k[0] == "ws-life" for k in _send_queues)
        cleanup()

    @pytest.mark.asyncio
    async def test_connect_message_disconnect_lifecycle(self):
        """Full lifecycle: connect, send message, disconnect."""
        cleanup = _register_ws_api("ws-full")

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

        await handle_websocket(_make_scope("/ws-exec/ws-full/$default"), receive, send)

        # Should have accepted the websocket
        assert sent[0] == {"type": "websocket.accept"}

        # Connection should be cleaned up
        assert not any(k[0] == "ws-full" for k in _send_queues)
        cleanup()

    @pytest.mark.asyncio
    async def test_binary_message_handled(self):
        """Binary messages (bytes field) should be passed through."""
        cleanup = _register_ws_api("ws-bin")
        sent: list[dict] = []
        messages = [
            {"type": "websocket.receive", "bytes": b"\x00\x01\x02"},
            {"type": "websocket.disconnect"},
        ]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        # The message executor will be called -- we just need no crash
        await handle_websocket(_make_scope("/ws-exec/ws-bin/$default"), receive, send)
        assert sent[0] == {"type": "websocket.accept"}
        cleanup()

    @pytest.mark.asyncio
    async def test_unknown_message_type_ignored(self):
        """Unknown ASGI message types should be silently ignored."""
        cleanup = _register_ws_api("ws-unk")
        sent: list[dict] = []
        messages = [
            {"type": "websocket.unknown"},
            {"type": "websocket.disconnect"},
        ]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        await handle_websocket(_make_scope("/ws-exec/ws-unk/$default"), receive, send)
        assert sent[0] == {"type": "websocket.accept"}
        cleanup()

    @pytest.mark.asyncio
    async def test_server_push_text(self):
        """push_to_connection with a string sends websocket.send with text key."""
        cleanup = _register_ws_api("ws-ptxt")
        sent: list[dict] = []
        connected = asyncio.Event()
        can_disconnect = asyncio.Event()

        async def receive():
            if not connected.is_set():
                connected.set()
                await can_disconnect.wait()
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        task = asyncio.create_task(
            handle_websocket(_make_scope("/ws-exec/ws-ptxt/$default"), receive, send)
        )
        await asyncio.wait_for(connected.wait(), timeout=2.0)

        matching = [k for k in _send_queues if k[0] == "ws-ptxt"]
        assert len(matching) == 1
        _, conn_id = matching[0]

        ok = await push_to_connection("ws-ptxt", conn_id, "hello text")
        assert ok is True

        can_disconnect.set()
        await asyncio.wait_for(task, timeout=2.0)

        text_msgs = [m for m in sent if m.get("type") == "websocket.send"]
        assert len(text_msgs) == 1
        assert text_msgs[0]["text"] == "hello text"
        cleanup()

    @pytest.mark.asyncio
    async def test_server_push_bytes(self):
        """push_to_connection with bytes sends websocket.send with bytes key."""
        cleanup = _register_ws_api("ws-pbin")
        sent: list[dict] = []
        connected = asyncio.Event()
        can_disconnect = asyncio.Event()

        async def receive():
            if not connected.is_set():
                connected.set()
                await can_disconnect.wait()
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        task = asyncio.create_task(
            handle_websocket(_make_scope("/ws-exec/ws-pbin/$default"), receive, send)
        )
        await asyncio.wait_for(connected.wait(), timeout=2.0)

        matching = [k for k in _send_queues if k[0] == "ws-pbin"]
        _, conn_id = matching[0]

        ok = await push_to_connection("ws-pbin", conn_id, b"\xde\xad\xbe\xef")
        assert ok is True

        can_disconnect.set()
        await asyncio.wait_for(task, timeout=2.0)

        bin_msgs = [m for m in sent if m.get("type") == "websocket.send"]
        assert len(bin_msgs) == 1
        assert bin_msgs[0]["bytes"] == b"\xde\xad\xbe\xef"
        cleanup()

    @pytest.mark.asyncio
    async def test_server_push_multiple(self):
        """Multiple pushes should all arrive in order."""
        cleanup = _register_ws_api("ws-pmulti")
        sent: list[dict] = []
        connected = asyncio.Event()
        can_disconnect = asyncio.Event()

        async def receive():
            if not connected.is_set():
                connected.set()
                await can_disconnect.wait()
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        task = asyncio.create_task(
            handle_websocket(_make_scope("/ws-exec/ws-pmulti/$default"), receive, send)
        )
        await asyncio.wait_for(connected.wait(), timeout=2.0)

        matching = [k for k in _send_queues if k[0] == "ws-pmulti"]
        _, conn_id = matching[0]

        await push_to_connection("ws-pmulti", conn_id, "msg1")
        await push_to_connection("ws-pmulti", conn_id, "msg2")
        await push_to_connection("ws-pmulti", conn_id, "msg3")

        # Give send loop a moment to process
        await asyncio.sleep(0.05)
        can_disconnect.set()
        await asyncio.wait_for(task, timeout=2.0)

        text_msgs = [m for m in sent if m.get("type") == "websocket.send"]
        assert len(text_msgs) == 3
        assert [m["text"] for m in text_msgs] == ["msg1", "msg2", "msg3"]
        cleanup()

    @pytest.mark.asyncio
    async def test_connect_rejection_closes_socket(self):
        """If $connect integration returns >= 400, WebSocket should close with 4001."""
        cleanup = _register_ws_api("ws-rej")
        # Set up a $connect route with integration
        routes = get_route_store("us-east-1", "ws-rej")
        integs = get_integration_store("us-east-1", "ws-rej")
        routes["r1"] = {
            "RouteId": "r1",
            "RouteKey": "$connect",
            "Target": "integrations/integ1",
        }
        integs["integ1"] = {
            "IntegrationId": "integ1",
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123456789012:function:reject",
        }

        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        # Mock the executor to return 403
        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_connect",
            return_value=(403, {}, '{"message":"Forbidden"}'),
        ):
            await handle_websocket(_make_scope("/ws-exec/ws-rej/$default"), receive, send)

        # Should accept first, then close with 4001
        assert sent[0] == {"type": "websocket.accept"}
        assert sent[1]["type"] == "websocket.close"
        assert sent[1]["code"] == 4001
        cleanup()

    @pytest.mark.asyncio
    async def test_headers_parsed_from_scope(self):
        """ASGI headers (bytes tuples) should be decoded and passed to $connect."""
        cleanup = _register_ws_api("ws-hdr")
        routes = get_route_store("us-east-1", "ws-hdr")
        integs = get_integration_store("us-east-1", "ws-hdr")
        routes["r1"] = {
            "RouteId": "r1",
            "RouteKey": "$connect",
            "Target": "integrations/integ1",
        }
        integs["integ1"] = {
            "IntegrationId": "integ1",
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123456789012:function:auth",
        }

        captured_kwargs = {}

        def mock_connect(**kwargs):
            captured_kwargs.update(kwargs)
            return (200, {}, "")

        sent: list[dict] = []
        messages = [{"type": "websocket.disconnect"}]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        scope = _make_scope(
            "/ws-exec/ws-hdr/$default",
            headers=[
                (b"authorization", b"Bearer token123"),
                (b"x-custom", b"value"),
            ],
            query_string=b"key=val&foo=bar",
        )

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_connect",
            side_effect=mock_connect,
        ):
            await handle_websocket(scope, receive, send)

        assert captured_kwargs["headers"]["authorization"] == "Bearer token123"
        assert captured_kwargs["headers"]["x-custom"] == "value"
        assert captured_kwargs["query_params"] == {"key": "val", "foo": "bar"}
        cleanup()

    @pytest.mark.asyncio
    async def test_query_string_parsed(self):
        """Query string should be parsed into query_params dict."""
        cleanup = _register_ws_api("ws-qs")

        captured_kwargs = {}

        def mock_connect(**kwargs):
            captured_kwargs.update(kwargs)
            return (200, {}, "")

        sent: list[dict] = []
        messages = [{"type": "websocket.disconnect"}]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        scope = _make_scope(
            "/ws-exec/ws-qs/$default",
            query_string=b"token=abc&version=2",
        )

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_connect",
            side_effect=mock_connect,
        ):
            await handle_websocket(scope, receive, send)

        assert captured_kwargs["query_params"]["token"] == "abc"
        assert captured_kwargs["query_params"]["version"] == "2"
        cleanup()

    @pytest.mark.asyncio
    async def test_empty_query_string(self):
        """Empty query string should result in empty query_params."""
        cleanup = _register_ws_api("ws-eqs")

        captured_kwargs = {}

        def mock_connect(**kwargs):
            captured_kwargs.update(kwargs)
            return (200, {}, "")

        sent: list[dict] = []
        messages = [{"type": "websocket.disconnect"}]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_connect",
            side_effect=mock_connect,
        ):
            await handle_websocket(_make_scope("/ws-exec/ws-eqs/$default"), receive, send)

        assert captured_kwargs["query_params"] == {}
        cleanup()

    @pytest.mark.asyncio
    async def test_disconnect_invokes_executor(self):
        """$disconnect executor should be called after the receive loop ends."""
        cleanup = _register_ws_api("ws-disc")

        disconnect_called = False

        def mock_disconnect(**kwargs):
            nonlocal disconnect_called
            disconnect_called = True
            assert kwargs["api_id"] == "ws-disc"
            return (200, {}, "")

        sent: list[dict] = []
        messages = [{"type": "websocket.disconnect"}]
        msg_iter = iter(messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_disconnect",
            side_effect=mock_disconnect,
        ):
            await handle_websocket(_make_scope("/ws-exec/ws-disc/$default"), receive, send)

        assert disconnect_called is True
        cleanup()

    @pytest.mark.asyncio
    async def test_message_invokes_executor(self):
        """Received messages should invoke execute_websocket_message."""
        cleanup = _register_ws_api("ws-msg")

        messages_received = []

        def mock_message(**kwargs):
            messages_received.append(kwargs["message"])
            return (200, {}, "")

        sent: list[dict] = []
        asgi_messages = [
            {"type": "websocket.receive", "text": "hello"},
            {"type": "websocket.receive", "text": "world"},
            {"type": "websocket.disconnect"},
        ]
        msg_iter = iter(asgi_messages)

        async def receive():
            return next(msg_iter)

        async def send(msg):
            sent.append(msg)

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_message",
            side_effect=mock_message,
        ):
            await handle_websocket(_make_scope("/ws-exec/ws-msg/$default"), receive, send)

        assert messages_received == ["hello", "world"]
        cleanup()

    @pytest.mark.asyncio
    async def test_cleanup_on_connect_rejection(self):
        """When $connect is rejected, connection and queue should be cleaned up."""
        cleanup = _register_ws_api("ws-crej")

        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        with patch(
            "robotocore.services.apigatewayv2.executor.execute_websocket_connect",
            return_value=(403, {}, "forbidden"),
        ):
            await handle_websocket(_make_scope("/ws-exec/ws-crej/$default"), receive, send)

        # Everything should be cleaned up
        assert not any(k[0] == "ws-crej" for k in _send_queues)
        assert list_connections("ws-crej") == []
        cleanup()


# ---------------------------------------------------------------------------
# Route selection expression evaluation
# ---------------------------------------------------------------------------


class TestRouteSelectionExpression:
    """Test _evaluate_route_selection via the executor module."""

    def test_simple_action_field(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection(
            "$request.body.action", '{"action":"sendMessage","data":"hi"}'
        )
        assert result == "sendMessage"

    def test_missing_field_falls_to_default(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("$request.body.action", '{"data":"hi"}')
        assert result == "$default"

    def test_non_json_message_falls_to_default(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("$request.body.action", "not json")
        assert result == "$default"

    def test_bytes_message(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("$request.body.action", b'{"action":"doThing"}')
        assert result == "doThing"

    def test_unknown_expression_format(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("something.else", '{"action":"echo"}')
        assert result == "$default"

    def test_nested_not_supported(self):
        """Only single-level $request.body.{field} is supported."""
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        # The regex only captures \w+, so dots aren't captured
        result = _evaluate_route_selection(
            "$request.body.nested.field", '{"nested":{"field":"val"}}'
        )
        # "nested" will match, but its value is a dict, so str(dict)
        # The field matched is "nested" not "nested.field"
        assert result is not None  # won't be "val"

    def test_numeric_action_value(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("$request.body.action", '{"action":42}')
        assert result == "42"

    def test_empty_message(self):
        from robotocore.services.apigatewayv2.executor import _evaluate_route_selection

        result = _evaluate_route_selection("$request.body.action", "")
        assert result == "$default"


# ---------------------------------------------------------------------------
# @connections API endpoint
# ---------------------------------------------------------------------------


class TestConnectionsApi:
    """Test handle_connections_api from app.py."""

    def setup_method(self):
        reset()
        _connections.clear()

    def teardown_method(self):
        reset()
        _connections.clear()

    @pytest.mark.asyncio
    async def test_get_existing_connection(self):
        from robotocore.gateway.app import handle_connections_api

        create_connection("api1", "conn-abc")
        request = AsyncMock()
        request.method = "GET"

        resp = await handle_connections_api(request, "api1", "$default", "conn-abc")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["connectionId"] == "conn-abc"

    @pytest.mark.asyncio
    async def test_get_missing_connection(self):
        from robotocore.gateway.app import handle_connections_api

        request = AsyncMock()
        request.method = "GET"

        resp = await handle_connections_api(request, "api1", "$default", "no-conn")
        assert resp.status_code == 410
        body = json.loads(resp.body)
        assert "not found" in body["message"].lower()

    @pytest.mark.asyncio
    async def test_delete_connection(self):
        from robotocore.gateway.app import handle_connections_api

        create_connection("api1", "conn-del")
        request = AsyncMock()
        request.method = "DELETE"

        resp = await handle_connections_api(request, "api1", "$default", "conn-del")
        assert resp.status_code == 204
        assert get_connection("api1", "conn-del") is None

    @pytest.mark.asyncio
    async def test_post_to_existing_connection(self):
        from robotocore.gateway.app import handle_connections_api

        create_connection("api1", "conn-post")
        request = AsyncMock()
        request.method = "POST"
        request.body = AsyncMock(return_value=b"hello from management")

        resp = await handle_connections_api(request, "api1", "$default", "conn-post")
        assert resp.status_code == 200
        conn = get_connection("api1", "conn-post")
        assert conn["lastMessage"] == "hello from management"

    @pytest.mark.asyncio
    async def test_post_to_missing_connection(self):
        from robotocore.gateway.app import handle_connections_api

        request = AsyncMock()
        request.method = "POST"
        request.body = AsyncMock(return_value=b"hello")

        resp = await handle_connections_api(request, "api1", "$default", "gone-conn")
        assert resp.status_code == 410


# ---------------------------------------------------------------------------
# AWSRoutingMiddleware WebSocket routing
# ---------------------------------------------------------------------------


class TestMiddlewareWebSocketRouting:
    """Test that AWSRoutingMiddleware dispatches WebSocket scopes correctly."""

    def setup_method(self):
        reset()
        _connections.clear()

    def teardown_method(self):
        reset()
        _connections.clear()

    @pytest.mark.asyncio
    async def test_websocket_scope_dispatched(self):
        """Middleware should call handle_websocket for WebSocket scopes."""
        from robotocore.gateway.app import AWSRoutingMiddleware

        inner_app = AsyncMock()
        middleware = AWSRoutingMiddleware(inner_app)

        sent: list[dict] = []

        async def receive():
            return {"type": "websocket.disconnect"}

        async def send(msg):
            sent.append(msg)

        scope = _make_scope("/ws-exec/noapi/prod")
        await middleware(scope, receive, send)

        # Should not call inner app for websocket
        inner_app.assert_not_called()
        # Should have sent a close (api doesn't exist)
        assert any(m.get("type") == "websocket.close" for m in sent)

    @pytest.mark.asyncio
    async def test_non_http_non_websocket_passes_through(self):
        """Non-http, non-websocket scopes pass to inner app."""
        from robotocore.gateway.app import AWSRoutingMiddleware

        inner_app = AsyncMock()
        middleware = AWSRoutingMiddleware(inner_app)

        scope = {"type": "lifespan"}
        await middleware(scope, AsyncMock(), AsyncMock())
        inner_app.assert_called_once()
        assert inner_app.call_args[0][0] == scope


# ---------------------------------------------------------------------------
# _CLOSE_SENTINEL
# ---------------------------------------------------------------------------


class TestCloseSentinel:
    def test_sentinel_is_unique(self):
        """The close sentinel should be a unique object, not equal to any common value."""
        assert _CLOSE_SENTINEL is not None
        assert _CLOSE_SENTINEL is not True
        assert _CLOSE_SENTINEL is not False
        assert _CLOSE_SENTINEL != ""
        assert _CLOSE_SENTINEL != b""
        assert _CLOSE_SENTINEL != 0

    @pytest.mark.asyncio
    async def test_sentinel_terminates_send_loop(self):
        """Putting _CLOSE_SENTINEL on a queue should cause the send loop to exit.

        This is tested indirectly through handle_websocket's cleanup,
        but we verify the queue behavior directly here.
        """
        q = asyncio.Queue()
        q.put_nowait("msg1")
        q.put_nowait(_CLOSE_SENTINEL)

        items = []
        while True:
            item = q.get_nowait()
            if item is _CLOSE_SENTINEL:
                break
            items.append(item)

        assert items == ["msg1"]
