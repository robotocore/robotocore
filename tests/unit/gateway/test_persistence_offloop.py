"""PERSISTENCE=1 auto-save must not stall the event loop.

The auto-save runs after every request, serializes every backend and does
disk work; if it executes inline in the request coroutine, all other in-flight
requests wait for the whole snapshot. The auto-save must run on a worker
thread (the same contract the /state/save endpoints observe).
"""

import asyncio
import time

import httpx
import pytest

from robotocore.gateway.app import app

BASE_URL = "http://localhost:4566"

_SAVE_SECONDS = 1.5
_TASK_RAMP = 0.15


class _SlowManager:
    """State manager whose single debounced save blocks for _SAVE_SECONDS."""

    def __init__(self):
        # Truthy so _maybe_persist does not auto-set a directory under TMPDIR.
        self.state_dir = True
        self.calls = 0

    def save_debounced(self):
        self.calls += 1
        time.sleep(_SAVE_SECONDS)


AWS_REQUEST_HEADERS = {
    "content-type": "application/x-amz-json-1.0",
    "x-amz-target": "DynamoDB_20120810.GetItem",
    "authorization": (
        "AWS4-HMAC-SHA256 "
        "Credential=123456789012/20240101/us-east-1/dynamodb/aws4_request, "
        "SignedHeaders=host, Signature=abc"
    ),
}


@pytest.mark.asyncio
async def test_health_responds_while_a_slow_auto_save_runs(monkeypatch):
    import robotocore.state.manager as state_manager
    import os

    manager = _SlowManager()
    monkeypatch.setattr(state_manager, "get_state_manager", lambda: manager)
    monkeypatch.setenv("PERSISTENCE", "1")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url=BASE_URL) as http:

        async def do_aws():
            return (
                await http.post(
                    "/",
                    json={"TableName": "t", "Key": {"id": {"S": "x"}}},
                    headers=dict(AWS_REQUEST_HEADERS),
                )
            ).status_code

        async def do_health():
            return (await http.get("/_robotocore/health")).status_code

        overall_t0 = asyncio.get_event_loop().time()
        aws_task = asyncio.create_task(do_aws())
        await asyncio.sleep(_TASK_RAMP)  # let the AWS request reach its save
        status = await do_health()
        health_done = asyncio.get_event_loop().time()
        await aws_task
        assert status == 200
        assert manager.calls >= 1, "the slow save must actually have run in the window"
        elapsed = health_done - overall_t0
        # The health request shares the event loop with the save: off-loop the
        # total stays well under the save's own duration; inline the loop is
        # wedged until the dump completes and the probe cannot finish sooner.
        assert elapsed < _SAVE_SECONDS, (
            f"health probe took {elapsed:.2f}s while a {_SAVE_SECONDS}s auto-save "
            "was mid-flight — the save is blocking the event loop"
        )
