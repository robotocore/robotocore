---
role: assistant
timestamp: "2026-03-11T21:15:00Z"
session: fix-moto-bridge-asyncio
sequence: 1
---

# Fix asyncio test in test_moto_bridge.py

## Problem
`TestHeadRequestInForwardWithBody::test_forward_to_moto_with_body_preserves_content_length_for_head`
uses `@pytest.mark.asyncio` with `async def`, but pytest-asyncio is not configured. In Python 3.12+
with pytest-xdist workers, `asyncio.get_event_loop()` raises RuntimeError.

## Fix
Replace `@pytest.mark.asyncio async def` with plain `def` using `asyncio.run()` to call the async
function. This matches the pattern used in all other async tests in the codebase.
