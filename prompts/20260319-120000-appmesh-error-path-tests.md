---
session: "agent-a9762f3c"
timestamp: "2026-03-19T12:00:00Z"
model: claude-opus-4-6
---

## Human

Write compat tests for 9 working AppMesh operations covering error paths
(NotFound errors for nonexistent meshes/resources) and ListMeshes empty response.

## Assistant

## Key decisions

Added 9 error-path tests in a new `TestErrorPaths` class to the existing
`test_appmesh_compat.py` file. The file already had happy-path tests for all
9 operations; these new tests verify proper error responses when resources
don't exist.

**Pattern used**: `pytest.raises(ClientError)` with assertion that the error
code contains "NotFound". For ListMeshes, asserted the "meshes" key exists
in the response (no error expected for listing with no results).

All 9 tests pass against the live server on port 4566.
