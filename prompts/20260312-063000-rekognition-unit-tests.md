---
session: "a9c77894"
timestamp: "2026-03-12T06:30:00Z"
model: claude-opus-4-6
---

## Human

Write comprehensive unit tests for the Rekognition native provider at
`src/robotocore/services/rekognition/provider.py`. Create the test file at
`tests/unit/services/rekognition/test_rekognition_provider.py`. Cover all
action functions: collections, faces, projects, stream processors, video
analysis, image analysis, face liveness, tagging, and error cases.

## Assistant

## Key decisions

**Test strategy**: Called inner handler functions directly (e.g., `_create_collection`,
`_index_faces`) rather than going through the async `handle_rekognition_request` for most
tests. This avoids needing pytest-asyncio (not available in this env) and tests the actual
logic without HTTP scaffolding. Four integration-style tests exercise the async dispatch
path using `asyncio.new_event_loop().run_until_complete()`.

**Coverage scope**: 102 tests covering all 35 actions in `_ACTION_MAP`:
- Collection CRUD (create, describe, list with pagination, delete with cascade cleanup)
- Face operations (index, list, search, search-by-image, delete with count updates)
- Image analysis (detect faces, moderation labels, protective equipment, celebrities)
- Video analysis (6 start/get pairs: face detection, label detection, celebrity recognition,
  content moderation, person tracking, segment detection)
- Projects (create, describe, delete with ARN lookup)
- Stream processors (create, describe, list, delete)
- Face liveness (create session, get results, session-not-found error)
- Tagging (tag/untag/list across collection/project/stream-processor resource types)
- Store isolation (account and region separation)
- ARN helpers and `_resource_exists` coverage
- Action map completeness assertion (guards against drift)

**Existing test file**: There was already a `tests/unit/services/test_rekognition_provider.py`
with ~25 tests covering collection errors and tagging. The new file is in a subdirectory
(`tests/unit/services/rekognition/`) and covers the full provider comprehensively including
all the operations the existing file didn't touch.
