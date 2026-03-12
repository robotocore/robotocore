---
session: "agent-a8f84678"
timestamp: "2026-03-12T06:00:00Z"
model: claude-opus-4-6
---

## Human

Write edge-case and stress tests for Python Lambda execution. The existing 133 tests cover basic CRUD and invocation. Write tests that catch subtle bugs: /tmp writes, threading, layers, unicode, concurrent invocations, code updates, context fields, error shapes, large responses, return types, etc. Fix any server bugs discovered along the way.

## Assistant

## Key decisions

**Tests written**: 35 edge-case compat tests in `test_lambda_python_edge_cases_compat.py` covering:
- /tmp filesystem writes and reads
- Return types: None, string, int, list, bool, nested dict, empty dict
- Deeply nested events (15 levels)
- Event isolation (handler mutation doesn't leak)
- Custom exception subclasses
- Threading inside handler
- Unicode in events and responses
- DryRun (204) and Event (202) invocation types
- Concurrent invocations (5 parallel via ThreadPoolExecutor)
- Update code then invoke (verify new code runs)
- Memory size in context (128MB vs 3008MB)
- get_remaining_time_in_millis() decreasing
- Context fields (function_name, function_version, log_group)
- Large response (260KB)
- Non-existent function invocation (ResourceNotFoundException)
- Invalid JSON payload
- Multi-module zip imports
- Layer imports (python/ subdirectory)
- Environment variables
- Print output in logs (LogType=Tail)
- Error shapes with stackTrace
- SyntaxError and ImportError
- Handler not found

**Bugs found and fixed (3)**:

1. **Boolean/int/float return serialization** (`provider.py`): `isinstance(True, (dict, list))` is False and `isinstance(True, str)` is False, so booleans fell through to `str(result).encode()` producing `b"True"` instead of valid JSON `b"true"`. Fixed by adding `bool, int, float` to the `json.dumps` branch.

2. **Lambda layer code not extracted during invoke** (`provider.py` + `executor.py`): The provider never called `get_layer_zips()` or passed `layer_zips` to the executor. Layers were silently ignored. Additionally, `get_layer_zips` had two bugs:
   - `fn.layers` returns `list[LayerDataType]` (dicts with "Arn" key), but code assumed string or object with `.arn` attribute
   - Layer data attribute is `content` not `code`, and the `hasattr(layer_ver, "code")` guard prevented the `code_bytes` fallback from running

3. **Handler-not-found returns null body** (`executor.py`): `Runtime.HandlerNotFound` returned `(None, error_type, msg)` so the response body was `b"null"` with no error details. Fixed to return proper `{"errorMessage": ..., "errorType": ...}` dict.
