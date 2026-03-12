---
role: agent
timestamp: 2026-03-12T07:00:00Z
session: state-threading-fix
sequence: 1
---

# Fix state save/load crashes with threading locks

## Human prompt
Fix state save/load crashes when Moto backends contain threading locks (SQS Condition, ECR RLock, StepFunctions Lock/RLock). Add custom pickle handling in the state manager that strips threading objects before serializing and recreates them on load. Write integration tests covering SQS, DynamoDB, S3, multi-service, and versioned snapshots.

## Approach
- Created `_ThreadSafePickler` (subclass of `pickle.Pickler`) that uses `reducer_override` to replace Lock/RLock/Condition/Event/Semaphore/BoundedSemaphore/Barrier with `_ThreadingSentinel` placeholders
- Created `_restore_threading_objects` to walk the deserialized object graph and replace sentinels with fresh threading primitives
- Updated `_RestrictedUnpickler` to allow `robotocore.state.manager` module (for sentinel class)
- Updated all 4 pickle call sites: `_save_moto_state`, `_load_moto_state`, `_capture_state`, `_restore_state`
- Wrote 5 compat tests that create resources, save state, reset, load state, and verify data survived

## Key decision
Used `reducer_override` on `pickle.Pickler` rather than walking the object graph pre-pickle. This is cleaner because it intercepts at the exact point where pickle encounters an unpicklable object, without modifying the original objects in memory.
