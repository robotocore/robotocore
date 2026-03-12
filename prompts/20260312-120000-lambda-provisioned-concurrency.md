---
session: "prov-concurrency-enforce"
timestamp: "2026-03-12T12:00:00Z"
model: claude-opus-4-6
---

## Human

Implement Lambda provisioned concurrency enforcement. Currently it's stored via CRUD
operations but NEVER used during invocations. Track provisioned vs on-demand invocations,
return ProvisionedConcurrencyInvocation header when provisioned capacity is used, enforce
pool routing, and add x-amz-executed-version header based on qualifier.

## Assistant

## Key decisions

**Provisioned pool routing in ConcurrencyTracker**: Extended `acquire()` to accept a
`qualifier` parameter and return a bool indicating whether provisioned concurrency was
used. The tracker now maintains `_provisioned_in_use` dict alongside `_provisioned`
capacity. When provisioned capacity has room, the invocation uses a provisioned slot;
when full, it falls through to on-demand. This keeps the enforcement in the tracker
(single lock, thread-safe) rather than scattering it across provider code.

**Qualifier-based keying**: Provisioned concurrency keys in the tracker use
`{account}:{region}:{func}:{qualifier}` to match AWS behavior where provisioned
concurrency is per-version/alias, not per-function. Different versions have independent
pools.

**x-amz-executed-version**: Now reflects the actual qualifier — numeric version strings
are passed through, while aliases and $LATEST resolve to "$LATEST". This matches AWS
behavior where the header shows the concrete version number.

**Utilization metrics**: `_get_provisioned_concurrency` now enriches the stored config
with live utilization data from the tracker, returning `ProvisionedConcurrencyUtilization`
(0.0-1.0) and updating `AvailableProvisionedConcurrentExecutions` based on actual in-use
count.

**What I preserved**: The `acquire()` API is backward-compatible — qualifier defaults to
`$LATEST`, and the bool return value is new but won't break callers that ignore it. All
existing tests pass unchanged.
