---
session: "appsync-unit-tests"
timestamp: "2026-03-12T06:02:00Z"
model: claude-opus-4-6
---

## Human

Write comprehensive unit tests for the AppSync native provider at
`src/robotocore/services/appsync/provider.py`. Cover all CRUD operations for
GraphQL APIs, API keys, schemas, resolvers, data sources, types, functions,
domain names, event APIs, channel namespaces, API cache, tags, and introspection
schema. Also cover error cases (not found, missing required params, duplicates).

## Assistant

## Key decisions

**Test strategy**: Tested all inner functions directly against a fresh `AppSyncStore`
instance per test (via pytest fixture), bypassing HTTP routing. This isolates
business logic from the ASGI handler and avoids needing to mock Starlette requests.

**Coverage**: 144 tests across 12 test classes covering every CRUD operation in the
provider plus helper/utility functions. Each resource type gets create, get, list,
update (where applicable), delete, and not-found error paths.

**No mocking needed**: The provider uses a simple in-memory store with no external
dependencies (no Moto, no network), so tests call the functions directly with real
store instances. This makes tests fast (~0.1s total) and deterministic.

**Edge cases covered**: duplicate data source names, missing required fields (name,
fieldName, domainName), tags on three different resource types (GraphQL API, Event
API, Channel Namespace), API deletion cascading to child resources (keys, data
sources, schemas), introspection schema in both SDL and JSON formats.
