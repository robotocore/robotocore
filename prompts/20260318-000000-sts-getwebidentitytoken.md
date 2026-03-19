---
session: "external"
timestamp: "2026-03-18T00:00:00Z"
model: claude-sonnet-4-6
sequence: 1
---

## Human

adding getWebIdentityToken to sts

## Assistant

## Key decisions

**Root cause of Moto failure**: Moto's `TokenResponse` class doesn't implement
`_get_multi_param`, which is required to parse `Audience.member.N` list params
in the query-protocol body. The operation crashes with an `InternalError` 500.

**Fix approach**: Native provider intercept in `handle_sts_request` for the
`GetWebIdentityToken` action. Reads raw `parsed` (parse_qs output, list-valued)
to correctly extract `Audience.member.1`, `Audience.member.2`, etc.

**Mock JWT**: Returns a structurally valid JWT (header.payload.signature) using
base64url encoding. The signature is a SHA-256 hash of the unsigned token —
not cryptographically valid, but correctly shaped for emulator use.

**Validation**: DurationSeconds 60-3600 (default 300), SigningAlgorithm must be
RS256 or ES384, Audience required (1-10 values).

**Tests**: 8 tests covering happy path, both algorithms, multi-audience, JWT
structure, expiration datetime parsing, and IAM policy acceptance of the action.
