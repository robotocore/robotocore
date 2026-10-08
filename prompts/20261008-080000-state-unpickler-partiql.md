---
session: "state-unpickler-partiql"
timestamp: "2026-10-08T08:05:00Z"
model: claude-opus-4-6
---

## Human (autonomous loop)

Explore remaining edge cases in robotocore and its tests; fix the ones found.

## Assistant

## Findings and fixes

**State snapshots refused to load once a Cognito user pool existed.** A multi-day
test-order flake (a rotating subset of state-snapshot tests failing in full suites while
passing alone) traced to `_RestrictedUnpickler`'s module allowlist: moto's cognito-idp
backend holds its user pool's signing key as a joserfc JWK (`joserfc._rfc7518.rsa_key`,
a private module), and every load of a snapshot taken with a user pool in the registry
raised `_DisallowedClassError` — autos-restore, cloud pods (as Management API 500s) and
multi-account save/load all failed as a rot of "any state load, ever, once a user pool
exists". Allowlisted the joserfc key classes and added a save→load→verify regression
test on the cognito backend.

**Unparseable PartiQL statements crashed as 500.** moto's `ExecuteStatement` unpacks a
parser result that py_partiql_parser returns as None for a statement it cannot parse,
so a typo'd statement surfaced as 500 InternalError where AWS answers 400
ValidationException. Fixed in the moto fork (raise MockValidationException on None) and
covered by a moto-suite test plus a robotocore gateway-level compat-style unit test;
the moto pin advanced to that commit.

**Client-side JSON decode errors** no longer surface as 500 InternalError inside the
moto bridge's other catches: a malformed JSON body now answers the wire's
ValidationException (400), keeping 500-gated probe flows meaningful.

## Tests added

- cognito-backend roundtrip through the restricted pickler (regression for #churn above)
- gateway-level test: ExecuteStatement syntax garbage == 400 ValidationException
- moto fork test: same assertion at moto's own layer
