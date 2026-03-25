---
session: "978da9c9"
timestamp: "2026-03-25T06:00:00Z"
model: claude-opus-4-6
---

## Human

We're partway through implementing this digital twin of AWS. Some of the features
work. Some might be broken. Some tests are good, some might be invalid. No human
has ever read the code, this is 100% AI-generated. Craft a means of 1) guaranteeing
semantic correctness of both the tests and the implementation - including by
deterministically knowing what parts are implemented already by Moto and need
neither testing nor implementation - and 2) creating a long, MECE task list of
remaining implementations and tests needed to 100% cover all operations in AWS.

Review this plan soberly. Consider what computer science fundamentals we might
need to use or invent in order to accomplish this correctly.

## Assistant

## Key decisions

**Problem framing**: Identified 6 hard CS problems: the Oracle Problem (no single
source of truth — triangulate across botocore shapes, Moto @aws_verified tests, and
botocore examples), co-generated bias (AI wrote both impl and tests — they could
agree on wrong behavior), Moto coverage is undecidable from static analysis alone,
MECE as algebraic partition (classification must be a total function), service name
fragmentation (10+ mapping tables, 350+ entries), and ResponseMetadata false
positives (boto3 injects client-side keys not in botocore shapes).

**Confidence lattice**: Defined a partial order of evidence levels for implementation
status: unimplemented < method_is_stub < method_exists < probe_working <
shape_conformant < behaviorally_verified. This acknowledges that "implemented"
is not binary — a method may exist but be a stub.

**Architecture**: Three new files, one generated artifact:
- `scripts/lib/service_names.py` — canonical name resolver consolidating 10+ tables
- `scripts/build_operation_catalog.py` — per-operation truth table (10,223 records)
- `scripts/validate_test_semantics.py` — static analysis for co-generated bias
- `data/operation_catalog.json` — generated catalog

**MECE classification**: 8 categories as a total function with catch-all. Priority-
ordered decision tree: skip > implement > fix_impl > verify > test > fix_test >
strengthen_test > done. Disjointness guaranteed by function semantics. Totality
guaranteed by catch-all return.

**Results**: 10,223 operations across 157 services. 7,203 done (70.5%),
1,789 implement (17.5%), 944 strengthen_test (9.2%), 257 test (2.5%),
24 fix_test (0.2%), 6 skip (0.1%).

**Semantic validation**: 26,842 test-operation pairs analyzed. Found 185 phantom
keys (asserted keys not in botocore output shapes — some are REST header-to-body
flattening, not bugs), 1,155 metadata-only tests, 2,824 missing error codes,
3,267 low coverage tests.

**Key tradeoff**: Chose offline static analysis over live probing for the catalog's
default mode. Live probing (--with-probe, --with-shapes) is optional. This means
the catalog can run in CI without a running server, but some confidence levels
(probe_working, shape_conformant) are only available with the server running.
