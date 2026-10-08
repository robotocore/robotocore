---
session: "fidelity-loop-cycle-vpce"
timestamp: "2026-10-08T22:40:00Z"
model: claude-opus-4-6
reconstructed: true
sequence: 3
---

## Human

Same session: after the ServiceRegion fix merged, the affected root's second
plan no longer replaces VPC endpoints, but the re-plan still reports in-place
updates on routes in the core-network attachment module. Work the next defect.

## Assistant

## Key decisions

**Root cause** (read from the re-plan diff, then checked against the botocore
service model): `aws_route`'s `core_network_arn` — the Cloud WAN routing
attribute — is `Optional+Computed`+forces-new in the aws provider's schema and
the `Route` shape carries `CoreNetworkArn`, but the emulator's `CreateRoute`
never read the parameter and the `Route` model never stored it, so after apply
the read-back dropped it and every subsequent plan re-proposed
`+ core_network_arn = ...` as an in-place update.

**Fix**: additive parameter on `CreateRoute`/`ReplaceRoute`, stored as
`self.core_network_arn` on the `Route` model; the response serializer picks
`CoreNetworkArn` from the botocore shape automatically. ReplaceRoute only
overwrites when a value is supplied, mirroring AWS's explicit-only semantics.

**Tests**: one moto test (create + replace round-trip through DescribeRouteTables)
and two live-server integration tests.

**Placement**: additive model fields on the vendored moto fork, consumed as a
submodule + lock bump — same class as the prior two VPC-endpoint fixes.
