---
session: "dev-ux-conformance"
timestamp: "2026-10-10T02:50:00Z"
model: claude-opus-4-6
sequence: 2
---

## Human

Continue improving robotocore (same standing goal).

## Assistant

## Key decisions

**Slice B: schema-driven round-trip conformance, first cut.** The three
merged fixes (#350–#352) are the same species — an attribute the botocore
service model exposes as an echo (present in the create-op input shape AND
the resource's output shape) that the emulated model silently dropped.
Rather than hand-list each, the contract should be derivable: for a
(resource × create-op × describe-op) probe, the expected echo set is the
intersection of the input-shape members and the output-shape members,
resolved through the describe wrapper (VpcEndpoints[...], Volumes[...],
RouteTables[].Routes[]) and minus call-only members (ClientToken,
TagSpecifications, DryRun).

**Probe assertions**: each member set on create must reappear in the
describe read-back, and scalar echoes must be value-equal modulo AWS's enum
casing (`dualstack` in, `Dualstack` out — AWS normalizes to its own
capitalization on the way back, and the sibling fix #351 relies on that
being stable). Volumes expose their Volume fields at the response root, not
under a wrapper — that cost one test iteration to notice.

**The harness guard** asserts the probe expectations against botocore's real
shapes, so a future botocore bump that reshapes a member fails the check
instead of leaving the suite vacuous.

**Found while writing it**: the suite itself surfaced the casing drift only
as a value mismatch, not a dropped member — the emulator's behavior was
already right; the assertion framework needed the AWS-casing nuance spelled
out. Shared fixture `make_boto_client` reused to keep the live-server boot
in one place.
