---
session: "dev-ux-conformance"
timestamp: "2026-10-10T03:20:00Z"
model: claude-opus-4-6
sequence: 3
---

## Human

Continue improving robotocore (same standing goal): agent-facing UX counts as
work product — error messages agents read are part of the API surface.

## Assistant

## Key decisions

**Concretized in this slice.** Signed off with a live probe: an unsigned request
carrying only `?Action=...&Version=...` got a one-line 400 — a dead end for any
client (agent or human) that cannot re-derive the routing rule set.

**Unroutable-request hints.** `handle_aws_request` now returns the 400 with a
`hints` list naming every routing cue the request offered and what it would take:
an unsigned call with a query Action learns that query-protocol services share
that shape (ec2/sqs/sns/sts/…) and that the service has to come from a signature,
a service path, or a service hostname; a malformed `x-amz-target` echoes the
unknown prefix; a path-driven candidate names itself; and if nothing matched, a
neutral hint replaces the empty list so the body is never bare. AGENTS.md's
"Reading errors correctly" table gains the matching row so agents do not mistake
this for a service absence (only 501 is a gap).

**Followed the loop's taste**: no new endpoints, no schema change — just the
payload shape of an existing failure path plus the doc table agents already
consult.
