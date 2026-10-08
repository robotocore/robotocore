---
session: "ipam-filter-wildcards"
timestamp: "2026-10-08T07:30:00Z"
model: claude-opus-4-6
---

## Human (external contributor, #346)

DescribeIpamPools/Scopes filter values are compared literally. EC2 filter values are
patterns — `*` matches any run of characters, `?` exactly one, case-sensitive — and the
`aws_vpc_ipam_pool` Terraform data source sends exactly that shape, so a lookup that
works on AWS finds nothing on the twin.

## Assistant

## Key decisions

**Semantics**: `fnmatch.fnmatchcase` per pattern (`*`, `?`, case-sensitive, any-of-list)
applied in `DescribeIpamPools` (fields plus `tag:` and `tag-key` filters) and in
`DescribeIpamScopes` (field filters).

**Extra fixes in the same change**: `TaggedEC2Resource.get_tags()` returns describe_tags
dicts (`{"key": ..., "value": ...}`), so the old `t.key` attribute access raised on any
tagged pool; tag filters now read the dicts correctly.

**Tests**: wildcard semantics at three heights — the helper itself, a
Terraform-data-source-shaped description filter, and tag filters — in
`tests/unit/services/test_ec2_ipam.py`.

**Deviations from pure fnmatch that are deliberate**: none; `*` and `?` carry AWS's
documented meanings, and erroneous over-matching of `[seq]` classes is accepted as AWS
does not document character-class semantics for filter values.
