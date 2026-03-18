---
session: "a86d371f"
timestamp: "2026-03-17T08:00:00Z"
model: claude-sonnet-4-6
sequence: 4
---

## Human

Run PYTHONPATH=. uv run python scripts/validate_response_shapes.py --top 20 --no-optional
[output showing 7 failures: stepfunctions DescribeMapRun/ListMapRuns, route53 list operations missing required fields]

## Assistant

## Key decisions

**StepFunctions DescribeMapRun**: Moto stub returned `{}` for any ARN instead
of raising `ExecutionDoesNotExist`. Fixed to raise the correct exception for
unknown ARNs → validator skips (expected for non-existent resources).

**StepFunctions ListMapRuns**: Moto stub returned bare `[]` but botocore expects
`{"mapRuns": [...]}`. The response handler `ActionResult([])` serialized as the
list itself, not under the `mapRuns` key. Two fixes:
1. `models.py`: verify execution exists before listing (raises ExecutionDoesNotExist for fake ARNs → validator skips)
2. `responses.py`: wrap result as `ActionResult({"mapRuns": runs})`

**Route53 missing required fields**: Moto templates used conditional blocks
`{% if marker %}<Marker>...{% endif %}` which omits the field entirely when no
marker was provided. botocore requires these fields unconditionally. Fixed:
- `ListHostedZones`: `Marker` always present (empty string default), `MaxItems` always present (default "100")
- `ListHostedZonesByName`: added `<MaxItems>100</MaxItems>`
- `ListHostedZonesByVPC`: added `<MaxItems>100</MaxItems>`
- `ListHealthChecks`: added `<Marker></Marker>`

**Route53 ListTrafficPolicies**: Native provider response was missing
`TrafficPolicyIdMarker` (required). Added to the XML template.

All fixes in vendor/moto, pushed to JackDanger/moto fork on
`robotocore/all-fixes` branch. `uv lock` updated to pin new commit.
