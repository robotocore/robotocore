---
role: assistant
timestamp: 2026-03-19T17:00:00Z
session: moto-upstream-sync
sequence: 1
---

# Sync moto fork with upstream getmoto/moto

## Human prompt
Rebase moto fork on upstream getmoto/moto main, merge into
robotocore/all-fixes branch, hard reset fork's main to upstream.

## Approach

1. Hard reset `jackdanger/moto master` to `getmoto/moto master`
2. Merge upstream into `robotocore/all-fixes` branch (our custom patches)
3. Resolve 20 merge conflicts keeping both sides
4. Fix rerere-cached bad resolutions from earlier attempt
5. Iteratively fix regressions: missing imports, lost handlers, syntax errors

## Key decisions

- Used merge (not rebase) because our branch has 58 merge commits and 294 total
  commits — rebasing would flatten/lose history and create conflicts
- Used `--strategy-option ours` initially but that dropped upstream's structural
  changes (ActionResult pattern, core.parse rename). Switched to default merge
  with manual conflict resolution.
- For services where upstream completely restructured responses (CloudFront,
  MediaLive), restored pre-merge handlers since they're incompatible with
  upstream's new ActionResult auto-parsing

## Upstream changes absorbed (43 commits)
- DynamoDB: new request parsing/serialization
- CloudFront: new request parsing/serialization
- Route53: new request parsing/serialization
- MediaLive: new request parsing/serialization
- S3: dualstack and FIPS endpoint URLs
- Cognito IDP: TOTP MFA support
- Pipes: ManagedState, Start/Stop methods

## Regressions fixed
- RDS: missing `from typing import Any` import
- Route53: `TYPE_RESPONSE` import, `_validate_resource_id` method placement
- Transfer: syntax errors in exceptions.py, types.py; restored responses.py
- CloudFront: restored full models/responses/urls from pre-merge
- MediaLive: restored responses.py, fixed models for renamed attributes
- DAX: removed duplicate tag methods from upstream
- Route53Domains: increased domain limit from 20→200
