---
session: "a3f7b2e1"
timestamp: "2026-05-12T16:30:00Z"
model: claude-sonnet-4-6
pr: 274
---

## Human

Fix this: https://github.com/robotocore/robotocore/issues/225

(Issue #225: S3 object URL — `GET http://localstack:4566/bucketname/filename.json` returns 400 "Could not determine target AWS service from request". User came from LocalStack community edition.)

## Assistant

## Key decisions

**Root cause**: The router had no fallback for requests arriving without AWS signing headers. A plain HTTP GET to `/bucket/key.json` (public S3 access, no `Authorization` header) fell through all 7 detection steps and returned `None`, triggering the 400.

**Scope of fix**: After reading the router, I identified two distinct gaps in the "S3 without auth" category, not just one:
1. Path-style anonymous access: `GET http://localhost:4566/bucket/key.json`
2. Virtual-hosted style with a plain localhost endpoint: `GET http://mybucket.localhost:4566/key.json`

Both share the same root cause — the router only detects S3 via auth credential scope, `.s3.` hostname substrings, or SigV2 signatures. Neither covers unsigned/anonymous requests.

**Path-style fallback (step 8, last resort)**: If the first path segment matches the S3 bucket name pattern (3–63 chars, `^[a-z0-9][a-z0-9\-\.]{1,61}[a-z0-9]$`) and nothing else matched, route to `s3`. This is safe because all AWS API calls with signing land in earlier steps, and internal `/_robotocore/` paths are explicitly excluded. The fallback only fires for requests that have zero service hints.

**Virtual-hosted fallback (step 5, after `.s3.` check)**: Match `{bucket}.localhost:{port}` hosts by checking `host_rest == "localhost"` (exact match, not substring). The `"localhost" in host_rest` form was too broad — it would catch `queue.localhost.robotocore.cloud` which belongs to SQS.

**Test updates**: Three existing tests used paths like `/unknown`, `/unknown-path`, `/some/unknown/path` that happened to look like valid bucket names (lowercase, 3+ chars, letters/digits/hyphens). Updated all three to use uppercase/underscore paths that are genuinely unroutable by any service.

**What I ruled out**: Modifying the `.s3.` host check to also catch `mybucket.s3.localhost.robotocore.cloud` — that already works. The virtual-hosted check I added is specifically for the plain `bucket.localhost:4566` pattern used when clients set a custom endpoint without the S3 subdomain convention.
