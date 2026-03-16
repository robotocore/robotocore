---
session: "be2d5c96"
timestamp: "2026-03-16T03:00:00Z"
model: claude-sonnet-4-6
---

## Human

Fix CI failures on PR #168. S3 Control tests (133 failures) and S3 bucket name collisions in parallel test runs.

## Assistant

Diagnosed two issues:

1. **S3 Control vhost collision**: boto3 sends S3 Control requests to `{AccountId}.localhost:{port}` (e.g., `123456789012.localhost:4566`). The new `.localhost` vhost pattern in `s3_routing.py` was matching 12-digit account IDs as bucket names. Fixed by adding a guard: `not (label.isdigit() and len(label) == 12)`.

2. **S3 test bucket name conflict**: Both `test_s3_compat.py` and `test_s3_ext_compat.py` used bucket name `test-compat-bucket`. With `-n8 --dist=loadfile`, concurrent workers on different files would delete each other's buckets. Fixed by using `test-compat-ext-bucket` in the ext file.

Wrote a diagnostic script using `botocore.httpsession` monkey-patching to capture the actual URL boto3 sends for S3 Control operations, which revealed the AccountId vhost pattern. Added unit test `test_s3control_account_id_not_bucket` to prevent regression.
