---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:25:09Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Two S3 defects broke most bucket configurations. (1) aws-sdk-go-v2 sends path-style sub-resource
requests with a trailing slash (`/bucket/?cors=`); the native handlers matched only `/bucket?cors`
and fell through to an object-key path, returning 400 — so the refresh every `aws_s3_bucket` does
failed. (2) Lifecycle rules were normalized on write, losing noncurrent-version transitions, `And`
filters and size bounds, and the minimum-object-size header was dropped; Terraform then reported an
inconsistent result after apply. Rules are now stored verbatim and the header persisted.
