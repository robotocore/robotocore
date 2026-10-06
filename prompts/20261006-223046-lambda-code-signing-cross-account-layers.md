---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:30:46Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Every `aws_lambda_function` refresh calls `GetFunctionCodeSigningConfig`; AWS answers 200 with an
empty ARN when none is set, robotocore answered 404, so every Lambda refresh failed. Functions that
use a vendor-published layer (an ARN in another account) also failed because layer lookups only
searched the caller's account; the lookup now resolves the owning account from the ARN.
(An unrelated auto-generated lifecycle test, `test_function_code_signing_config_lifecycle`, already
fails on main because it creates against a nonexistent function; its post-delete expectation is
updated to the AWS behaviour.)
