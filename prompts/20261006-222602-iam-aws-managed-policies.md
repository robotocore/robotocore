---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:26:02Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Most real IAM configurations attach AWS-managed policies (`arn:aws:iam::aws:policy/...`), which
robotocore only knew about when moto's `MOTO_IAM_LOAD_MANAGED_POLICIES` was set — and that loads
~1,300 policies into every account. Instead, a managed policy is materialized lazily in the caller's
account the first time a request names it, from moto's bundled catalog plus a small supplement of
newer policies, keeping `Scope=Local` listings clean. Also: `GetAccountSummary` now reports the
STS token version set by `SetSecurityTokenServicePreferences`.
