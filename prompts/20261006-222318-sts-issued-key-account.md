---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:23:18Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

The most common failure: every provider block assumes a deploy role in its target account, and
calls signed with the resulting `ASIA…` key landed in the default account `123456789012`, so
`allowed_account_ids` rejected them. Moto already mints the temporary key in the target account's
IAM backend, so the gateway now resolves any `AKIA`/`ASIA` access key to the account that owns it
(scanning IAM backends once per key, cached positively since keys never change owner) before
falling back to the default account.
