---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:28:00Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Replaying an existing organization from its Terraform needs `CreateAccount` to hand back the ids
the rest of the configuration already references (account maps, `allowed_account_ids`, ARNs).
Random ids made every downstream configuration fail. Added an opt-in registry, by email or account
name, settable through an admin endpoint or an env var pointing at a JSON file. This is a new
`/_robotocore/*` admin surface, so it is proposed separately for maintainer judgment.
