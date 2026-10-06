---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:24:23Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

A restart of a persistent instance lost every SSM parameter and KMS key, so a second `terraform
plan` after restart wanted to recreate them. The state manager catches pickling errors per backend
and skips the backend, which hid the failure. SSM's backend holds `defaultdict`s whose factories are
lambdas (unpicklable); KMS holds `cryptography` private-key objects. Fix: a custom pickler reducer
for `defaultdict` that substitutes a picklable equivalent factory (preserving subclasses such as
`ParameterDict`), and a reducer that serializes private keys as DER.
