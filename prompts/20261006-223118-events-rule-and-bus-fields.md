---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:31:18Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

EventBridge rules and buses showed perpetual diffs (`role_arn`, `tags`, `description`) and, worse,
updating a rule's pattern re-created it and silently dropped its targets. The native models now
carry every field Terraform sets, `DescribeRule`/`DescribeEventBus` return them, `PutRule` on an
existing rule is an in-place update, and `UpdateEventBus` is implemented.
