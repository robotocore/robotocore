---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:31:50Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

`aws_ssm_service_setting` never converged: updates were not stored, and reading by the ARN that
`GetServiceSetting` itself returns built an ARN-inside-an-ARN. Service settings now live in a small
per-account/region store with AWS defaults and `Customized` status. Configurations that read a
shared parameter by its full ARN in another account (a common pattern for org-wide values) failed
with ParameterNotFound; the ARN's account now selects the backend.
