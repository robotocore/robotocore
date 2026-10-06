---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:34:37Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Three small, independent fidelity gaps grouped into one change. Cognito `DeleteResourceServer`
returned 501, blocking replacement of resource servers. Moto drops `disableSessionTags` and
`externalId` from pod identity associations, so every association showed a diff after apply; the
native EKS layer now records and returns them. `DescribeDBEngineVersions` had a narrow catalog and
exact-match versions; it now carries current Aurora/PostgreSQL/MySQL versions and honours prefix
matching, `Filters` and `DefaultOnly`, which `data.aws_rds_engine_version` relies on.
