---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:33:15Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Configurations that loop over every enabled region (e.g. to remove default VPCs) failed with a
partition mismatch because Moto's `DescribeRegions` returned GovCloud and China regions to a
commercial caller; the response is now filtered to the caller's partition. Several AWS-managed
interface endpoint services (EKS OIDC and the FIPS variants) were missing from Moto's catalog, so
endpoint-service lookups failed; they are registered at import.
