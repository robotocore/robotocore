---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:27:04Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

A landing-zone layout (management account, a network account owning an org-wide IPAM, members in
nested OUs) failed at three layers: member accounts could not accept an invitation, so
`DescribeOrganization` from a member said "not in use"; RAM had no cross-account view
(`resourceOwner=OTHER-ACCOUNTS` was 501); and IPAM ignored filters and could not allocate a VPC
CIDR from a pool. Organizations and RAM become native providers layered on moto's backends: the
handshake is visible to its target, acceptance joins the org, and RAM principal matching walks the
caller's OU path. IPAM gains filter evaluation, shared-pool visibility, first-fit allocation for
`CreateVpc` from a pool, and a `DescribeIpamScopes` that honours ids/filters and returns the ARN
(the provider crashes without it).
