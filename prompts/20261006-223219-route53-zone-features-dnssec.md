---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:32:19Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Two Route 53 gaps. AWS provider 6.33 added `Features.AcceleratedRecoveryStatus` to hosted zones
and dereferences it unconditionally, so every `aws_route53_zone` crashed the provider; zones now
report `DISABLED` by default and `UpdateHostedZoneFeatures` sets it. DNSSEC: `GetDNSSEC` ignored the
zone's KSKs and never reported `SIGNING`, so the provider's key-signing-key waiter timed out;
signing status is now tracked and zone ids normalized (`/hostedzone/` prefix) across KSK calls.
