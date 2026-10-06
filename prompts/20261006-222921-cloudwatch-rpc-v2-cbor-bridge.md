---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:29:21Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Current AWS SDKs (aws-sdk-go-v2, which the Terraform AWS provider uses) talk to CloudWatch over
Smithy RPCv2 CBOR. robotocore's native CloudWatch handler covered a few operations; everything else
returned 501, so every `aws_cloudwatch_metric_alarm` failed. Rather than reimplement each operation,
the bridge decodes CBOR, re-encodes the request as AWS JSON 1.0 for Moto, and converts the response
back to CBOR, using the botocore service model to restore timestamps and blobs that JSON flattens.
