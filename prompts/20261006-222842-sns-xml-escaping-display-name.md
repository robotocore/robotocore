---
session: "multi-account-terraform-fidelity"
timestamp: "2026-10-06T22:28:42Z"
model: claude-opus-5-5
---

## Human

Apply a large real-world multi-account Terraform codebase (AWS provider v5/v6, dozens of accounts
in one AWS Organization, every provider assuming a deploy role into its target account) against
robotocore, and fix the robotocore defects that make `terraform apply` fail or leave a perpetual
diff. Rank fixes by how many configurations each unblocks; add tests and a Terraform reproducer for
each.

## Assistant

Webhook-style HTTPS subscriptions carry query strings (`?token=…&service=…`); the SNS query
responses interpolated values into XML unescaped, so SDKs failed to parse
`GetSubscriptionAttributes` and `ListSubscriptionsByTopic`. All values, list members and error
messages are now escaped. Separately, `DisplayName` defaulted to the topic name, which AWS never
does, producing a perpetual diff on every `aws_sns_topic`.
