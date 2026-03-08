---
session: "a09a1d51"
timestamp: "2026-03-08T07:00:00Z"
model: claude-opus-4-6
---

## Human

Pick 20 other parts of our product that you believe the overnight script
implemented extremely well. Launch an agent for each to review it (using Opus).
For every problem each agent finds, have it design a system-wide categorical
set of unit tests for it that can catch the whole category of failures. When
those tests fail, implement the fix. Thoughtfully merge each worktree branch
as each agent finishes, launching a new agent as you think of other parts
of our overnight to review.

## Assistant

## Key decisions

**Launched 20 agents in 4 waves**, each in an isolated git worktree, reviewing:

Wave 1 (large native providers): Cognito, Events, ECS, Batch, SNS
Wave 2 (core services): Kinesis, SQS, Step Functions, CloudWatch, Firehose
Wave 3 (critical providers): Lambda, S3, DynamoDB, Route53, CloudFormation
Wave 4 (remaining + infra): OpenSearch, SES, IAM, EC2, STS + Gateway Router

**Categorical bug patterns being hunted across all providers:**
- Tag stubs (creation tags not queryable via ListTags API)
- Parent-child cascade failures (delete parent, orphan children)
- Thread safety (inconsistent lock usage)
- Error handling (wrong error codes for nonexistent resources)
- ARN construction errors
- Key case conversion (PascalCase/camelCase corruption of user data)
- Silent success on delete of nonexistent resources (should 404)
- Hardcoded counts in describe/summary operations
- Missing pagination support
- Naive ARN parsing (split("/")[-1] fails for hierarchical ARNs)

**Merge strategy:** Copy changed files from each worktree to main, verify tests
pass, accumulate into commits. Each agent independently runs the full unit suite
to verify 0 regressions before completion.

**First completion — Kinesis agent found 5 categorical bugs:**
1. DeleteStream doesn't clean up resource policies (cascade)
2. DescribeStreamSummary hardcodes ConsumerCount: 0 (hardcoded count)
3. DeregisterStreamConsumer silently succeeds for nonexistent (silent pop)
4. Resource policy ops parse consumer ARNs wrong (naive ARN parsing)
5. ListTagsForStream ignores pagination params (missing pagination)
