---
session: "sqs-attribute-md5"
timestamp: "2026-04-02T22:59:48Z"
model: gpt-5
tools: [git-worktree, browser-tools]
---

## Human

Create a new branch and PR to fix an SQS compatibility bug where
robotocore omits `MD5OfMessageAttributes` from SQS responses when
message attributes are present. The fix needs AWS-compatible hashing,
coverage for send, receive, batch send, and absence when no attributes
exist, and the repo markdown/process docs should be reviewed first.

## Assistant

## Key decisions

**Isolated worktree**: The original checkout already had unrelated,
uncommitted SQS edits. I created a clean worktree branch so this PR only
contains the attribute-MD5 fix and its tests, without sweeping in the
separate query-protocol work already in progress elsewhere.

**Provider helper over model expansion**: Instead of adding another
persisted checksum field to `SqsMessage`, I implemented the AWS message
attribute digest as a provider helper and used it at each response point
that needs it. That keeps the stored message shape small while still
ensuring send, receive, and batch responses all derive from the same
encoding logic.

**Anchored to AWS rules, not guessed**: I verified the algorithm against
AWS documentation and the AWS PHP SDK implementation before coding it.
The important details were sorting by attribute name, encoding lengths as
4-byte big-endian integers, using transport byte `1` for string/number
attributes and `2` for binary, and including custom data type suffixes in
the hashed type string.

**Binary normalization**: The live compatibility run exposed a subtle
wire-format mismatch for binary attributes. Query-protocol requests carry
binary values as base64 text, so the digest must be computed from the raw
decoded bytes, not the base64 literal. I normalized query-protocol binary
values to bytes before storing and hashing them.

**Verification strategy**: The local environment lacked a working async
pytest plugin for the pre-existing async handler tests, so I treated the
direct provider/unit coverage plus live compatibility tests against a
branch-local server as the authoritative proof for this change. I also
ran the repo's test-quality analyzer on the touched compatibility file to
confirm the added tests are real server-contacting checks.
