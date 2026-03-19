# Skill: Write a Prompt Log Entry

Use this skill when it's time to record the conversation in `prompts/`.

## When to use

- Before committing code (the pre-commit hook requires it)
- At the end of a session
- Any time the user says "log the prompts" or "record this session"

## Process

1. **Check what changed** in the current session:
   ```bash
   git diff --name-only --diff-filter=ACM HEAD~5..HEAD 2>/dev/null | head -40
   git log --oneline -8
   ```

2. **Determine the timestamp and slug**:
   - Timestamp: current UTC time, formatted `YYYYMMDD-HHMMSS`
   - Slug: 2-4 lowercase words describing the session (e.g., `fix-cognito-auth`, `add-sqs-fifo-tests`)
   - File: `prompts/{timestamp}-{slug}.md`

3. **Write the file** using the required format:

```markdown
---
session: "<short-id>"
timestamp: "<ISO 8601 UTC>"
model: claude-sonnet-4-6
---

## Human

<Summarize what the user asked for. Redact secrets, PII, internal URLs.
Keep technical substance and intent. One paragraph per distinct request.>

## Assistant

### Key decisions

**<Decision title>**: <Why this approach was chosen. What alternatives were
considered and rejected, and why. Future reviewers need the reasoning, not
a changelog — git diff shows what changed.>

**<Another decision>**: <...>
```

4. **Stage the file** immediately:
   ```bash
   git add prompts/{filename}.md
   ```

## Rules

- Session ID: short unique string, reused for all files in the same conversation
- Timestamp: when the conversation or session phase *started*, not when you're writing this
- DO NOT summarize what files changed (that's what `git diff` is for)
- DO explain WHY — architectural choices, rejected alternatives, tradeoffs
- Keep `reconstructed: true` in frontmatter if writing retroactively
- Never rewrite or shorten existing prompt files — they are permanent record
- For external contributor PRs: use `author` and `pr` frontmatter fields to give credit. Write the log on their behalf rather than blocking the PR.

## Example

```markdown
---
session: "sqs-fifo-and-dlq"
timestamp: "2026-03-10T14:00:00Z"
model: claude-sonnet-4-6
---

## Human

SQS FIFO queues aren't deduplicating messages with the same MessageDeduplicationId
within the 5-minute window. Fix it and add tests.

## Assistant

### Key decisions

**Deduplication store location**: Added `_dedup_cache: dict[str, float]` directly
to the SQS queue model rather than a separate per-backend store. This keeps
deduplication scoped to the queue (correct AWS semantics) and avoids cross-queue
collisions on shared MessageDeduplicationIds.

**5-minute window**: Used `time.time()` rather than a monotonic clock because
the dedup window is wall-clock time per AWS docs. Tests mock `time.time` to
avoid flakiness.

**What I didn't do**: Implement persistence for the dedup cache across server
restarts — not needed for dev/test use and adds complexity.
```
