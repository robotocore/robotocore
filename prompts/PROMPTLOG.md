# Prompt Log

When you make a git commit, also commit a sanitized log of the prompts that led to it. Both the human's and yours.

## Why

AI-assisted code has provenance — someone asked for it, an AI reasoned about it, decisions were made. The prompt log captures that provenance so future contributors (human or AI) can understand not just _what_ the code does, but _why_ it was written this way and _who_ (human or model) made each decision.

## Format

One file per conversation phase at `prompts/{timestamp}-{slug}.md`. Timestamp is UTC, formatted `YYYYMMDD-HHmmss`. Slug is a few lowercase url-safe words summarizing the phase.

Each file has **one YAML frontmatter block** with file-level metadata. The body uses `## Human` and `## Assistant` headings to separate entries. This avoids the "duplicated frontmatter" problem where multiple `---` blocks make the file look like two files concatenated together.

```
---
session: "a1b2c3"
timestamp: "2026-03-07T14:32:08Z"
model: claude-opus-4-6
---

## Human

The auth endpoint is returning 500s for a customer. Stack trace shows
a nil pointer in the token refresh handler. [Pasted: 12 lines of stack
trace from auth service, showing nil dereference at token_refresh.go:47].
Fix the nil pointer in the token refresh handler.

## Assistant

## Key decisions

**Root cause**: The `refreshToken` field is nil when the token was issued
via API key auth (no refresh token exists). The handler assumed all tokens
have a refresh path.

**Fix**: Added a nil check at token_refresh.go:45 before dereferencing.
Chose a nil check over a type switch because only this one code path
is affected — the broader token interface doesn't need to change.
```

That file would be named `prompts/20260307-143208-fix-nil-pointer-auth.md`.

**Do NOT use multiple `---` frontmatter blocks in one file.** Standard YAML/markdown parsers only recognize the first block. A second `---`...`---` block mid-file looks like corruption or concatenation.

### Frontmatter fields

**Required:**
- `session` — hex ID reused for all prompts in one conversation
- `timestamp` — ISO 8601 UTC when the conversation phase started

**Optional:**
- `model` — the model that generated the assistant reasoning (e.g. `model: claude-opus-4-6`)
- `tools` — list of tools/agents used (e.g. `tools: [subagent, git-worktree]`), helps reviewers understand how work was parallelized
- `sequence` — integer ordering files within a session (1, 2, 3...) when a session spans multiple files
- `reconstructed` — set to `true` when logging retroactively from session transcripts

### Linking prompts to commits

A commit SHA can't appear inside the commit itself. Three options:

1. **Amend after** — commit the prompt file, note the SHA, amend the commit to add `commits: [abc1234]` to the frontmatter. Works for single commits but rewrites history.
2. **Next-commit backfill** — commit the prompt file without SHAs, then in the _next_ commit, update the prompt file to add `commits:`. This preserves linear history. Preferred for shared branches.
3. **Don't link at all** — rely on `git log --follow prompts/` and timestamps to correlate. Simpler, and `git blame` already ties every line to a commit. This is fine for most projects.

Pick whichever fits your workflow. For retroactive reconstruction (logging an entire repo's history at once), option 2 or 3 is the only practical choice.

## What to log

Log prompts that lead to code changes. Skip purely conversational messages. If the user says "don't log this," don't.

Include your own reasoning as `assistant` entries when you make non-obvious decisions — architectural choices, workaround strategies, why you chose one approach over another. These entries are the most valuable part of the log for future reviewers trying to understand _why_ the code looks the way it does.

For assistant entries during autonomous work (long-running sessions where the human said "keep going"), log the high-level plan and key decision points, not every tool call. One assistant entry per logical phase of work is enough.

### What a good assistant entry looks like

A reviewer in 6 months doesn't need to know _what_ changed — `git diff` tells them that. They need to know _why_. Write decisions, not a changelog.

**Good** — explains reasoning:

```
## Assistant

## Key decisions

**Chose X over Y** because Z. Considered W but rejected it — it would have required
changing the wire format which breaks existing clients.

**Routing strategy**: JSON-protocol services use TARGET_PREFIX_MAP. REST-json services
with unique signing names need no special routing — the auth header extraction already
maps correctly. Only ambiguous cases (same prefix, different service) need aliases.

**What I skipped**: Operation-level routing for services that share both signing name
AND target prefix. Deferred because both backends work independently via Moto names
and unblocking the main path mattered more right now.
```

**Avoid** — this is a changelog, not a prompt log:

```
## Assistant

- Updated router.py
- Fixed bug in serializer
- Added 22 aliases to SERVICE_NAME_ALIASES
- Removed duplicate dependency
```

That belongs in a commit message. The prompt log is for the reasoning behind those changes.

## Retroactive logging

When reconstructing a prompt log from session transcripts after the fact:
- Use the session transcript timestamps, not the current time
- Mark retroactive entries with `reconstructed: true` in the frontmatter
- It's fine to summarize — a retroactive log captures intent and provenance, not a verbatim transcript
- For long autonomous sessions, one assistant entry summarizing the work is better than trying to reconstruct every step

## Sanitization

The prompt must be safe to persist in a shared repo. Redact secrets, PII, internal URLs, and customer data. Keep the intent, the technical substance, and anything already public.

The test: **would you be comfortable if this file appeared in a public GitHub repo?**

Here's what the original prompt behind the above example actually looked like:

> "Hey, the auth endpoint at https://api.internal.myco.com/v2/auth is returning 500s for customer Acme Corp (account #4821). Here's the stack trace: [paste]. Can you fix the nil pointer in the token refresh handler?"

The internal URL, customer name, and account number are gone. The intent, the file, the line number, and the problem are all still there.

When something is ambiguous — a colleague's name, an internal project codename, a business metric — redact the specific identifier but keep the context. "[A colleague] suggested we use Postgres" is fine. Summarize large pastes rather than including them verbatim: `[Pasted: 40 lines of payments controller with customer lookup logic]`.

When in doubt, redact.

## Multi-branch sessions

When a single conversation spawns multiple PRs (e.g., parallel worktree agents), use the same `session` ID across all prompt files with `sequence` numbers to chain them:

```
prompts/20260312-003433-robotocore-cli.md       # session: "migration-parity", sequence: 1
prompts/20260312-003433-awsroboto.md             # session: "migration-parity", sequence: 2
prompts/20260312-003433-versioned-snapshots.md   # session: "migration-parity", sequence: 3
```

Each file lives on its own feature branch. Sequence 1 should contain the full conversation chain (the human prompts that led to this work). Later sequences can reference it with `[Continuation of session — see sequence 1 for full chain]` to avoid duplicating the conversation history.

**Why this matters**: When 7 PRs land from one conversation, a reviewer needs to understand that they were part of a coordinated effort, not 7 unrelated changes. The shared session ID and sequence numbers make this traceable.

## Worktree agents and prompt logs

When using `isolation: "worktree"` agents, the agent cannot create prompt log files (they don't commit). The orchestrating conversation must create and push prompt logs to each worktree branch after the agent completes.

**Pattern**:
1. Agent completes work in worktree (code + tests, no prompt log)
2. Orchestrator commits and pushes the agent's changes
3. Orchestrator creates the prompt log file capturing the agent's task and key decisions
4. Orchestrator commits and pushes the prompt log as a follow-up commit

This is why `SKIP_PROMPT_LOG=1` exists — the first commit (code) bypasses the hook, and the second commit (prompt log) satisfies it. CI will re-run on the prompt log push and pass.

## Committing

Add `prompts/` to the same commit as the code changes. If a session produces multiple commits, each one includes only the prompts that led to it. If you are working in a worktree agent context where the code was committed separately, a follow-up commit with just the prompt log file is acceptable.

Create `prompts/` on first use. Don't add it to `.gitignore` unless the user asks.

## Never compact or summarize existing prompt files

Prompt files are permanent historical record. Never rewrite, shorten, merge, or "clean up" existing entries — not even to save tokens or tidy the directory. The verbosity is the point. A reviewer six months from now needs the actual words, not a digest.

This applies to autonomous cleanup passes too. If an agent is asked to "tidy the repo," `prompts/` is off-limits. The only valid operations on existing prompt files are:

- Adding optional frontmatter fields (e.g. backfilling `commits:`)
- Redacting newly-discovered secrets or PII (with a note in the file explaining what was redacted and why)

Everything else is append-only.

## Pre-commit hook

The pre-commit hook enforces that commits touching `src/` or `tests/` include a `prompts/` file. To bypass for legitimate cases (e.g., worktree agent code commits that will get a prompt log in a follow-up):

```bash
SKIP_PROMPT_LOG=1 git commit -m "..."
```

The CI `Prompt log check` job also enforces this. A follow-up commit adding the prompt log file will make the check pass on the next push.

**Do not routinely skip the hook.** It exists because prompt logs are easy to forget during autonomous work. The two-commit pattern (code then prompt log) is the intended workflow for worktree agents — not a workaround.
