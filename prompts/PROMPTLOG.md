# Prompt Log

When you make a git commit, also commit a sanitized log of the prompts that led to it. Both the human's and yours.

## Why

AI-assisted code has provenance — someone asked for it, an AI reasoned about it, decisions were made. The prompt log captures that provenance so future contributors (human or AI) can understand not just _what_ the code does, but _why_ it was written this way and _who_ (human or model) made each decision.

## Format

One file per prompt at `prompts/{timestamp}-{slug}.md`. Timestamp is UTC, formatted `YYYYMMDD-HHmmss`. Slug is a few lowercase url-safe words summarizing the prompt.

```
---
role: human
timestamp: "2026-03-07T14:32:08Z"
session: "a1b2c3"
---

The auth endpoint is returning 500s for a customer. Stack trace shows
a nil pointer in the token refresh handler. [Pasted: 12 lines of stack
trace from auth service, showing nil dereference at token_refresh.go:47].
Fix the nil pointer in the token refresh handler.
```

That file would be named `prompts/20260307-143208-fix-nil-pointer-auth.md`.

### Fields

**Required:**
- `role` — `human` or `assistant`
- `timestamp` — ISO 8601 UTC when the prompt was sent
- `session` — hex ID reused for all prompts in one conversation

**Optional:**
- `model` — for assistant entries, the model that generated the reasoning (e.g. `model: claude-opus-4-6`)
- `tools` — list of tools/agents used (e.g. `tools: [subagent, git-worktree]`), helps reviewers understand how work was parallelized
- `sequence` — integer ordering prompts within a session (1, 2, 3...) so readers can follow the conversation flow without sorting by timestamp
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
---
role: assistant
model: claude-opus-4-6
timestamp: "2026-03-07T20:16:00Z"
session: "a1b2c3"
sequence: 2
---

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

## Committing

Add `prompts/` to the same commit as the code changes. If a session produces multiple commits, each one includes only the prompts that led to it.

Create `prompts/` on first use. Don't add it to `.gitignore` unless the user asks.

## Never compact or summarize existing prompt files

Prompt files are permanent historical record. Never rewrite, shorten, merge, or "clean up" existing entries — not even to save tokens or tidy the directory. The verbosity is the point. A reviewer six months from now needs the actual words, not a digest.

This applies to autonomous cleanup passes too. If an agent is asked to "tidy the repo," `prompts/` is off-limits. The only valid operations on existing prompt files are:

- Adding optional frontmatter fields (e.g. backfilling `commits:`)
- Redacting newly-discovered secrets or PII (with a note in the file explaining what was redacted and why)

Everything else is append-only.
