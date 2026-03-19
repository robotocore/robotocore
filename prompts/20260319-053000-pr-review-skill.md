---
session: "pr-review-skill"
timestamp: "2026-03-19T05:30:00Z"
model: claude-opus-4-6
---

## Human

Write a PR review skill that warmly welcomes first-time contributors, exits
early without approval if no prompts/ file, critically reviews from multiple
angles, adds fixup commits, approves when good, and sets to automerge. The
submitter deserves credit — prioritize merging their PR in the shape they
submitted it. Improvements go in follow-up PRs.

Also holistically improve skills and take a fresh look at PROMPTLOG.md.

## Assistant

## Key decisions

**Slash command, not a skill file**: Created `.claude/commands/review-pr.md`
rather than `.claude/skills/review-pr.md`. Commands are user-invocable via
`/review-pr 191`, while skills are reference docs. The review workflow needs
to be triggered explicitly per-PR.

**Credit-first philosophy**: The skill's core principle is that the PR belongs
to the submitter. Fixup commits only address CI blockers (bare except blocks,
missing imports, lint). Anything beyond that goes in a follow-up PR after
theirs merges. This prevents the "helpful reviewer" antipattern of rewriting
someone's contribution.

**Prompt log as gate, not blocker**: If the contributor didn't include a
prompt log, the reviewer requests changes but can also write one on their
behalf using the new `author` and `pr` frontmatter fields. This keeps the
provenance requirement without punishing contributors unfamiliar with the
convention.

**PROMPTLOG.md improvements**: Added `author` and `pr` optional frontmatter
fields, plus an "External contributor PRs" section with a concrete example.
The spec now explicitly supports the workflow of writing prompt logs on
behalf of others.

**CLAUDE.md addition**: Added a "Reviewing pull requests" subsection under
"Working with Agents" that summarizes the key principles without duplicating
the full skill. Points to `/review-pr` for the detailed workflow.
