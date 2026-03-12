---
session: "migration-parity"
timestamp: "2026-03-11T22:30:00Z"
model: claude-opus-4-6
sequence: 2
---

## Human

On a branch make a change to our primary README that mentions all of the systems
one might migrate from, linking to the migration PRs for each. Keep it a compact,
terse bullet-point list. Include LocalStack and link to a LOCALSTACK.md that
describes the migration needed in fair, sober terms.

## Assistant

## Key decisions

**README section is a bullet list, not a table**: Compact, scannable, links to
per-emulator guides. Each bullet names the emulator, links the guide, and adds
one clause about what you gain.

**LOCALSTACK.md tone is deliberately fair**: Acknowledges LocalStack's business
decision as reasonable, lists genuine gaps ("what you lose"), and has a "when to
stay on LocalStack" section. This builds credibility — people trust migration
guides that are honest about tradeoffs.

**"What you lose" became the feature roadmap**: The 7 items in the loss column
(CLI, awsroboto, versioned snapshots, Docker Lambda, dashboard, TLS, SERVICES
filter) were subsequently implemented as PRs #108-#114, closing most of the gap.
