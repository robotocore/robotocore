---
session: "changelog-policy-and-multiversion-entry"
timestamp: "2026-05-13T23:28:28Z"
model: claude-opus-4-7
---

## Human

Ensure this is recorded as a major new feature in our changelog and
release a new version. Update our rules and skills to keep the
changelog intact and also update the changelog now.

## Assistant

## Key decisions

Two findings before writing the entry:

1. ``CHANGELOG.md`` was abandoned at 1.0.0 (2026-03-07). Every CalVer
   release since (~30+ tags up through ``v2026.5.12``) shipped without
   a changelog entry. The auto-release workflow tags + builds + pushes
   Docker images but doesn't touch ``CHANGELOG.md`` — that gap is the
   ongoing process problem this commit fixes.

2. The release workflow has been **failing for 6 consecutive runs**
   since 2026-05-12 with ``insufficient_scope`` errors pushing to
   ``robotocore/robotocore`` on Docker Hub. The credentials in repo
   secrets appear expired or revoked. This is an infrastructure issue
   I can't resolve from this session — surfaced in the PR description
   so the user can rotate the secret.

### The 2026.5.13 entry

Wrote a substantial Keep-a-Changelog entry for the multi-version
Lambda runtime feature that just merged in PR #275. Structure:

* Lead paragraph framing the user-visible change: faithful per-version
  execution for all 5 multi-version Lambda runtime families
  (previously a ``python3.10`` Lambda silently ran on host 3.12).
* Table mapping AWS runtime IDs → default (baked) vs fault-in source
  per family.
* ``Added``: per-runtime executor caching, ``_RUNTIME_BINARY`` maps,
  fault-in install framework, new endpoints, env vars.
* ``Changed``: image sizes (standard 722→463 MB, java-and-dotnet
  1,578→1,320 MB), ``_detect_tfm()`` per-runtime preference, honest
  endpoint reporting, divergence warnings.
* ``Fixed``: ``Bootstrap.java --release 8`` (ClassFormatError),
  unified DOTNET_ROOT (faulted-in SDK invisibility), exec bit
  preservation in tar extraction.
* ``Migration``: callout that functions which previously relied on the
  silent version mismatch (running 3.10 code on 3.12) will now
  actually execute on 3.10 and any version-specific differences will
  surface.

### Process changes to keep this from drifting again

The auto-release flow's silence about changelog updates is the root
cause. Two complementary fixes:

1. ``CLAUDE.md`` — added a *Changelog discipline (CRITICAL)* subsection
   under *Commit cadence*. Specifies: when to update (user-visible
   changes), how to update (Keep-a-Changelog format, newest first,
   per-date CalVer sections), what to include (specific names and
   numbers, migration notes for behaviour changes), and that the
   review-pr command should gate on it.
2. ``.claude/skills/changelog.md`` — new skill with concrete format,
   process steps, rules, and an example. Future Claude sessions will
   read this skill (similar to ``promptlog.md``) when prompted about
   the changelog.
3. ``.claude/commands/review-pr.md`` — added step 3a "Check for
   changelog" parallel to the existing "Check for prompt log" gate.
   The reviewer's expected to read the diff, decide if the change is
   user-visible per the discipline doc, and either accept or request a
   changelog entry before approving.

### Release implications

Once this PR lands on main, the release workflow will attempt
``v2026.5.13.1`` (because tag ``v2026.5.13`` would exist if the prior
release had succeeded; since it didn't, CalVer will compute
``v2026.5.13`` on the first successful run). If Docker Hub creds are
fixed by then, the multi-version Lambda runtime feature will ship to
end users in that release. If still broken, the changelog is at least
on main and the next successful release picks it up.
