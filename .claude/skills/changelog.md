# Skill: Update the Changelog

Use this skill when a PR introduces a user-visible change (new feature,
endpoint, env var, behaviour change, breaking change, user-reported bug
fix, ≥10% image-size or perf shift, or new runtime/service support).

The authoritative rule lives in [`CLAUDE.md`](../../CLAUDE.md) under
*Changelog discipline*; this skill is the *how*.

## When NOT to use

- Pure internal refactors (renames, dead code, comment-only changes).
- Test-only changes (new tests for existing behaviour, lint fixes,
  CI tweaks that don't change runtime behaviour).
- Changes to `prompts/`, `.claude/`, `vendor/`, `docs/`.

When in doubt: if a user reading the release notes would want to know
about it, add an entry.

## Format

`CHANGELOG.md` uses [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
with CalVer dates. **Newest first, top of the file.**

```markdown
## YYYY.M.D (YYYY-MM-DD)

### Major: <one-line elevator pitch>      ← only for major features

<Optional 2-4 sentence paragraph explaining what shipped and why.
Lead with user value, not implementation.>

#### Added
- **<Feature name>** — one-line description with the concrete endpoint
  / env var / runtime ID. Sub-bullets allowed for nested detail.

#### Changed
- **<What changed>** — *with the number*. "Image dropped from 987 MB to
  463 MB" beats "smaller image".

#### Deprecated
#### Removed
#### Fixed
#### Security

#### Migration
<Only when behaviour changed at all — even non-breaking. Especially
important when something that used to silently work the wrong way is
now correct: users will see the new correct behaviour as a "regression"
unless told.>
```

## Process

1. **Locate today's date section** at the top of `CHANGELOG.md`. If
   it exists (another PR landed today), append your subsections to it.
   If not, create a new `## YYYY.M.D (YYYY-MM-DD)` section above the
   most recent.
2. **Pick the right subsections** from the standard six. Most PRs use
   1–3. A major feature usually uses `Added` + `Changed` + `Fixed` +
   `Migration`.
3. **Be specific about scope**:
   - Names: name the new endpoint, env var, runtime ID, function.
   - Numbers: image sizes, test counts, timeouts, percent deltas.
   - Files: when a contract changed (config file, headers, IDs).
4. **Write the Migration block** if any of the following is true:
   - A previously silent bug now warns or fails.
   - Output format changed (new fields, removed fields, renamed fields).
   - Performance characteristics changed (faster ≠ silent).
   - A default changed.
5. **Stage in the same commit** as the code change:
   ```bash
   git add CHANGELOG.md src/path/that/changed.py tests/...
   git commit -m "feat: ..."
   ```

## Rules

- **Same PR, not a follow-up**. The changelog entry is part of the
  feature, not a separate cleanup.
- **No "fixed bugs" without naming the bug**. "Fixed crash" tells the
  reader nothing.
- **No metrics without numbers**. "Faster" is noise.
- **First-person plural is fine** but not required. Match the tone of
  the existing entries (the 1.0.0 entry is a useful reference).
- **Don't link to PRs in the changelog body** — the git history already
  links commits to PRs. Save PR references for the prompt log.

## Example

```markdown
## 2026.5.13 (2026-05-13)

### Major: real per-version dispatch for every Lambda runtime

Robotocore previously ran every Lambda function on whatever single
interpreter was baked into the image. This release ships faithful
per-version execution for all five multi-version Lambda runtime
families…

#### Added

- **Per-runtime executor caching** — `get_executor_for_runtime("ruby3.3")`
  and `get_executor_for_runtime("ruby3.4")` now return distinct cached
  instances threaded with the requested runtime ID.
- **`POST /_robotocore/runtimes/install`** `{"runtimes": [...]}` to
  pre-warm one or more runtimes synchronously.
- **Env vars**: `ROBOTOCORE_RUNTIME_CACHE_DIR`,
  `ROBOTOCORE_RUNTIME_BIN_DIR`,
  `ROBOTOCORE_RUNTIME_DOWNLOAD_TIMEOUT`,
  `ROBOTOCORE_RUNTIME_FAULTIN`.

#### Changed

- **Docker images are smaller** despite shipping true per-version dispatch:
  - `robotocore:latest`: **722 MB → 463 MB**
  - `robotocore:java-and-dotnet`: **1,578 MB → 1,320 MB**

#### Fixed

- `Bootstrap.java` is now compiled with `--release 8` so the cached
  `Bootstrap.class` loads on any JVM major from 8 onward — fixes
  `ClassFormatError` on faulted-in older JREs.

#### Migration

Functions that *relied* on the previous silent version mismatch (e.g. a
`python3.10` function that quietly ran on 3.12) will now execute under
Python 3.10 on first invocation. Any version-specific stdlib or syntax
differences will surface.
```
