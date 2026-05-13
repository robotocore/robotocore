---
session: "dockerhub-rename-jackdanger"
timestamp: "2026-05-13T23:40:03Z"
model: claude-opus-4-7
---

## Human

In this same PR fix all references to the dockerhub images because
Docker charges $15/month for a new organization so we'll be using
jackdanger/robotocore as the image name. The GitHub release will be
more properly robotocore/robotocore. Update all references thoroughly
and make sure the release works.

## Assistant

## Key decisions

The triage had three categories of `robotocore/robotocore` references
that all looked alike but needed different treatment:

| Pattern                                  | Meaning                  | Action |
|------------------------------------------|--------------------------|--------|
| `robotocore/robotocore` (bare)           | Docker Hub image         | **Rename** to `jackdanger/robotocore` |
| `ghcr.io/robotocore/robotocore`          | GHCR (GitHub Container Registry) | **Keep** — GitHub org is free |
| `github.com/robotocore/robotocore[/...]` | GitHub repository URL    | **Keep** — repo path is unchanged |
| `robotocore/robotocore/.github/...`      | GitHub Action path ref   | **Keep** — also a GitHub repo path |

Used a perl one-liner with negative lookbehind / lookahead to do the
rename safely on every relevant file in one pass:

```
perl -i -pe 's{(?<!github\.com/)(?<!ghcr\.io/)robotocore/robotocore(?!/\.github)}{jackdanger/robotocore}g'
```

Applied to all .md / .py / .yml / .yaml / .toml / .json / .sh /
Dockerfile* files except those under .git/, vendor/, .venv/, prompts/
(historical record), and CHANGELOG.md (already had the right names in
the existing entry — would've been a re-rename). 14 files modified,
62 Docker Hub refs flipped, 6 GitHub URLs + 7 GHCR refs preserved
verbatim. ``tests/unit/ci/test_setup_action.py`` asserts on the GHCR
string and was correctly untouched.

### CHANGELOG entry

Added a `### Breaking: Docker Hub image renamed to jackdanger/robotocore`
section as the FIRST subsection under `## 2026.5.13` (before the
major feature), because the rename is the only breaking change in
this release and users running `docker pull robotocore/robotocore`
need to know to switch. Showed the diff inline plus called out that
GHCR (`ghcr.io/robotocore/robotocore`) is unchanged.

### Release workflow

Updated `release.yml` Docker Hub login step's comment to spell out:

* WHY we use jackdanger's namespace ($15/month for the
  `robotocore` Docker Hub org).
* WHICH secrets need to be set
  (`DOCKERHUB_USERNAME` = "jackdanger",
  `DOCKERHUB_TOKEN` = jackdanger's PAT).
* WHAT failure looks like (`insufficient_scope: authorization
  failed`) so operators can grep for it.

The push targets at lines 113-114 and 131-132 are now
`jackdanger/robotocore:*` and `ghcr.io/.../robotocore:*`. The GHCR side
uses `${{ github.repository }}` so it tracks the GitHub repo
automatically; no static reference to flip.

### What I deliberately didn't do

Considered splitting the build-push-action into per-registry steps
with `continue-on-error: true` on the Docker Hub side, so a Docker
Hub auth failure wouldn't block GHCR. Decided against — the user's
request was about renaming, not architecture changes to the release.
A single failing path is easier to diagnose than half-shipped
releases that look mostly green. If Docker Hub auth keeps being a
problem, splitting can be a separate change.

### Operator action required

The repo secrets still hold the old `robotocore`-org credentials
(per the previous failure trail). Until rotated to jackdanger's
account, the release will keep failing with the same
`insufficient_scope` error. That's outside what I can fix from this
session — surfaced explicitly in the PR description.
