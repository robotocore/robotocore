---
session: "20260511-open-issues"
timestamp: "2026-05-11T00:03:00Z"
model: claude-sonnet-4-6
sequence: 4
---

## Human

Why did we remove dockerhub publishing? Why not publish there as well? Folks use it all the time.

## Assistant

## Key decisions

**Root cause from git history**: commit `a5b21e5a` removed Docker Hub publishing because the Docker Hub org `robotocore/robotocore` didn't exist yet, causing every release to fail. The commit message said "Docker Hub can be added back once the org and secrets are configured." The `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` secrets were already set (added before the org was ready), then the publishing code was pulled.

**Fix**: Restore Docker Hub publishing in `release.yml` alongside GHCR. Both registries now get versioned and `:latest` tags.

**Docs**: Updated README quick start to use `robotocore/robotocore:latest` (shorter, conventional Docker Hub default) with GHCR as a noted alternative. This is consistent with the docker-compose and GitHub Actions examples that already use the short form on main.

**Closed PR #268** (which had updated all docs from `robotocore/robotocore:latest` → `ghcr.io/robotocore/robotocore:latest`). That PR was the right fix while Docker Hub wasn't available, but it's now superseded — with Docker Hub restored, the short name works again and is preferable.
