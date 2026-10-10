---
session: "dev-ux-conformance"
timestamp: "2026-10-10T02:15:00Z"
model: claude-opus-4-6
sequence: 1
---

## Human

Improve robotocore continuously: adversarial review should not find any
remaining way to improve it — correctness, completeness, performance and
every kind of agent/developer experience polish.

## Assistant

## Key decisions

**Diagnosis that anchored this slice.** Running `uv run pytest tests/` bare
over the whole tree reported ~870 failures that all disappeared when the
same tests ran against a server started by the run. Root cause: the
compatibility suites address a long-lived server at `ENDPOINT_URL`
(default localhost:4566) and populate API state as they go; against a
warm server that some other work owns they fail on state they did not
create — indistinguishable from real defects. One more instance: one
compat file hardcoded the host ignoring `ENDPOINT_URL` altogether.

**Fix**: `pytest_configure` in `tests/compatibility/conftest.py` refuses
collection when the target server's `/health` uptime exceeds
`ROBOTOCORE_COMPAT_MAX_UPTIME` (default 600s), with instructions to start
a fresh server or opt out via `ROBOTOCORE_COMPAT_ALLOW_WARM_SERVER=1`.
Verified both directions: a warm twin aborts collection with a pointed
message; a fresh server collects as before.

**Local CI.** Added `scripts/local-ci.sh`, replicating the CI jobs
(ruff/format/mypy, the three unit splits, integration, the three
compatibility shards plus the cross-service suite and parity) — the
live-server stages each boot their own fresh robotocore on a free port
with CI's env (HTTPS/DNS disabled) and stop it afterwards. This encodes
the empirical lesson: what a green merge looks like locally without
GitHub's runner queue, and without the phantom-failure trap.

**Edge case found while smoking the script**: pytest does not expand
shard globs itself — a quoted `tests/compatibility/test_[a-g]*.py`
argument collects zero tests and "passes" hollow. The script must reach
the shell with the glob unquoted so bash expands it before pytest
collection; a second smoke run confirmed 6145 collected vs 0 quoted.

**Changelog.** Removed a duplicated `2026.10.6` Added section that a
merge left behind.
