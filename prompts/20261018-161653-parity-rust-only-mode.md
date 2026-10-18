---
session: "rot-harness"
timestamp: "2026-10-18T16:16:53Z"
model: kimi
---

## Human

Rework the parity harness for the post-Python era: the Python ground-truth
server will be deleted and only the Rust server will remain. Add a
`--rust-only` mode to `scripts/harness/parity.py` (defaulted whenever the
Python ground-truth server is absent) that runs every compat suite ONCE
against the Rust server. Add a golden baseline `.parity/expectations_current.json`
(ship it) holding per-suite pass counts; on each rust-only run compare against
it and surface regressions as `EXPECTATIONS DRIFT: suite X expected N, got M`.
Add `RUST_ONLY=1` to `scripts/start_parity_servers.sh` to start only the
sidecar + rust server, skipping python-lane checks. Keep `--dual` (py vs rust)
functional in parallel while the porting fleet still needs ground truth.
Endgame: a bare `parity.py` invocation must equal rust-only mode reading the
baseline — update README accordingly. No edits to tests/**.

## Assistant

## Key decisions

- **Mode resolution**: explicit `--dual` / `--rust-only` flags win; with
  neither, parity.py probes the python ground-truth health endpoint and
  auto-picks dual when it is present, rust-only when absent. That makes the
  endgame a no-op: once the python server is deleted, bare invocation IS
  rust-only — no flag flip needed later.
- **Baseline numbers come from the rust column of the last full dual run**
  (recorded `rust_pass`, with `py_ground_truth_pass` kept for the audit
  trail). Using the rust column avoids permanent false drift where the rust
  lane legitimately differs from python today by a few tests (ec2's three
  hard-coded-ELB artifacts, dynamodb's +7). ec2 expectation is 717 — exactly
  the number in the requested example message.
- **Truncation**: the trust rule stays central. Dual `classify()` is
  untouched; the single lane got a sibling `classify_rust_only(results,
  timed_out)` marking a run truncated when the suite timed out or the JUnit
  XML was empty/missing (CRASH). Truncated runs never feed fidelity or drift
  comparisons, and are never promoted by `--update-expectations`.
- **Drift is one-directional**: actual < expected prints
  `EXPECTATIONS DRIFT ...`, is recorded under `expectations_drift` in the
  state file, and exits 2 so CI can gate. actual > expected shows as >100%
  fidelity and an `--update-expectations` hint; no failure.
- **`--update-expectations` is merge-safe**: suites not in the run (subset
  runs, truncated, missing) keep their existing baseline entries.
- **Drift exit code 2**: distinct from the abort exit 1 so CI can tell a
  fidelity regression from infra failure.

## Verification

- `parity.py --rust-only --services sqs,s3,ec2` against the live rust lane:
  sqs 95/95, s3 190/191, ec2 717 — exactly the dual run's rust column.
- Synthetic baseline with ec2=999 produced the drift line and exit code 2;
  sqs baseline 94 with actual 95 improved without failing.
- `PARITY_SUITE_TIMEOUT=1` run marked sqs truncated, omitted fidelity/drift.
- Mode auto-detect verified both ways (`mode` recorded in each state file).
- `RUST_ONLY=1 bash scripts/start_parity_servers.sh` on ports 4569/4570:
  sidecar + rust only, bridge WORKING, python lane untouched; a rust-only
  parity run against those env ports passed 95/95.
- `--update-expectations` merge verified (run suite refreshed, other suites
  kept).
- No edits to tests/**.
