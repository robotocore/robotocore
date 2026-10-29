# Moto-parity port goal — the contract

This file is the durable goal contract for the moto-coverage port on the
`rust` branch of the robotocore repo. It survives context compaction: when
resuming, **read this file and `docs/moto-parity-progress.md`, then run the
status command. The status command, not memory, is the current state.**

## Objective

Port the remaining AWS services to the `rotocore` crate so that every
service the pre-rotocore tree served — whether through the moto sidecar or
through a python-era native provider — is served **natively by rotocore**
with the same fidelity the parity suites measured.

### The data that defines "must-port"

Deterministic, re-computable from repo history plus the live twin
(anchor SHA: `1e9a1c37`, the pre-rotocore tip of the previous port goal):

- Old served surface = every entry of
  `git show 1e9a1c37:src/core/service_registry.rs` whose status was
  `moto` or `native`.
- At goal start (SHA `391b54bc`) the live twin reports 65 services as
  non-native. Of those:
  - **54** were routed through the moto sidecar in the old tree,
  - **4** were python-era **native providers** that regressed in the port
    (`rdsdata`, `route53`, `ses`, `synthetics`),
  - **7 + 1** were never registered in the old product either
    (`applicationautoscaling`, `dynamodbstreams`,
    `kinesis-video-archived-media`, `sagemaker-runtime`, `scheduler`,
    `servicecatalogappregistry`, `servicediscovery`, plus `neptune` whose
    suite was also never promoted). For these, moto's own coverage is not
    proven by our artifacts.

## Done when — all six gates pass

- **G1 — coverage gate** (mechanical, exit-code checkable):
  `python3 tooling/moto_port_progress.py` exits `0`. This means every
  old-served service is now `native` on the live twin, except services the
  ledger has explicitly CLOSED with evidence (see below). The script is
  the gate; do not bypass or edit its logic to pass.
- **G2 — uncertain services**: each of the 8 never-registered services is
  either `native` on the twin, or CLOSED in the ledger with concrete
  evidence that it is out of moto's surface too (e.g. every operation in
  its botocore model is unimplemented moto-side, or it has no botocore
  model at all). "Probably absent" is not evidence.
- **G3 — quality gates** (run before any push batch, all must pass):
  ```
  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace --lib          # 0 failed
  make parity                           # exit 0, drift-clean
  ```
- **G4 — no test regression**: the total pass count of
  `uv run pytest tests/compatibility -q` at goal end is **>=** the count
  recorded in the ledger's start row (`9254`), and the promoted baseline
  (`.parity/expectations_current.json`) never goes down. Restored suites
  count toward this. Deleting an assertion is allowed only for
  operations that turn out to be outside moto's surface too, and only with
  a ledger entry.
- **G5 — introspection honesty**: `/_robotocore/services` reports
  `native` (never `absent`) for every G1-covered service; `handles_service`
  and the dispatch arms stay in sync (the spot-check test exists and
  passes).

## Loop — how to iterate

1. Pick the next service from the status script's remaining-queue output
   (order: easiest first — services whose probe file shows the most
   `working` operations come last).
2. Ground truth per operation: `probes/<svc>.json` (per-op moto-era
   behavior, recorded when the sidecar served it) plus the botocore
   service model (`python3 -c "import botocore.session; ..."`).
3. Implement `rotocore/src/services/<svc>/` (mod.rs with `dispatch`),
   arm it in `rotocore/src/services/mod.rs` (both `dispatch` and
   `handles_service`), add registry/protocol entries if the service needs
   a gateway route.
4. Restore the deleted compat suite from git history — it will not
   exist on disk:
   ```
   git show 7170f904^:tests/compatibility/test_<svc>_compat.py \
     > tests/compatibility/test_<svc>_compat.py    # 59-suite batch
   git show 391b54bc^:tests/compatibility/... # (dynamodbstreams, chaos -> chaos skipped)
   ```
   Run it: `uv run pytest tests/compatibility/test_<svc>_compat.py -q`
   Green before the service counts as ported. Tests for operations that
   are genuinely outside the service's implemented surface (see G2 rule)
   are removed with a ledger entry, not xfailed.
5. Per wave (cluster of services): run the G3 gates, then
   `uv run python scripts/harness/parity.py --rust-only --update-expectations`
   to promote, then commit (each commit self-contained and green) and add
   a ledger row to `docs/moto-parity-progress.md`.
6. Push on wave boundaries (maintainer cadence), parallel worktrees per
   service cluster are expected; adversarial review subagent on each
   wave's diff, findings fixed in the same wave.
7. Cheap tests only between changes: filter to the touched crate's unit
   tests. Full lane / parity runs at wave boundaries, not per-commit.

## Boundaries

- Only these areas: `rotocore/`, `crates/`, `src/core/services/`,
  `src/core/service_registry.rs` (protocol entries), `tests/compatibility/`
  (restorations only), the two parity files via the harness, the ledger,
  and status script. No edits to `src/router.rs` unless a service provably
  needs a new route arm (then document in the ledger).
- Never edit `.parity/expectations_current.json` by hand — only via the
  harness at wave end.
- No python product may return: no `.py` outside `bindings/`, `tests/`,
  `scripts/`, `tooling/`.
- No xfail-centric completion; no skipping CI gates; no touching the
  status script's logic to make it pass — if the script is wrong, fix it
  in a ledger-documented commit and let the number be what it is.

## Stop rule

- A service gets at most 3 dedicated failed attempts; then park it: add a
  ledger row `PARKED <svc>: <reason>`, move to the next queue item, and
  include parkeds in the final report.
- External blockers (no network for a needed reference, botocore spec
  missing) get the same treatment: record, park, continue.
- When the queue is empty except CLOSED/PARKED entries: run the full gate
  set once, write the final ledger row, and report. Do not call the goal
  complete while the status script exits non-zero.

## Status command (the gate)

```
python3 tooling/moto_port_progress.py          # exit 0 only at G1+G2 done
python3 tooling/moto_port_progress.py --gates  # also runs G3 (slow)
```
