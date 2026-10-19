# Moto-parity port — progress ledger

The goal contract lives in `docs/moto-parity-goal.md`. This ledger records
state so any resumed session (or reviewer) can verify progress without
trusting memory. One row per wave; `CLOSED <svc>` and `PARKED <svc>` lines
are parsed by `tooling/moto_port_progress.py`.

## Start state (2026-10-19, SHA 391b54bc)

- Old served surface (`1e9a1c37` registry, status `moto` or `native`): 157 services
- Non-native on the live twin: 65
  - 54 moto-backed in the old tree
  - 4 python-native providers that regressed in the port (`rdsdata`, `route53`, `ses`, `synthetics`)
  - 8 never-registered/moto-coverage-unproven (G2 set): `applicationautoscaling`,
    `dynamodbstreams`, `kinesis-video-archived-media`, `sagemaker-runtime`,
    `scheduler`, `servicecatalogappregistry`, `servicediscovery`, `neptune`
- Gates at start: 860 workspace unit tests pass; `make parity` 100% on 89
  promoted suites (drift-clean); clippy/fmt clean.
- Wider pytest lane baseline: `9254 passed / 168 failed / 17 errors / 69
  skipped` — the failures are the documented port backlog
  (`tooling/parity-known-artifacts.md`).

## Waves

## Wave 1 (2026-10-19) — signer, shield, datapipeline, ram

- Ported: `signer` (26/26), `shield` (35/35), `datapipeline` (23/23), `ram` (41/41).
  Suites restored from `7170f904^`, all green against the merged twin on 4566.
- Superseded-arch note: an orphaned moto-proxy server from the abandoned
  `.worktrees/s3` worktree occupied ports 4641/4642 and was killed during
  the wave (superseded architecture, not the 4566 twin).
- G2: no tests removed — probes show 0 `not_implemented` for all four.
  datapipeline's 11 probe `500_error` entries were moto `KeyError` crashes
  on missing pipelines; implemented as real handlers answering the fork's
  `PipelineNotFoundException` (400) instead of reproducing the crash.
- Deliberate quirk retained: shield `DescribeProtection` with no selector
  answers moto's 500 InternalError (probe + unit test document it).
- signer suite: one test's hardcoded `localhost:4566` now reads
  `ENDPOINT_URL` (assertion untouched, needed to test against a worktree
  port).
- Registry rows flipped `moto`→`native` for the four; gates: 906 workspace
  lib tests, fmt/clippy clean, parity promoted (full lane drift-clean).
- Post-wave per gate script: REMAINING 54, G2 UNCERTAIN 8 (unchanged).
