# soak-hunter-2026-10-25 — wave-3 HUNTER-SOAK lane

30-minute mixed soak + chaos against a scratch twin built at rust HEAD
(91a001e2) with one soak-lane fix applied (`events: PutRule honors the
caller's State param`, 3cb14881) plus a docker-mode lambda executor.

## Environment

- Twin: `robotocore-rust` release binary from this branch (fix included),
  `--host 0.0.0.0 --port 4802`, `LAMBDA_EXECUTOR=docker GATEWAY_PORT=4802`.
- Baseline reference: `tooling/perf-ab-2026-10-24` (rust container bench-rust
  path).
- Harness: `soak.py` (mixed SQS/S3/DDB steady state + docker-metric lambda
  waves + scheduler-edge rules + chaos: rule churn, 50-target no-op
  fan-out, stray cross-account rules, 5-deep bus chain, 2-bus cycle,
  Scheduled->Lambda and Scheduled->SNS hops; 60s resource sampler).
- Teardown: `teardown.py`.
- Analyzer: `analyze.py` -> `logs/analysis.json`.
- Accounts: 773100000001/.2/.3, 663200000001, 663300000001, 663400000001
  (all dedicated, all torn down after the run).

## Findings

- `events: PutRule ignores the caller's State` — fixed in this branch
  (commit 3cb14881); era parity: moto fork `events/models.py:84`
  (`self.state = state or "ENABLED"`). Live probe on the scratch twin:
  put DISABLED -> 0 fires; after EnableRule -> fires resume.
- `soak profile`: see soak-summary.md in this directory (written at end of
  run).
