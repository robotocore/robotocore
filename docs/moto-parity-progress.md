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

(none yet)
