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

## Wave 2 (2026-10-19, through SHA 04b99c03 + reconciliation/perf pieces) — ten services

- Ported: `ce` (61/61), `glacier` (38/38, re-armed an inert wave-3 leftover
  module), `workspaces` (97/97), `bedrock` (132/132), `wafv2` (76/76),
  `servicecatalog` (104/104 after reconcile), `s3control` (162/162),
  `textract` (44/44 after reconcile), `mq` (27/27), `mediastore` (29/29).
- **Probe-staleness ruling** (recorded by parent, verified by wave-2 review):
  probes were captured 2026-03-19, one day BEFORE the deployed moto fork
  gained surfaces (JackDanger/moto@robotocore/all-fixes `2016563fd`;
  uv-cache fork and archives are sha256-identical). Evidence per service:
  - mq: probes/mq.json marks 8 ops `not_implemented` while
    `moto/mq/responses.py:192-249` has all handlers → implemented per fork.
  - mediastore: probes mark 8 + 2 `needs_params`, but moto commit
    09ec12906 (2026-03-20) added the 10 missing MediaStore ops → all 21
    botocore ops implemented 1:1 with `moto/mediastore/responses.py:16-141`.
  - s3control: probes mark the tag trio 501 while
    `moto/s3control/responses.py:358/363/373` implements them → implemented;
    the 14 genuinely-absent ops stay `501 NotImplemented`.
  - textract: probes mark 19 ops 501 while the fork's urls.py routes the
    whole service and responses.py handlers cover 25/25 → reconcile commit
    `b2c9b9ad` re-implemented all, suite restored to 44/44.
  - servicecatalog: probes mark the 3 org-access ops 501 while fork commit
    065b86c15 provides them → reconcile commit `4fa24056` restored 104/104
    (state default DISABLED, per fork).
- **G2 deletion (kept):** wafv2 `GetTopPathStatisticsByTraffic` — zero
  occurrences in the fork checkout and moto 5.2.4 → the single suite test
  was removed with evidence (the only assertion deletion this wave).
- Deliberate shapes: mq normalizes moto IndexError-500 paths to the modeled
  `NotFoundException` 404 (only 501 means absent); workspaces mirrors moto's
  KeyError-500 shapes; s3control empty-body paths keep moto's 500
  InternalError; bedrock adds ARN resolution for job lookups; servicecatalog
  keeps fork's DISABLED default; shield's wave-1 500 quirk still documented.
- Infra: parity harness endpoint moved to `localhost` (AccountId subdomain
  APIs like s3control cannot use an IP endpoint — fixed the promotion hang
  and the phantom `pass=1/28` record); `--services` default now derives
  from suites on disk (129 gated); drift-tuple unpack bug fixed so real
  drift surfaces as exit 2; compat conftest stays on `localhost` (reverted
  a 127.0.0.1 "harden" that broke subdomain routing).
- Perf lane merged (port/perf2): OnceLock memoization of gateway native
  service names in server.rs — `/_robotocore/{health,services}` −39%
  (1.089→0.661 ms / 1.130→0.685 ms), byte-identical payloads, sqs suite
  green before/after. Standing perf agents have reclaimed disk to ~66G free.
- Promotion: full lane exit 0 on the warm twin; 129 suites drift-clean.
  (First fresh-twin attempt had a one-off apigateway 128/129 cold-start
  flake — isolation-greens; next rerun clean.)
- G4 lane count: 10160 passed / 157 failed / 17 errors (start row: 9254).
- Gate after wave: old served surface 157, native 106, REMAINING 51
  (43 to port + 8 G2-uncertain).

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
