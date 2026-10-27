# HUNTER-SOAK wave-3 — 30-minute soak profile

Twin: this branch's release binary (rust HEAD 91a001e2 + lane fix
`3cb14881 events: PutRule honors the caller's State param`), host
process `--host 0.0.0.0 --port 4802`, `LAMBDA_EXECUTOR=docker
GATEWAY_PORT=4802`, docker.sock + /tmp mounted. Fresh in-memory state,
dedicated accounts (773100000001/.2/.3, 663200000001, 663300000001,
663400000001 — all torn down after the run).

## Resource profile (30 samples @60s; `analysis.json`)

- RSS 12.2 → 19.6 MB over 30 min; **slope 5.76 MB/hour** (< the 10 MB/h
  leak threshold → no leak finding); peak VmHWM 19.6 MB. Bounds match the
  rust-twin baseline in `tooling/perf-ab-2026-10-24` (bench-rust mem peak
  16.0–20.6 MB) — no drift beyond noise.
- Threads 20 → 25, open fds 16 → 20 — both plateau (sampler/boto pools).
- Lambda docker containers after settle (45 s): **0 leftovers** across
  the whole run (sampler every 60 s also saw 0 mid-run spikes).
- `/tmp/robotocore-lambda-{docker,exec,build,node,bridges}` entry counts:
  0 at end; 0 mid-run except transient extraction dirs that the executor
  reaps per invocation (`crates/lambda/src/docker_runtime.rs:1854`, the
  startup sweep (`:1720`) also reaped the pre-existing 13 stale bridge
  roots left by a previous process).

## Workload served

- Steady state: SQS 16,357 / S3 11,533 / DDB 11,406 / Lambda 59 / Events
  1,198 audit-listed requests; 0 steady-state errors, 0 twin errors
  (`usage/errors.total_errors=0`), audit tail 100% 200s (+10×400
  validation probes), zero 5xx.
- Docker-mode lambda: 30 waves (54 invokes incl. 5-concurrent bursts
  every 5th wave), 0 failures. Wave wall time 2.0 s → 12.9 s worst-case
  (the 5-concurrent burst stacking with the scheduled
  `chain-sched-lambda` docker invocation on the same tick boundary —
  the scheduler-thread edge; cold-start dominated, no wedging: the next
  waves return to normal and the families stay flat).
- Latency drift: minute-1 vs minute-25 checkpoint p50s flat
  (sqs_send 1.0→0.9 ms, s3_put 1.4→1.3, ddb_get 0.6→0.6).
- Post-soak serial bench vs `tooling/perf-ab-2026-10-24` rust baseline:
  every op within ±6% (`postsoak_bench.json`; baseline `results.json` b).
  sts 0.442 vs 0.469, s3_put 0.647 vs 0.676, sqs_send 0.416 vs 0.440,
  ddb_put 0.424 vs 0.449, ddb_query_range 0.917 vs 1.084, iam_list_users
  0.434 vs 0.461 — no degradation after the soak.

## Scheduler-thread edge (EventBridge windows)

158 scheduled deliveries counted over 5+ sweeps; per-control cadence
matches the era approximation (`crates/events/src/scheduler.rs`):
rate(1m)=25, cron(0/2)=13, wildcard `cron(* * * * ? *)`=5,
annual `cron(15 10 1 1 ? *)`=5, monthly `cron(30 12 1 * ? *)`=5 (300 s
default), fixed-minute `cron(45 * * * ? *)`=1 (3600 s bucket, only the
first-tick fire). Disabled rules: **0 fires across every sweep** (the
lane fix). Second account (SCHED_B) received exactly its own rules'
deliveries — account-scoped isolation holds.

Stray/unscoped shapes held cleanly:
- scheduled rule on a NON-default bus: accepted + fired by the era twin
  (moto fork rejects ScheduleExpression off the default bus,
  `events/models.py:1260`; recorded divergence, found harmless).
- stray rules in account 663300000001 pointing at the soak account's
  queue ARN delivered by ARN (`crates/sqs/src/bridge.rs` resolves the
  ARN's (account, region)) while the moto fork would deliver into the
  rule owner's own account (`events/models.py:254` uses
  `self.account_id`) — dual-family divergence noted; no wedging, no
  family latency regression, steady-state p95s stayed ≤1.8 ms.
- 20-rule create/delete churn every 3 min + a 50-target
  hooked-None codebuild rule each tick: scheduler cadence unaffected.
- 5-deep bus chain + 2-bus cycle + Scheduled→Lambda (docker) +
  Scheduled→SNS→SQS hops all bounded (chain sink drained 18 then
  bounded windows; `MAX_BUS_RECURSION=10` cap held; cycle kicks 1 ms).

## Gate results

- cargo fmt -p events / clippy -p events --all-targets -D warnings: clean.
- events unit tests 16/16; events compat suite 112/112 against the
  fixed twin on 4802.
- `make error-census` (sampled): 23/23 deny rows conform — fleet twin
  contract intact.
- `make probe-freshness`: EXIT 1 at HEAD for 22 services (pre-existing
  across arms, incl. events — re-probe events after this lane's arm
  merges; `probes/events.json` predates the 2026-10-25 arms).

## Harness notes

- `soak.py` bug found mid-run: with a 300 s sweep cadence the
  `int(elapsed_min)%10==4` gate never matches, so its chain-hop block
  never fired; `chain_driver.py` ran the chain probes beside the soak
  instead (hops 15+ kicks at 1 ms, bounded drains).
- Health `uptime_seconds` counts from first request, not process start
  (minor introspection semantic; noted, not a leak/drift metric).
