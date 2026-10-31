# Botocore-surface coverage & exercise plan — the road to 100%

Status: design (2026-10-22). Companion artifacts: `probes/*.json` (per-op
wire truth), `tooling/ec2-crash-triage.json` (crash triage),
`tooling/perf-ab-2026-10-21/` (perf baseline), `docs/moto-parity-progress.md`
(the wave ledger).

---

## 0. Why this plan exists (the numbers, machine-computed from probes)

The fleet gates above (G1–G5) prove *moto parity*: every operation the
deployed moto fork implements is served natively, and there are zero
moto-implemented operations still 501ing on the twin (the last true gap,
IoT's tag family, closed in `b57b4c97`). That contract is satisfied, and
the remaining not_implemented rows are moto-absent — verified
mechanically (1,146 of the 1,163 lack any fork handler; the handful of
mislabels were re-homed in the classifier fix).

But the contract is *not* the botocore surface. Today's measured state
over all 10,391 modeled operations (160 registered services):

| bucket | ops | share | meaning |
|---|---|---|---|
| working | 8,396 | 81.8% | answered on the wire, probe-verifiable |
| not_implemented | 1,163 | 11.3% | honest moto-absence: 501, like moto |
| needs_params | 649 | 6.3% | probe could not credibly fill required inputs |
| 500_error | 60 | 0.6% | crash-faithful (triaged, see tooling/) |

"100% coverage of the botocore surface" therefore decomposes into four
distinct gap classes, each needing a different machine. Note the three
biggest clusters are all `fork-present` services (connect 133,
lexv2models 75, personalize 67, networkfirewall 61, forecast 57,
bedrockagent 56, directconnect 52, cognito-idp 52, ssoadmin 47 …) — moto
implemented *some* of those services and we mirror it; the tail is where
moto stopped and we inherit the cliff. Everything in the top-22 is
fork-present: the surface is dense in its long tail.

## 1. Definitions (the contract)

- **Operation** = one wire method+URI (+X-Amz-Target) in a botocore
  model. Exactly 10,391 across the 160 registered services.
- **Covered** means an operation (a) dispatches to a real rotocore arm,
  (b) answers a *modeled* wire envelope — HTTP 2xx/4xx whose code and
  body validate against the botocore output shape — and (c) has at
  least one committed exercise artifact (probe row + a test method).
- **Modeled absence is a decision**, recorded per op in an absence
  manifest, not a way out of coverage: the twin answers the same deny
  shape it answers today, the probe and the budget count it as
  `modeled_deny` (absent-but-accounted), and the manifest row carries
  the reason (no moto handler; requires unmodeled real-AWS state;
  docker-runtime-only).
- The coverage number CI tracks is then
  `covered = working + denied-with-modeled-shape`. **A 500 is never
  covered** (a crash either is or is not faithful; both states are
  recorded, the shape is pinned by a test, and the crash is a bug to
  file each time the crash is the twin's own).

## 2. Ground-truth hierarchy (unchanged, one tier added)

1. compat-suite recorded behavior (the executable spec)
2. the deployed moto fork (`2016563fd`) — where moto implements
3. NEW — the botocore wire model itself (`service-2.json.gz`): required
   members, enums, ARN formats, pagination tokens, output shapes. For
   the 1,146 moto-absent ops this is the only mechanical authority:
   shapes give required inputs + declared output members; the modeled
   response synthesizes valid values for required outputs (real AWS
   never returns null for members the model requires)
4. real-AWS observation, when an operator records a live sweep — the
   captured shape then becomes the fixed fixture
5. deny-envelope (modeled_deny, manifest row + unit test pinning it)

Tiers apply in order per op; the outcome and tier used are recorded in
the op's manifest row.

## 3. The machines to build (tooling tier)

**3a. `scripts/coverage.py` — the single surface machine.** Inputs:
botocore models (repo venv), the twin registry, probes. Output
`tooling/coverage.json`: `service → op → {status, screens passed,
exercised-by [probe|compat|unit|iac|recipes], bucket, absence reason}`.
One artifact; the site's coverage dashboard reads it; nothing else
claims to know what "covered" means.

**3b. Probe auto-fill gen-2 — closes the 649 needs_params.** Under-fill
causes measured: required nested members, enum members, ARN-format
members, date-time floors, base64 blobs, pagination-required tokens.
Teach `lib/param_filler.py` shape-driven synthesis: walk required
members recursively (botocore shapes carry `required` lists), synthesize
enums from `enum`, timestamps from clock, ARNs from the partition/regional
patterns, and honor member patterns. Exit: needs_params = 0 and every
modeled op contacts the twin exactly once per sweep.

**3c. Response-shape validator — the "exercised correctly" instrument
at the wire tier.** After each probe response, parse per the model's
response shape: (a) no undeclared members, (b) required members present
and typed, (c) written attributes non-null where required. One
`conformance: true|false` row per op, evaluated on every sweep — the
machine gate for wire correctness, run each probe sweep rather than
per commit.

**3d. Absence manifest.** `tooling/surface-absence.json`: per
not-yet-covered op → {service, why, deny envelope, owning wave}. The
manifest *is* the backlog; rows only disappear by implementation or a
ledger-documented decision. Nothing is deleted.

**3e. Coverage budget in CI.** Two gates on the machine, wired into
`make parity` so the harness regenerates and fails on real drift
(exit code 2 on any non-monotonic movement): (1) per-service
`covered` monotone ratchet (the G4 pattern, applied per op), (2) an
absolute budget line updated only by ledger-documented ratchet commits.
The parked CI workflow runs these gates the moment the workflow token
lands.

## 4. The port plan (machine side: reaching 100% modeled)

Sequenced by cost, reusing the fleet pattern that closed waves 1–10
(per-service worktree + gates + suite + probe refresh + push rhythm):

- **W-A crash sweep hand-off (1 agent).** The 60 crash rows are already
  triaged in `tooling/ec2-crash-triage.json`; where moto crashes but the
  botocore model promises a modeled 4xx or 2xx, implement; where the
  crash is moto's own faithful behavior, pin it with a unit test (the
  shield-500-quirk pattern) and record `modeled_deny`.
- **W-B alignment sweep.** The residual `fork-has-handler` rows (single
  digits, recalculated per sweep after the classifier fix) — cheap
  routing alignments, one agent.
- **The 1,163-implementation tier ~8 waves × ~150 ops**, regrouped so
  each agent carries ≤ ~110 ops across ≤ 3 services:
  - cluster 1: connect (133)
  - cluster 2: lexv2models (75) + personalize (67)
  - cluster 3: networkfirewall (61) + forecast (57) + directconnect (52)
  - cluster 4: bedrockagent (56) + cognito-idp (52) + ssoadmin (47)
  - cluster 5: workspacesweb (37) + service-quotas (21) + ec2 (236
    spread across its family generators — carried by two agents
    splitting on family)
  - clusters 6..n: memorydb (15), datasync (15), emrcontainers (12),
    cognitoidentity (12), s3control (11), redshift (11), fsx (11),
    timestreamquery (10) and the long tail ≤ 10 rows per service,
    finishing with twice as many as the biggest per-agent load.
- Per-op procedure: botocore shape → arm skeleton from the module's
  existing family idioms → state fields where the op is meaningfully
  stateful (create/update machinery the module already models) → unit
  tests in the module idiom → probe round 1 → generated suite row
  (see §5 screen 3) → absence-manifest row retired.
- **Terminating the per-op 501 catch-alls**, one cluster at a time:
  a module's `_ =>` catch-all starts sourcing its deny envelope from
  the absence manifest, and the manifest shrinks to zero as arms land.

## 5. How everything gets exercised correctly (the four screens)

Defined so "correctly exercised" is mechanical, cumulative, and
reviewable:

1. **Wire/datalogger screen** — the probe sweep with the response-shape
   validator of §3c; fleet conformance ratio reported; regression
   breaks `make parity`.
2. **Unit screen** — every implemented op gets a unit test asserting
   the wire envelope under its protocol (query/json/rest-json/
   rest-xml) plus the state writes that matter. Modules scale with
   their existing test idiom.
3. **Caller screen** — a compat suite where a boto3 client assesses
   every op of the service with parameters the twin honors, asserting
   row-level state semantics: create → describe(+assert) → mutate →
   re-describe → delete → negative path. Auto-generated seed rows are
   permitted but named `TestGen<Op>` and reviewed opportunistically;
   the 12,873 hand-curated tests remain the core.
4. **System screens** — the durable harness tiers: IaC materialization
   lanes (the deepest cross-service mechanic), cross-service recipes
   (RGTA tag surfacing, ESM, destinations, notifications), and the
   perf/robotics harness.

"Exercised correctly" = green on screens 1–2 fleet-wide, plus green on
screens 3–4 for all ops of every live-usage service family. Modeled
denies skip screen 3 wiring but carry their manifest row plus a unit
test asserting the exact deny envelope.

## 6. Costs & honest boundaries

- ~1,163 moto-absent ops ≈ 8–15 waves at fleet cadence (wave-8 twice
  shipped 12 services with this pattern in days) — 1–2 weeks of
  focused rounds.
- needs_params (649) is one filler-upgrade turn; dependencies: none.
- Crash triage (60) bounded by the existing EC2 artifact + sweep.
- Structurally out of reach without novel modeling (must be declared
  in the manifest, not implicit): Lambda container-image runtimes and
  anything needing an external endpoint live. For these, respond with
  the same JSON error envelope real AWS returns, never a 500.
- Coverage budget ratchet edges: no new python outside the four
  allowed dirs; the coverage machine is python in scripts/ (allowed)
  with its JSON artifact in tooling/. The budget gates step into CI
  when the workflow token lands.

## 7. Wave order & exit criteria

- W1 — coverage machine (`scripts/coverage.py`, `tooling/coverage.json`)
  + filler gen-2 (needs_params → 0) + shape validator in probe runs.
- W2 — absence manifest for all 1,163 rows (no behavior change; the
  manifest becomes the backlog).
- W3.. — one cluster-wave at a time (~150 ops each), budget ratchet
  per wave; per-wave exit: probes 0 needs_params for the cluster,
  conformance green, unit+caller tests in the tree, manifest rows
  retired.
- Crash triage re-run after each region of clusters touches the
  family (the EC2 pattern: triage → implement faithful or modeled).
- Final exit: `tooling/coverage.json` shows 10,391/10,391 covered,
  modeled_deny ≤ declared absence, conformance green everywhere, and
  every op present on ≥ 2 of the 4 screens. That number — not a vibe —
  is the finish line.
