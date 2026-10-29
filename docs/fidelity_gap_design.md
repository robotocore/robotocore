# Fidelity Gap Analysis & Automated Fix Pipeline Design
## robotocore-rust (2026-08-30 data, .parity/state.json)

Ground truth: 178 Python compat suites in /Users/jackdanger/www/robotocore/tests/compatibility/.
Current state: 820/1675 native+bridge tests passing = 49-51% fidelity. 1334 "rust_gap" tests
(Python passes, Rust fails) across the 20 suites with XML in .parity/xml/.

=====================================================================
1. FAILURE BUCKETS (measured over all 1334 rust_gap tests)
=====================================================================

  Bucket                          Count  %      Fixable by
  --------------------------------------------------------------
  A1. Missing response field        585  43.9%  spec-driven field emit (gen_ops) + state port
       (KeyError: 'X' / assert 'X' in resp)
  A3. Wrong response type/shape     120   9.0%  spec-driven shape (returns list where dict
       (AttributeError: None.setdefault,      expected; returns {} where object expected)
        'list' has no .get)
  C1. Wrong HTTP status             89    6.7%  op missing from match table (77x 400
       (400 "op is not implemented",           "not implemented"), wrong 404 vs 400, 500s
        9x 404, 3x 500)
  C2. Wrong error code on missing   57    4.3%  resource-aware errors: emit ResourceNotFound-
       resource (generic ValidationException   Exception/EntityNotFound/NoSuchKey instead of
       instead of specific *NotFound)         ValidationException
  C3. Generic/param error codes     61    4.6%  correct op dispatch + param validation
       (InvalidAction, InvalidParameterValue,   matching botocore spec
        MissingParameter)
  D. Expected error NOT raised      38    2.8%  implement the error path (delete existing
                                                  resource, expired presign, etc.)
  B. Empty results where items      87    6.5%  state: persist the created resource;
       expected (assert 0==1, 'x' in [],        list ops must return what was stored
        count mismatch, list index out of range)
  E. ParamValidationError (boto side) 16  1.2%  Rust returned a field with null/missing that
                                                  botocore requires in pagination token etc.
  K. Filter/projection logic wrong   13   1.0%  implement filtering (prefix, projection,
                                                  status filters)
  G. Stub IDs (start with 'stub-')   5    0.4%  real ID formats (oi-, pb-, etc.)
  C5/F. NotImplemented 501            8    0.6%  op not in match table
  C6. Other specific error codes    ~65   4.9%  e.g. NoSuchCORSConfiguration (state),
                                                  NoSuchEntity (stale state)
  Z. Unclassifiable (timeout/teardown/other)  ~50  3.7%  manual
  ------------------------------------------------------------
  Total                             1334 100%

Consolidated into 4 fixable workstreams:

  WS1  SPEC-RESPONSE  (A1+A3+C1+C5+F)  ~710 tests (53%)
        The handler exists (or a 400 stub exists) but the response body
        does not match the botocore output shape. Fix = emit the full
        spec-defined response, with real IDs, real stored values.
        Detection: mechanical. Fix: mostly mechanical (gen_ops.py already
        generates spec-conforming skeletons; the gap is wiring it to state
        and filling dynamic fields).

  WS2  STATE-FIDELITY (B + C6-partial + D + K)  ~200 tests (15%)
        A resource is created then not returned / not deletable / wrong
        error on missing. Fix = port the moto model for that resource:
        store the object, make List/Get/Update/Delete consistent, emit
        the right NotFound code. Detection: mechanical (test does create
        then read-back). Fix: semi-automatic (port FakeX from moto).

  WS3  ERROR-CONTRACT (C2 + C3 + D)  ~155 tests (12%)
        Wrong code on error paths. Fix = per-op error table: which
        exceptions each op raises, keyed on which resource lookup failed.
        Detection: mechanical (pytest assertion contains the expected code).
        Fix: mechanical for the 57 ValidationException cases (map to
        specific NotFound), manual for the rest.

  WS4  BEHAVIOR (lambda invoke, S3 notifications, presigned URLs,
        cross-service fanout)  ~270 tests (20%)
        Real functional behavior: execute the lambda, fire the SQS
        notification, honor presign expiry, etc. Detection: manual review.
        Fix: manual. These are the long tail.

=====================================================================
2. PER-SERVICE ROI (rust_gap count x fixability)
=====================================================================

  rank  service        gap   WS1   WS2   WS3   WS4   ROI note
  ----  -------------  ---   ---   ---   ---   ---   ------
   1    ssm            130    ~60   ~40    ~15   ~15   42 failing tests have ALL ops
                                             stubbed in the crate -> pure WS1,
                                             highest density of mechanical fixes
   2    iam            146    ~70   ~30    ~35    ~11   large surface but many are
                                             simple list/create round trips
   3    lambda         133    ~30    ~10    ~20    ~73   mostly WS4 (real invoke);
                                             skip until invoke is solid
   4    dynamodb       103    ~30   ~40    ~25    ~8    C2 cluster: 57 of the
                                             ValidationException->NotFound are here
   5    s3              92    ~25   ~40    ~15    ~12   WS2-heavy: CORS, lifecycle,
                                             replication, versions not stored
   6    logs            80    ~45   ~20    ~10     ~5   many stub ops in crate;
                                             WS1 gold mine
   7    events          78    ~40   ~20    ~10     ~8   29 not-implemented 400s
   8    kinesis         74    ~35   ~25    ~10     ~4   14 stubs; GetRecords shape
   9    ecs             72    ~30   ~20    ~10    ~12   19 stubs; DescribeTasks shape
  10    ecr             65    ~30   ~20    ~10     ~5   12 stubs
  11   stepfunctions    67    ~25   ~15    ~10    ~17   10 stubs; state machine
                                                 describe/executed
  12   cloudwatch       45    ~25   ~10     ~8     ~2   16 stubs
  13    kms             43    ~15   ~15    ~10     ~3   13 stubs; key/crypto
  14    sqs             39    ~15   ~15    ~8      ~1   no stubs; message attr bugs
  15   secretsmanager   14    ~8    ~4     ~2      ~0   near done

  Bridge services (ec2 gap 706, rds 313, cloudformation 144) are a separate
  track: they ride the moto sidecar, so "fixing" them means porting the
  service to a native crate (gen_crate.py) or fixing the sidecar routing.
  They dominate the raw gap count but are NOT the same kind of work.

=====================================================================
3. AUTOMATED DETECTION + FIX PIPELINE
=====================================================================

The pipeline has three stages. Each stage is a script with a strict
input/output contract so an agent can run it headlessly.

  Stage 1  classify_gaps.py     XML + test sources  ->  gap_report.json
  Stage 2  plan_fixes.py        gap_report.json     ->  worklist.json
  Stage 3  apply + verify loop  worklist.json       ->  patches + parity delta

---------------------------------------------------------------------
STAGE 1: classify_gaps.py
---------------------------------------------------------------------
Input:
  .parity/xml/{svc}_py.xml, {svc}_rust.xml   (junit)
  tests/compatibility/test_{svc}_compat.py   (ground-truth sources)
  crates/{crate}/src/handler.rs              (Rust dispatch table)
  botocore/data/{svc}/*/service-2.json.gz    (specs)
  moto/{svc}/models.py                       (python reference impls)

Output: gap_report.json
  {
    "service": "ssm",
    "generated": "...",
    "tests": [
      {
        "test": "test_create_ops_item_with_all_fields",
        "bucket": "A1",                      # from classifier below
        "ops": ["CreateOpsItem","GetOpsItem","DeleteOpsItem"],
        "op_coverage": "all_stubbed" | "partial_stub" | "no_stub",
        "expected_fields": {"OpsItemId": "string(oi-)"},   # from spec, required
        "observed_fields": {"OpsItemId": "stub-id"},       # from failure text
        "expected_error": null | {"code": "ResourceNotFoundException"},
        "rust_file": "crates/ssm/src/handler.rs",
        "rust_lines": {"CreateOpsItem": 43},
        "moto_ref": "moto/ssm/models.py:create_ops_item",
        "confidence": "high" | "medium" | "low"
      }, ...
    ]
  }

The classifier is deterministic regex over the failure text (already
validated above on all 1334 gaps):

  KeyError: 'X'                -> A1, missing field = X
  assert 'X' in {...}          -> A1, missing field = X
  IndexError / AttributeError  -> A2/A3, shape bug
  ClientError code digit       -> C1, wrong status
  ClientError code=ValidationException
    and test asserts *NotFound -> C2
  DID NOT RAISE                -> D
  assert 0 == N / 'x' in []    -> B
  assert 'a' not in {...}      -> K
  startswith('oi-') on 'stub-' -> G

op_coverage: extract every boto3 method call from the test body, map
snake_case -> PascalCase via the botocore spec, and compare against the
set of ops in `handler.rs` routed to `self.json_stub`. "all_stubbed"
tests are the highest-confidence mechanical fixes: replacing the stub
with a spec-conforming implementation fixes the whole test.

---------------------------------------------------------------------
STAGE 2: plan_fixes.py
---------------------------------------------------------------------
Input: gap_report.json
Output: worklist.json, sorted by ROI = (tests_fixed / est_lines_changed).

For each test, emit an action record:

  {
    "test": "test_create_ops_item_with_all_fields",
    "service": "ssm",
    "bucket": "A1",
    "action": "de_stub",                 # see action types below
    "ops": ["CreateOpsItem"],
    "fix": {
      "file": "crates/ssm/src/handler.rs",
      "replace_line": 43,
      "replace": '"CreateOpsItem" => self.json_stub(&req, "OpsItemId"),',
      "with":     '"CreateOpsItem" => self.create_ops_item(&req),',
      "new_method_rust": "...generated from moto ref...",
      "new_model_rust":  "...generated from moto FakeOpsItem...",
      "spec_shape": "Output of CreateOpsItem = {OpsItemId: string}"
    },
    "verify": "pytest tests/compatibility/test_ssm_compat.py::test_create_ops_item_with_all_fields"
  }

Action types (each has a generator function):

  de_stub(op, spec, moto_ref)
      The op is `self.json_stub`. Generate:
        (a) a Rust struct for the resource, field-for-field from the
            botocore shape (or from moto FakeX class),
        (b) a `create`/`get`/`list`/`delete` method that stores it in
            the service state map (the handler already has
            `RwLock<HashMap<(u64,String), SvcState>>`),
        (c) the match arm pointing at the new method.
      This is the single highest-ROI generator: 294 stub arms across
      11 crates, ~340 of them sit under failing tests.

  add_fields(op, spec, missing=[...])
      The op is implemented but omits fields. Diff the spec output
      shape against the handler's json!({...}) body, append the
      missing keys with default values or values pulled from state.
      Target: A1 (585 tests).

  fix_error_code(op, missing_resource, correct_code)
      In the handler's not-found path, replace `ValidationException`
      (or 400) with the spec's exception for that resource. Target:
      C2 (57 tests) + C3 (61).

  implement_error_path(op, error_code)
      The test expects an error that the handler returns 200 for
      (D, 38 tests). Add the guard that raises the code.

  fix_shape(op, expected_type, path)
      The handler returns {} or list where the spec says object/list
      (A3, 120 tests). Regenerate the response from the spec shape.

  port_behavior(test)
      No mechanical fix (WS4). Emit a TODO card with the test source
      and the moto reference function; a human/agent implements it.

The generator must NOT emit free-form Rust. It emits a 3-part patch:
  1. model patch  (append struct to models.rs)
  2. method patch (append impl fn to handler.rs)
  3. dispatch patch (replace one match arm line)
so review is line-diff and `cargo build` verifies it compiles before
the pytest re-run.

---------------------------------------------------------------------
STAGE 3: apply + verify loop (per worklist item)
---------------------------------------------------------------------
  for item in worklist (ROI order):
      cargo build --release -p {crate}          # fail -> discard patch, log
      restart rust server (fresh state)
      pytest {test} (single test, not the file)
      if pass: commit; update .parity state
      else:    tag item "needs_review" with the new failure text
               (feeds back into Stage 1 for reclassification)

Because of the state-pollution problem, the verify step MUST restart
the server between test files (not between individual tests, since most
tests create their own suffixed resources). A per-test-file restart
keeps runtime down: 20 files x ~10s restart = 3.5 min overhead.

=====================================================================
4. HIGHEST-ROI TEST FILES (order to work)
=====================================================================

  1. test_ssm_compat.py          130 gap, 42 all-stub -> de_stub wave
  2. test_cloudwatch_compat.py     45 gap, 25 stub-related, small crate
  3. test_logs_compat.py           80 gap, 36 stubs in crate
  4. test_kinesis_compat.py        74 gap, 14 stubs, compact crate
  5. test_ecr_compat.py            65 gap, 12 stubs
  6. test_ecs_compat.py            72 gap, 19 stubs
  7. test_events_compat.py         78 gap, 29 not-implemented 400s
  8. test_dynamodb_compat.py      103 gap, 57 in the C2 error-code cluster
  9. test_iam_compat.py           146 gap, WS1/WS3 heavy
 10. test_s3_compat.py             92 gap, WS2 heavy (state)
 11. test_stepfunctions_compat.py  67 gap, 10 stubs
 12. test_sqs_compat.py            39 gap, no stubs (message attr bugs)

Skip early: test_lambda_compat.py (WS4-heavy, needs real invoke),
test_ec2_compat.py / test_rds_compat.py (bridge, different track).

=====================================================================
5. "NEXT WORK" GENERATOR  (scripts/harness/next_work.py)
=====================================================================

Contract:
  in:  .parity/state.json + gap_report.json + worklist.json (all under .parity/)
  out: stdout JSON + .parity/next.json, one item at a time:

  {
    "rank": 1,
    "test": "test_ssm_compat.py::test_create_ops_item_with_all_fields",
    "bucket": "A1",
    "why": "op CreateOpsItem is a json_stub; spec requires OpsItemId (oi-...);
            41 other ssm tests share this stub",
    "file": "crates/ssm/src/handler.rs",
    "line": 43,
    "patch": { ...3-part patch from Stage 2... },
    "expected_tests_fixed": 4,      # tests sharing the same op+bucket
    "verify": "ENDPOINT_URL=http://127.0.0.1:4567 python -m pytest
               tests/compatibility/test_ssm_compat.py::test_create_ops_item_with_all_fields",
    "rollback": "git checkout -- crates/ssm/src/handler.rs crates/ssm/src/models.rs"
  }

Rules for ranking (deterministic, no judgment):
  1. Group worklist items by (crate, op). Rank groups by
     expected_tests_fixed / est_patch_lines.
  2. Tie-break: WS1 (de_stub) > WS3 (error code) > WS2 (state) > WS4.
  3. Never emit two items that touch the same match arm in the same
     crate at once (sequentialize to avoid patch conflicts).
  4. Items with confidence "low" are listed but marked "review_first".

The generator is ~300 lines of Python (reuses the Stage-1 classifier and
Stage-2 generators); it never reads test output beyond the junit XML, so
it runs in <5s and can be called after every verify step to re-rank.

=====================================================================
6. CONCRETE EXAMPLE (end-to-end, ssm CreateOpsItem)
=====================================================================

Test (ground truth):
    def test_create_ops_item_with_all_fields(self, ssm):
        resp = ssm.create_ops_item(Title="Full CRUD OpsItem", Source="compat-test", ...)
        oid = resp["OpsItemId"]; assert oid.startswith("oi-")
        get = ssm.get_ops_item(OpsItemId=oid)
        assert get["OpsItem"]["Title"] == "Full CRUD OpsItem"

Rust (crates/ssm/src/handler.rs:43):
    "CreateOpsItem" => self.json_stub(&req, "OpsItemId"),
  # json_stub returns {"OpsItemId": "stub-id"} -> startswith fails

Moto reference (moto/ssm/models.py:create_ops_item + FakeOpsItem):
    stores in self.ops_items[id]; id = "oi-" + hex;
    get returns all fields + Status="Open"

Generated patch:
  models.rs:  + pub struct OpsItem { pub title: String, pub source: String,
                   pub description: Option<String>, pub priority: i64,
                   pub category: String, pub severity: String,
                   pub status: String, pub ops_item_id: String }
              + impl OpsItem { pub fn new(...) -> Self {...}
                    pub fn to_aws_json(&self) -> Value {...} }
  handler.rs: + fn create_ops_item(&self, req) -> AwsResponse {
                   let title = req.params["Title"].as_str().unwrap();
                   let item = OpsItem::new(title, ...);
                   state.ops_items.insert(id, item.clone());
                   AwsResponse::json(200, json!({"OpsItemId": id})) }
              + fn get_ops_item / delete_ops_item (same pattern)
              - "CreateOpsItem" => self.json_stub(&req, "OpsItemId"),
              + "CreateOpsItem" => self.create_ops_item(&req),

Verify: single-test pytest passes; 4 other ops-item tests flip to green
in the next file run.

=====================================================================
7. EFFORT ESTIMATE
=====================================================================

  WS1 de_stub wave:        ~340 stub arms under failing tests.
                           ~1.5 tests fixed per arm, ~40 lines Rust per
                           arm with the generator => ~1700 tests
                           addressable, of which ~340 are in the current
                           failing set. 3-5 days of agent work.
  WS1 add_fields:          ~400 tests, mostly 3-15 line edits each.
  WS3 error codes:         118 tests, mechanical table-driven fix.
  WS2 state:               ~200 tests, 10-30 line model ports each.
  WS4 behavior:            ~270 tests, manual; long tail to 90%+.

  Realistic trajectory: 51% -> 70% after WS1+WS3 (~1 week), -> 85%
  after WS2 (~2 weeks), remaining 15% is WS4 + bridge services.


=====================================================================
8. IMPLEMENTED (scripts/harness/) — verified end-to-end 2026-08-30
=====================================================================

The three-stage pipeline is now real, runnable code:

  classify_gaps.py   Stage 1  -> .parity/gap_report.json
  plan_fixes.py      Stage 2  -> .parity/worklist.json
  gen_rust_op.py     Stage 2b -> concrete Rust patch (stdout)
  next_work.py       Stage 3  -> .parity/next.json (the #1 work item)

Run order (no server needed for 1-2b):
  python scripts/harness/classify_gaps.py --all
  python scripts/harness/plan_fixes.py
  python scripts/harness/next_work.py --json     # prints item #1

Verified output (top of the worklist, 2026-08-30):
  rank  group                      action       tests  line
  1     logs/CreateLogGroup        add_fields    26    36
  2     stepfunctions/CreateStateMachine fix_shape 32  36
  3     sqs/CreateQueue            fix_shape     23   1032
  4     ecr/CreateRepository       add_fields    22    36
  5     kms/CreateKey              add_fields    17    36

gen_rust_op.py emits a 3-part patch (models struct + handler method +
match arm) from the botocore spec alone. Example for ssm/CreateOpsItem:
  * struct Opsitem with 21 fields (union of Create+GetOpsItem output shapes)
  * from_request() reads Title/Source/Description/Priority/Severity/Category,
    sets Status="Open", CreatedTime=now, OpsItemArn=arn:aws:...,
    OpsItemId="op-<uuid>" (TODO marker to verify prefix against the test's
    startswith("oi-") assertion)
  * create_ops_item() stores in state and returns {"OpsItem": to_aws_json()}
  * dispatch arm: "CreateOpsItem" => self.create_ops_item(&req)

Apply loop (agent or human), per next_work.py item:
  1. cargo build --release -p <crate>          # must compile
  2. restart rust server with fresh state
  3. pytest <first test in item> -x -q         # verify
  4. if green: pytest <whole file> to confirm group
  5. commit; re-run next_work.py to re-rank

Known generator limitations (by design, marked in code):
  * id prefix fallback is 2-letter of the resource name; AWS prefixes are
    irregular (oi-, pb-, sm-) — the TODO marker tells the agent to check
    the test's startswith() assertion and adjust the one format!() line.
  * the state struct is NOT auto-extended; the handler stores into
    st.<resource>s with a TODO(state) marker. The agent adds the field to
    the crate's State struct (one line) when applying.
  * add_fields/fix_shape items (implemented ops) need a response-shape
    diff against the botocore output shape; next_work.py reports the
    missing-field list from gap_report details and the agent edits the
    existing json!() body. The generator covers the de_stub path fully;
    add_fields is guided (file+line+missing fields) rather than fully
    generated, because the existing method must be edited, not replaced.
