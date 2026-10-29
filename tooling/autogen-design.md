# Auto-generation & diff-fixing pipeline design for robotocore-rust

## 1. Current tooling audit

The scripts live in `scripts/harness/` (not top-level `scripts/`):

| Script | Purpose | Quality |
|---|---|---|
| `gen_crate.py` (10.7KB) | Scaffold a whole crate (Cargo.toml, lib.rs, protocol.rs, models.rs, handler.rs with 501 stubs) from a botocore `service-2.json.gz` | Decent skeleton, but: hard-coded venv path, ignores REST protocols (rest-json/rest-xml path routing), models.rs is a generic `HashMap<String,Value>` with no real resource model, no error type generated |
| `gen_ops.py` (6.9KB) | Generate spec-conforming **stub** responses (default values for every output field, correct wire names incl. `locationName`) | Good for shape-correctness but the defaults are fake (`""`, `0`, `[]`) — passes nothing behavioral; query/XML branch emits hand-rolled XML strings (brittle); no input-side extraction |
| `spec_ops.py` (4.6KB) | Coverage report: which spec ops are in the dispatch `match`, which are missing; writes commented-out stubs | Read-only, useful as coverage oracle. Breaks if the venv path changes |
| `parity.py` (16.4KB) | Runs the Python compat suites vs :4566 (ground truth) and :4567 (Rust), 4-way classifies, writes `.parity/state.json`, derives "next work" | The single most important script. It's the *measurement* layer. It does NOT produce per-test raw responses — only pass/fail — which is exactly what the auto-fixer is missing |
| `diff_test.py` (27KB) | Hand-written per-service boto3 call pairs run against both endpoints, normalize volatile fields, report first diff | 18 services × ~5 ops each = ~90 hand-written scenarios. Duplicated knowledge with the compat suites; not driven by the spec; normalization list is wrong (strips `TableName`, `Count`, `Count` etc. — real assertions!) |
| `auto_validate.py` (6.8KB) | Single op, single param set, call both endpoints, field-by-field diff | Correct primitive. One-shot, no fixture setup, no param generation, no patch emission |
| `capture.py` + `golden_test.py` | Record raw HTTP req/resp from Python server (re-signed on replay), replay vs Rust, diff | The most reusable infra here: golden baselines are the right oracle for auto-generation |
| `gen_provider.py` (14KB, in `scripts/`) | Python-side provider scaffolding (starlette) | Not for the Rust port |

Key data points measured from the repo:

- 17 crates, 969 dispatch arms; ~305 (31%) are stub-shaped (`json_stub`, `NotImplemented`, fake values), ~66% are "real" implementations.
- Of the 17 native services, spec totals ≈ 950 ops; only ~970 dispatch arms exist, so **~25–30% of spec ops are entirely missing from dispatch** (s3: 40 missing of 111; ssm: 43/146; iam: 7/176; sqs: 9/23).
- `.parity/state.json` (2026-08-30): 820/3379 = **24.3%** test fidelity across 18 native services (parent's 51.1% figure was an earlier snapshot).
- Complexity census from botocore specs (member counts of input+output shapes): **87% of all ops in the 17 services are "simple or medium"** (≤30 total members); only ~13% are structurally complex.
- Every existing handler follows the same 3-part pattern: (a) extract params via `req.params.get(...).and_then(...)`, (b) read/write a per-(account,region) `RwLock` store, (c) build the response with `json!`/`xml!`. That uniformity is what makes generation feasible.

## 2. Mechanical vs. genuinely complex

**Mechanical (auto-generatable from spec, ~70–80% of code volume):**

1. Crate scaffolding — `gen_crate.py` does this already.
2. Dispatch table (`"Op" => self.op(&req)`) — pure spec data.
3. Param extraction — spec gives member name, type, required, wire name (`locationName`, `location`, `locationNameList`).
4. Response serialization — spec output shape → `json!`/XML tree with correct field names, including required-vs-optional handling.
5. Error shape — spec gives `errors: [{code, shape, exception}]` with the error member shape; the `__type`/`Code` body is fully determined.
6. ID generation (UUIDs, ARNs from `resourceArn` patterns), timestamp fields, `NextToken` presence — determined by spec + a small conventions table.
7. "Empty-but-correct" responses for read/list ops on a fresh account (empty lists, `NextToken` absent) — this alone flips hundreds of parity tests from fail to pass.

**Genuinely complex (requires the Python server as oracle or human/agent judgment, ~20–30%):**

1. **Cross-operation state semantics**: e.g. SQS `ReceiveMessage` visibility timeout + redelivery, message MD5 (body+attrs), delayed messages; S3 multipart ETag composition, conditional PUT (IfMatch/ETag), versioning; DynamoDB conditional writes, TTL, Streams sequence numbers.
2. **Validation rules** beyond spec: name formats, length limits, region constraints (S3 `CreateBucketConfiguration`), cross-field invariants. The spec has `min`/`max`/`pattern` for some but not all.
3. **Behavioral quirks**: pagination token round-trips, eventual consistency (DynamoDB table ACTIVE), error *message* text (tests assert exact messages in some cases), ordering of list results.
4. **REST protocol routing** (s3 rest-xml, lambda rest-json): path+method+header-based dispatch, subresource semantics — cannot be a flat match on operation name.
5. **Side effects between services** (SNS→SQS fanout, Lambda triggers) — out of scope for per-op generation; needs an event layer.

**Estimated auto-generation coverage of the 1425 failing parity tests:**
- ~40% (≈570) are "field missing/shape wrong/empty response wrong" → fixed by spec-driven response generation alone.
- ~25% (≈360) are "value wrong for a concrete input" (e.g. computed MD5, ARN format, idempotency) → fixed by oracle-diff auto-fixer using golden captures.
- ~35% (≈500) are cross-op stateful behavior or validation → need agent-implemented logic, but the generator can scaffold the correct skeleton so each is a fill-in, not a from-scratch task.

## 3. Auto-generation pipeline design

### 3.0 Shared foundation: `scripts/harness/spec_lib.py`

One module both generators and the fixer import, so spec reading stops being duplicated (currently 3 different `find_spec` implementations with 3 different venv paths):

```python
# scripts/harness/spec_lib.py

BOTOCORE_DATA = os.environ.get("BOTOCORE_DATA",
    "/Users/jackdanger/www/robotocore/.venv/lib/python3.12/site-packages/botocore/data")

def load_spec(service: str, version: str | None = None) -> dict
    # service-2.json.gz for the latest (or given) version; also reads
    # endpoint-1.json for rest-xml/rest-json path/param/header placement

def op_info(spec: dict, op: str) -> OpInfo
    # dataclass: name, method, uri, input_shape, output_shape,
    #           error_codes: dict[code, shape_name], protocol, target_prefix,
    #           required_inputs: [name], idempotent: bool, deprecated: bool

def walk_shape(shapes: dict, name: str) -> Shape
    # recursive resolver -> RustType: String | Int | Long | Float | Bool
    #   | List(Shape) | Map(Shape) | Structure({name: Member}) | Timestamp | Blob
    # Member carries: wire_name (locationName or name), location
    #   (body|query|header|uri|statusCode), required, enum_values, min/max/pattern

def rust_type(shape: Shape) -> str          # &str, i64, f64, bool, Vec<T>, Map, ...
def default_value_rust(shape: Shape) -> str # json!(...) literal per type (fixes gen_ops.py)
def required_check_code(op: OpInfo) -> str  # generates the MissingParameter checks
```

### 3.1 `scripts/harness/gen_handler.py` — the main generator

Replaces the stub emission in `gen_crate.py`/`gen_ops.py` with *executable* skeletons.

```python
# scripts/harness/gen_handler.py
#
# Usage:
#   python scripts/harness/gen_handler.py --service ssm --out crates/ssm/src
#   python scripts/harness/gen_handler.py --service ssm --ops PutParameter,GetParameter \
#       --oracle http://127.0.0.1:4566   # fills values from live Python server
#
# Exit codes: 0 = generated & cargo check passed; 1 = spec error; 2 = compile error

def generate_crate(service: str, out_dir: Path, ops: list[str] | None = None) -> None
    # superset of current gen_crate.py:
    #  - Cargo.toml (existing)
    #  - lib.rs (existing)
    #  - protocol.rs: PROTOCOL-CORRECT request accessors, not one generic
    #    `params: Value`:
    #      json / query :  params from body/form
    #      rest-json    : params from path/query/header per `location`
    #      rest-xml     : XML body parse via quick-xml, path routing table
    #  - error.rs: one enum per spec error code (from op errors + service
    #    "error" shapes), with to_status() and to_json()/to_xml() per protocol.
    #  - models.rs: per-resource structs derived from spec resource shapes
    #    (spec.resources gives ARN + identifiers), e.g. Queue, Parameter, Table.

def generate_handler_file(service: str, ops: list[str], oracle: str | None) -> str
    # For each op, emit:
    #   fn <snake_op>(&self, req: &AwsRequest) -> Result<AwsResponse, SvcError> {
    #       let name: Option<&str> = req.param("Name");       // typed extraction
    #       let name = name.ok_or(SvcError::MissingParameter("Name"))?;
    #       <pattern/min/max validation from spec>            // mechanical
    #       let store = self.get_store(req.account, &req.region);
    #       <BODY>                                             // see oracle fill below
    #       Ok(AwsResponse::json(200, json! { ... } ))         // from output shape
    #   }
    # Plus the dispatch match arm.

def body_from_oracle(service: str, op: str, params: dict, oracle_url: str) -> str
    # (a) generate a minimal valid request for `op` (required fields filled
    #     from param-fill rules, optional fields left out);
    # (b) send it to the Python server via botocore;
    # (c) capture the raw response (reuse capture.py's normalization);
    # (d) if the response is a pure function of the input (re-run 2x,
    #     same result), emit the mapping code: param extraction -> value
    #     expression. E.g. GetCallerIdentity -> Account derived from req.account.
    # (e) if stateful, emit a *skeleton with TODO + the captured golden
    #     response as a #[cfg(test)] expected value* so the test is generated too.

def emit_test(service: str, op: str, params: dict, golden: dict) -> str
    # a Rust unit test in crates/<svc>/src/tests.rs that drives the handler
    # directly (no server) with the captured golden, volatile fields masked.

def main() -> None
    # 1. load spec
    # 2. generate
    # 3. run `cargo check -p <crate>` and `cargo test -p <crate> --no-run`
    #    on the generated crate; on compile error, write the failing
    #    snippet to stderr (the generator must be idempotent and
    #    re-runnable after a fix)
```

Why the oracle step matters: the output shape from the spec tells you *fields*, not *values*. Calling the Python server with a generated param set gives values, and the generator can then classify each output field:

- **Constant across calls** → literal in code (e.g. `"Status": "ACTIVE"`).
- **Echop of an input param** → `req.param(...)` pass-through (covers ~40% of outputs).
- **Derived from account/region/uuid** → convention (ARN template from spec resource).
- **Function of input state** → left as TODO with golden test.

### 3.2 `scripts/harness/gen_params.py` — request parameter synthesis

Needed by both the generator and the fixer:

```python
# scripts/harness/gen_params.py
def synth_params(spec: dict, op: str,
                 fixtures: dict[str, str] | None = None) -> dict
    # Fill required members: strings from name pool + uuid, ints from
    # plausible ranges, timestamps to now, enums to first value,
    # blobs to b"test". `fixtures` injects pre-created resource names
    # (bucket=..., queue=...) so dependent ops reference live resources.
    # Returns (params, cleanup_calls) so the caller can tear down.

def fixture_plan(spec: dict, ops: list[str]) -> list[tuple[service, op, params]]
    # topological order of create-op-before-read-op using the spec's
    # "creates"/"resource" hints + a small per-service table
    # (create_bucket before put_object, create_queue before send_message...).
```

### 3.3 `scripts/harness/param_fill.json` — the one human-maintained file

Small data file encoding the conventions the spec cannot express:

```json
{
  "sqs":  {"QueueName": "q-{uuid8}", "region": "us-east-1"},
  "s3":   {"Bucket": "b-{uuid8}", "Key": "k-{uuid8}.txt", "Body": "hello"},
  "dynamodb": {"TableName": "t-{uuid8}"},
  "conventions": {
    "arn": {"sqs": "arn:aws:sqs:{region}:{account}:{name}", ...},
    "id":  {"Parameter": "p-{uuid12}", "BaselineId": "pb-{uuid8}", ...},
    "md5": "computed"   // field names requiring real computation
  }
}
```

This is the ONLY hand-maintained artifact of the pipeline; everything else is spec-derived.

### 3.4 Per-service protocol notes (why S3/Lambda get a second pass)

`gen_handler.py` v1 supports `json`, `query` fully and `rest-json` partially. `rest-xml` (S3, EC2, STS, Route53) needs an additional step:

```python
def generate_rest_xml_routing(service: str) -> str
    # builds a (method, path-regex) -> op table from endpoint-1.json +
    # requestUri, emits a match on (req.method, req.path) instead of
    # req.operation, plus quick-xml request parsing per input shape.
```

## 4. Diff-driven auto-fixer design

### 4.1 `scripts/harness/diff_fix.py`

The loop the parent asked for: failing test → expected response → actual Rust response → exact patch.

```python
# scripts/harness/diff_fix.py
#
# Usage:
#   python scripts/harness/diff_fix.py --service ssm --op GetParameter \
#       --params '{"Name": "/x"}'            # ad-hoc single op
#   python scripts/harness/diff_fix.py --from-parity .parity/state.json \
#       --service sqs [--max-fixes 20] --apply
#
# Pipeline:
#   1. REPRODUCE   send the request to Rust :4567 (and optionally Py :4566
#                  for the expected value) using botocore.
#   2. EXTRACT     expected = py response (or golden capture); actual = rust response.
#                  Both normalized with capture.py's volatile rules.
#   3. CLASSIFY    each diff is one of:
#                     MISSING_FIELD   rust response lacks a field
#                     EXTRA_FIELD     rust emits a field AWS doesn't
#                     VALUE_MISMATCH  field present, wrong value
#                     TYPE_MISMATCH   str vs number etc.
#                     ERROR_MISMATCH  different status/Code
#                     ORDER_MISMATCH  list element order
#   4. LOCATE      map (service, op, field) -> the Rust fn in handler.rs
#                  (fn name = snake_case(op); field = the json! key string).
#                  Parse handler.rs with a small regex-based locator; the
#                  generated code has a stable marker comment per op:
#                    // <<op:GetParameter>>          <- emitted by gen_handler
#                  so the patch target is always findable.
#   5. PATCH       per classification:
#                     MISSING_FIELD   insert `"Field": <expr>` into the json!
#                                     (expr from: input echo / convention /
#                                     captured value / TODO-golden)
#                     VALUE_MISMATCH  replace the literal; if the wrong value
#                                     is a captured constant, replace with it;
#                                     if it's an input echo, replace with
#                                     req.param("..."); otherwise emit
#                                     a TODO with both values + golden test.
#                     ERROR_MISMATCH  replace the error code/status; if the
#                                     spec defines the error shape, generate
#                                     the full error branch.
#                     EXTRA_FIELD     remove the key from json!.
#   6. VERIFY      cargo build -p <crate> && run the targeted unit test
#                  (the golden test from 3.1e) and re-run the original
#                  failing parity test against a fresh server build.
#   7. RECORD      append to .parity/fixlog.jsonl: op, diff, patch,
#                  verified (yes/no), so the agent can see what was
#                  auto-fixed vs what needs judgment.

def reproduce(service, op, params, rust_url, py_url) -> Repro
    # dataclass: req_b64, expected: RawResp|None, actual: RawResp,
    #            both_errors: bool

def classify(expected: RawResp, actual: RawResp) -> list[Diff]
    # Diff: kind, json_path, expected_value, actual_value

def locate(service: str, op: str, field: str) -> (file, line_span)
    # find // <<op:...>> marker, then the field key within the fn body

def patch(diff: Diff, ctx: LocContext) -> Patch | None
    # returns None when the fix needs judgment (record as TODO)

def verify(crate: str, op: str, patch: Patch) -> bool
    # apply to a temp copy? No — apply in place, cargo build, cargo test,
    # keep the fix; revert if the test fails (git stash or .bak).

def fix_from_parity(state: dict, service: str, max_fixes: int, apply: bool) -> None
    # For each rust_gap_test: parse the test name -> (class, method);
    # extract the boto3 call + params from the Python test source
    # (ast.parse the compat test file — tests are declarative enough);
    # run steps 1-7.
```

**Test-source extraction**: the parity `rust_gap_tests` are pytest nodeids. `ast`-parse `tests/compatibility/test_<svc>_compat.py`, find the method, and walk the boto3 client calls in it to recover (op, params, fixtures). This is the missing link that makes the fixer test-driven rather than param-guessing.

### 4.2 `scripts/harness/golden_fill.py` — batch golden capture per op

```python
# For every (service, op) in the parity gap:
#   1. fixture_plan -> create prerequisites on BOTH endpoints
#   2. capture.py-style raw capture against Python
#   3. store golden at .goldens/<service>/<op>.json
#      (request + expected response + volatile paths)
#   4. emit a Rust golden test (3.1e)
# This turns every failing parity test into a unit test that needs no
# server and can run in `cargo test` — the fixer then works offline.
```

## 5. What CANNOT be auto-generated, and how to minimize it

| Category | Examples | Minimization strategy |
|---|---|---|
| Stateful semantics | SQS visibility timeout, S3 versioning, DynamoDB conditional writes, KMS key state machine | Generator emits the **store access + TODO**; the Python server's recorded multi-op sequences (golden capture of *sessions*, not single ops) give the expected transitions, so the agent gets a "fill the diff on this state machine" task, not a blank file |
| Cross-field validation | S3 bucket name rules, IAM trust policy JSON | Keep a per-service `validation.md` checklist; the generator emits the *check call sites* (where a `validate_bucket_name()` must run) and the agent fills the rules. Most are 3–10 lines |
| Exact error *messages* | tests assert `"Queue does not exist: ..."` | The oracle diff captures the exact string; auto-fixer replaces the literal. Only fails when the message embeds a resource id — handled by a `format!` template derived from the golden |
| REST/S3 subresource semantics | S3 `?acl`, `?tagging`, multipart | `generate_rest_xml_routing` handles routing; subresource handlers are scaffolded per query-param; the agent implements each against its golden test |
| Cross-service side effects | SNS→SQS, Lambda triggers | Out of the generator's scope; keep the existing event layer. The pipeline should explicitly mark these ops as `skip: cross-service` so they don't pollute fixer stats |
| Time/sequence-dependent behavior | DynamoDB table ACTIVE transition, Kinesis shard sequence numbers | Golden *session* captures (ordered multi-op) as the oracle; generator scaffolds, agent implements state machine |

**Workflow structure to minimize the human/agent slice:**

1. **Generator pass** (minutes, no judgment): `gen_handler.py --all` scaffolds every unimplemented op with correct param extraction, error branches, and spec-correct response shells. Compiles by construction.
2. **Golden pass** (minutes, no judgment): `golden_fill.py` captures a golden + unit test for every op in the parity gap. Now every gap is a *failing Rust unit test* — no server needed to iterate.
3. **Auto-fix pass** (hours, low judgment): `diff_fix.py --from-parity --apply` fixes MISSING/EXTRA/constant-value/error-code diffs. Expect this to close ~60% of the remaining gap automatically.
4. **Agent pass** (the real work, now bounded): each remaining failing golden test is a small, isolated stateful-semantics task with the expected response pinned in the test. No exploration, no reverse-engineering the API.
5. **Re-measure**: `parity.py` after each pass; the state file drives which service/op the agent picks next (largest-gap-first, already implemented in `derive_next_work`).

The invariant that keeps the agent slice small: **every op the agent touches has (a) a compiling skeleton, (b) a failing golden unit test with the exact expected bytes, and (c) the param set that reproduces it.** The agent never has to discover the API surface or guess expected values.

## 6. Concrete file layout

```
scripts/harness/
  spec_lib.py            # NEW: single botocore-spec reader + shape walker
  gen_handler.py         # NEW: replaces gen_crate.py + gen_ops.py (keeps CLI compat)
  gen_params.py          # NEW: param synthesis + fixture planning
  param_fill.json        # NEW: hand-maintained conventions (only manual file)
  golden_fill.py         # NEW: batch golden capture + Rust test emission
  diff_fix.py            # NEW: reproduce → classify → locate → patch → verify
  parity.py              # unchanged (measurement layer)
  capture.py             # reused for normalization/capture primitives
  golden_test.py         # unchanged (replay + diff of raw HTTP)
  gen_crate.py           # deprecated (subsumed by gen_handler.py)
  gen_ops.py             # deprecated (subsumed by gen_handler.py)
  diff_test.py           # deprecated (subsumed by diff_fix.py + parity.py)
  auto_validate.py       # becomes a thin CLI wrapper around diff_fix.reproduce
scripts/
  gen_provider.py        # unchanged (Python-side, not the Rust port)
```

Key integration points with existing code:
- `gen_handler.py` must emit the `// <<op:Name>>` marker comment (one line) in every generated fn so `diff_fix.locate()` can anchor patches.
- The Rust golden tests go in `crates/<svc>/src/tests.rs` (already exists per crate) and must construct `AwsRequest` directly (no HTTP) so `cargo test` runs serverless.
- `diff_fix.py --apply` should `git add -p`-style stage only the patched file and run `cargo test -p <crate>` before committing; on failure, revert the single file (atomic per-op).
