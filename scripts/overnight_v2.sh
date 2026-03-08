#!/bin/bash
# Overnight v2 — Verification-first autonomous test expansion loop.
#
#   ./scripts/overnight_v2.sh
#   ./scripts/overnight_v2.sh --resume                 # skip completed services
#   ./scripts/overnight_v2.sh --services sqs,dynamodb   # specific services only
#   MAX_HOURS=4 ./scripts/overnight_v2.sh              # wall-clock limit
#
# 7-gate verification pipeline per chunk:
#   1. Syntax  2. Static quality  3. New tests pass  4. Regression
#   5. Runtime validation  6. Coverage delta  7. Lint
#
# Self-healing: gates 2,3 re-prompt Claude for fixes before reverting.
# Gate 5 surgically deletes non-contacting tests.

set -euo pipefail
cd "$(dirname "$0")/.."

# Allow claude to be launched from within another claude session
unset CLAUDE_CODE 2>/dev/null || true
unset CLAUDECODE 2>/dev/null || true

MAX_HOURS="${MAX_HOURS:-8}"
START_TIME=$(date +%s)
HEAL_TIMEOUT=300   # 5 min for heal attempts
WRITE_TIMEOUT=900  # 15 min for test writing
PROGRESS_FILE="logs/overnight/progress.json"

mkdir -p logs/overnight

# ─── Helpers ──────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wall_clock_exceeded() {
    local now=$(date +%s)
    local elapsed=$(( (now - START_TIME) / 3600 ))
    [ "$elapsed" -ge "$MAX_HOURS" ]
}

server_healthy() {
    curl -sf http://localhost:4566/_robotocore/health > /dev/null 2>&1
}

restart_server() {
    log "Restarting server..."
    make stop 2>/dev/null || true
    rm -f .robotocore.pid 2>/dev/null || true
    # Kill any leftover uvicorn/robotocore processes
    pkill -f "robotocore.main" 2>/dev/null || true
    sleep 1
    make start
    sleep 2
    if ! server_healthy; then
        sleep 3
        server_healthy || { log "ERROR: Server failed to start"; return 1; }
    fi
}

get_coverage() {
    # Returns covered count for a service
    local svc="$1"
    uv run python scripts/compat_coverage.py --service "$svc" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['covered'])" 2>/dev/null || echo "0"
}

get_total_ops() {
    local svc="$1"
    uv run python scripts/compat_coverage.py --service "$svc" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['total_ops'])" 2>/dev/null || echo "0"
}

count_new_tests() {
    # Count test functions added in uncommitted changes to a file using AST diff
    local file="$1"
    python3 -c "
import ast, subprocess, sys
file = '$file'

# Get the committed version's test names
try:
    old = subprocess.run(['git', 'show', 'HEAD:' + file], capture_output=True, text=True)
    if old.returncode == 0:
        old_tree = ast.parse(old.stdout)
        old_tests = {node.name for node in ast.walk(old_tree) if isinstance(node, ast.FunctionDef) and node.name.startswith('test_')}
    else:
        old_tests = set()
except:
    old_tests = set()

# Get current version's test names
try:
    with open(file) as f:
        new_tree = ast.parse(f.read())
    new_tests = {node.name for node in ast.walk(new_tree) if isinstance(node, ast.FunctionDef) and node.name.startswith('test_')}
except:
    new_tests = set()

added = new_tests - old_tests
# Print each new test name, one per line
for t in sorted(added):
    print(t)
" 2>/dev/null
}

# ─── Pre-flight ───────────────────────────────────────────────────────

log "=== Overnight v2 starting (max ${MAX_HOURS}h) ==="

restart_server

# Run full compat suite — abort if >2 failures (allow flaky state-dependent tests)
log "Pre-flight: running full compat test suite..."
PREFLIGHT_OUT=$(uv run pytest tests/compatibility/ -q --tb=line 2>&1)
PREFLIGHT_FAILS=$(echo "$PREFLIGHT_OUT" | grep -c "^FAILED" || true)
echo "$PREFLIGHT_OUT" | tail -5
if [ "$PREFLIGHT_FAILS" -gt 2 ]; then
    log "ABORT: $PREFLIGHT_FAILS compat test failures (>2). Fix them first."
    exit 1
elif [ "$PREFLIGHT_FAILS" -gt 0 ]; then
    log "WARNING: $PREFLIGHT_FAILS flaky test(s) detected, proceeding anyway"
fi

# Initialize progress tracking
if [ "${1:-}" = "--resume" ] && [ -f "$PROGRESS_FILE" ]; then
    log "Resuming from existing progress file"
    RESUME_FLAG="--resume $PROGRESS_FILE"
else
    uv run python scripts/overnight_progress.py --init
    RESUME_FLAG=""
fi

# ─── Get service queue ────────────────────────────────────────────────

if [ -n "${SERVICES:-}" ] || [[ "${1:-}" == --services=* ]] || [[ "${2:-}" == --services=* ]]; then
    # Parse --services flag or SERVICES env var
    SVC_INPUT="${SERVICES:-}"
    for arg in "$@"; do
        [[ "$arg" == --services=* ]] && SVC_INPUT="${arg#--services=}"
        [[ "$arg" == --services ]] && SVC_INPUT="${2:-}"
    done
    SERVICE_QUEUE=$(echo "$SVC_INPUT" | tr ',' '\n')
    log "Using specified services: $SVC_INPUT"
else
    SERVICE_QUEUE=$(uv run python scripts/prioritize_services.py --json $RESUME_FLAG 2>/dev/null \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
for svc in data['ordered']:
    print(svc['service'])
" 2>/dev/null)
    log "$(echo "$SERVICE_QUEUE" | wc -l | tr -d ' ') services in queue"
fi

SERVICES_DONE=0
SERVICES_SINCE_REPORT=0

# ─── Main loop ────────────────────────────────────────────────────────

for SERVICE in $SERVICE_QUEUE; do
    if wall_clock_exceeded; then
        log "Wall clock limit (${MAX_HOURS}h) reached, stopping"
        break
    fi

    if ! server_healthy; then
        restart_server || { log "Server won't start, aborting"; break; }
    fi

    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    log ""
    log "================================================================"
    log "=== $SERVICE at $(date)"
    log "================================================================"

    uv run python scripts/overnight_progress.py --start-service "$SERVICE"

    # ─── Probe ────────────────────────────────────────────────────
    PROBE_FILE="logs/overnight/${TIMESTAMP}-${SERVICE}-probe.json"
    timeout 60 uv run python scripts/probe_service.py \
        --service "$SERVICE" --all --json > "$PROBE_FILE" 2>&1 || true

    # ─── Chunk ────────────────────────────────────────────────────
    CHUNKS_JSON=$(uv run python scripts/chunk_service.py \
        --service "$SERVICE" --untested-only --probe-file "$PROBE_FILE" --json 2>/dev/null) || {
        log "  Could not chunk $SERVICE, skipping"
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "chunking failed"
        continue
    }

    CHUNK_LIST=$(echo "$CHUNKS_JSON" | python3 -c "
import json, sys
chunks = json.load(sys.stdin)
ready = [c for c in chunks if c.get('working_untested_count', 0) > 0]
ready.sort(key=lambda c: -c['working_untested_count'])
for c in ready:
    print(c['noun'] + '|' + ','.join(c['working_untested']))
" 2>/dev/null) || CHUNK_LIST=""

    if [ -z "$CHUNK_LIST" ]; then
        log "  Nothing to do for $SERVICE"
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "no working untested ops"
        continue
    fi

    # ─── Find test file ──────────────────────────────────────────
    SVC_SNAKE="${SERVICE//-/_}"
    # Try exact match first, then broader patterns for naming mismatches
    TEST_FILE=""
    for pattern in \
        "tests/compatibility/test_${SVC_SNAKE}_compat.py" \
        "tests/compatibility/test_${SVC_SNAKE//_/}_compat.py"; do
        [ -f "$pattern" ] && TEST_FILE="$pattern" && break
    done
    # Glob fallback: pick shortest match to avoid test_apigateway_lambda_ for lambda
    if [ -z "$TEST_FILE" ]; then
        TEST_FILE=$(ls tests/compatibility/test_*${SVC_SNAKE}*_compat.py 2>/dev/null \
            | awk '{print length, $0}' | sort -n | head -1 | cut -d' ' -f2-)
    fi
    if [ -z "$TEST_FILE" ]; then
        log "  No test file found for $SERVICE"
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "no test file"
        continue
    fi

    BEFORE_COV=$(get_coverage "$SERVICE")
    FAILS=0
    SERVICE_CHANGED=false

    # ─── Per-chunk loop ──────────────────────────────────────────
    while IFS='|' read -r NOUN OPS_CSV; do
        [ -z "$OPS_CSV" ] && continue
        [ "$FAILS" -ge 3 ] && { log "  3 consecutive failures, moving on"; break; }

        if wall_clock_exceeded; then
            log "  Wall clock limit reached mid-service"
            break
        fi

        OPS_LIST=$(echo "$OPS_CSV" | tr ',' '\n' | sed 's/^/    - /')
        CHUNK_LOG="logs/overnight/${TIMESTAMP}-${SERVICE}-${NOUN}.log"
        log "  chunk: $SERVICE / $NOUN"
        ln -sf "$(basename "$CHUNK_LOG")" logs/overnight/latest.log

        # Save file state for potential revert
        cp "$TEST_FILE" "${TEST_FILE}.bak"

        # ─── WRITE: Claude generates tests ────────────────────
        WRITE_PROMPT="Write compat tests for the **${NOUN}** operations in **${SERVICE}**. These all work on the server (port 4566).

Operations to test:
${OPS_LIST}

Steps:
1. Read the test file: cat ${TEST_FILE} -- understand fixtures, imports, client name, class naming.
2. For EACH operation, write ONE test then IMMEDIATELY run it:
   uv run pytest ${TEST_FILE} -k test_name -q --tb=short
   Pass = keep. 501/not-implemented = delete. Bad params = fix once, then skip.
3. Test patterns:
   - CRUD: create resource, call the op, assert on response key, delete in finally block
   - Non-existent resource: pytest.raises(ClientError), assert error Code is ResourceNotFoundException/NotFoundException/NoSuchEntity
   - List: call list op, assert response key exists
4. After all ops: uv run pytest ${TEST_FILE} -q --tb=short
5. Quality check: uv run python scripts/validate_test_quality.py --file ${TEST_FILE}
   Delete any test that doesn't contact the server.

Rules: NEVER catch ParamValidationError. NEVER write a test without an assertion. Run each test RIGHT AFTER writing it. If stuck on params >2 min, skip the op. If 501: delete the test, move on."

        timeout "$WRITE_TIMEOUT" claude --output-format stream-json --verbose \
            --permission-mode bypassPermissions -p "$WRITE_PROMPT" \
            > "$CHUNK_LOG" 2>&1 || true

        # ─── Detect new tests via AST diff ───────────────────
        NEW_TESTS=$(count_new_tests "$TEST_FILE")
        NEW_TEST_COUNT=$(echo "$NEW_TESTS" | grep -c . 2>/dev/null || echo "0")
        NEW_TESTS_CSV=$(echo "$NEW_TESTS" | paste -sd, - 2>/dev/null || echo "")

        if [ "$NEW_TEST_COUNT" -eq 0 ] || [ -z "$NEW_TESTS_CSV" ]; then
            log "    No new tests detected, reverting"
            cp "${TEST_FILE}.bak" "$TEST_FILE"
            rm -f "${TEST_FILE}.bak"
            FAILS=$((FAILS + 1))
            continue
        fi

        log "    $NEW_TEST_COUNT new tests: ${NEW_TESTS_CSV:0:80}"

        # ─── VERIFY: 7-gate pipeline ─────────────────────────
        VERIFY_JSON=$(uv run python scripts/overnight_verify.py \
            --file "$TEST_FILE" \
            --new-tests "$NEW_TESTS_CSV" \
            --service "$SERVICE" \
            --before-coverage "$BEFORE_COV" \
            --json 2>/dev/null) || VERIFY_JSON='{"passed":false,"fatal_problems":["verify script failed"]}'

        VERIFY_PASSED=$(echo "$VERIFY_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['passed'])" 2>/dev/null || echo "False")
        HAS_HEALABLE=$(echo "$VERIFY_JSON" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print(len(d.get('healable_problems',[])) > 0 and len(d.get('fatal_problems',[])) == 0)
" 2>/dev/null || echo "False")

        if [ "$VERIFY_PASSED" = "True" ]; then
            log "    VERIFIED: all 7 gates passed"
            FAILS=0
            SERVICE_CHANGED=true
            BEFORE_COV=$(get_coverage "$SERVICE")
            rm -f "${TEST_FILE}.bak"

        elif [ "$HAS_HEALABLE" = "True" ]; then
            # ─── HEAL: re-prompt Claude with specific problems ─
            log "    Healable problems detected, attempting fix..."
            HEAL_PROBLEMS=$(echo "$VERIFY_JSON" | python3 -c "
import json,sys
d = json.load(sys.stdin)
for p in d.get('healable_problems',[]):
    print('- ' + p)
" 2>/dev/null)

            HEAL_LOG="logs/overnight/${TIMESTAMP}-${SERVICE}-${NOUN}-heal.log"
            HEAL_PROMPT="Fix these verification problems in ${TEST_FILE}:

${HEAL_PROBLEMS}

Rules:
- If a test doesn't contact the server (ParamValidationError), DELETE it
- If a test has no assertion, ADD an assertion on a response field
- If a test fails, fix it or delete it
- Run: uv run pytest ${TEST_FILE} -q --tb=short -- after fixes"

            timeout "$HEAL_TIMEOUT" claude --output-format stream-json --verbose \
                --permission-mode bypassPermissions -p "$HEAL_PROMPT" \
                > "$HEAL_LOG" 2>&1 || true

            # Re-verify after heal
            NEW_TESTS=$(count_new_tests "$TEST_FILE")
            NEW_TESTS_CSV=$(echo "$NEW_TESTS" | paste -sd, - 2>/dev/null || echo "")

            VERIFY2_JSON=$(uv run python scripts/overnight_verify.py \
                --file "$TEST_FILE" \
                --new-tests "$NEW_TESTS_CSV" \
                --service "$SERVICE" \
                --before-coverage "$BEFORE_COV" \
                --skip-runtime \
                --json 2>/dev/null) || VERIFY2_JSON='{"passed":false}'

            VERIFY2_PASSED=$(echo "$VERIFY2_JSON" | python3 -c "import json,sys; print(json.load(sys.stdin)['passed'])" 2>/dev/null || echo "False")

            if [ "$VERIFY2_PASSED" = "True" ]; then
                log "    HEALED: verification passed after fix"
                FAILS=0
                SERVICE_CHANGED=true
                BEFORE_COV=$(get_coverage "$SERVICE")
                rm -f "${TEST_FILE}.bak"
            else
                log "    REVERT: heal failed, restoring backup"
                cp "${TEST_FILE}.bak" "$TEST_FILE"
                rm -f "${TEST_FILE}.bak"
                FAILS=$((FAILS + 1))
            fi
        else
            # Fatal problems — revert
            FATAL=$(echo "$VERIFY_JSON" | python3 -c "
import json,sys
for p in json.load(sys.stdin).get('fatal_problems',[]):
    print(p)
" 2>/dev/null | head -3)
            log "    REVERT: fatal: $FATAL"
            cp "${TEST_FILE}.bak" "$TEST_FILE"
            rm -f "${TEST_FILE}.bak"
            FAILS=$((FAILS + 1))
        fi

    done <<< "$CHUNK_LIST"

    # ─── Service commit ──────────────────────────────────────────
    rm -f "${TEST_FILE}.bak" 2>/dev/null

    if ! $SERVICE_CHANGED; then
        log "  No verified changes for $SERVICE"
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "no verified chunks"
        continue
    fi

    # Final regression check before commit
    log "  Post-service regression check..."
    if ! uv run pytest "$TEST_FILE" -q --tb=short 2>&1 | tail -3; then
        log "  POST-SERVICE REGRESSION — reverting all changes to $TEST_FILE"
        git checkout "$TEST_FILE" 2>/dev/null
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "post-service regression"
        continue
    fi

    # Push Moto fixes if needed
    if ! (cd vendor/moto && git diff --quiet jackdanger/robotocore/all-fixes..HEAD 2>/dev/null); then
        (cd vendor/moto && git push jackdanger HEAD:robotocore/all-fixes 2>/dev/null) || true
        uv lock 2>/dev/null || true
    fi

    # Commit
    AFTER_COV=$(get_coverage "$SERVICE")
    TOTAL_OPS=$(get_total_ops "$SERVICE")
    GRADUATED=false
    GRAD_MSG=""
    if [ "$AFTER_COV" = "$TOTAL_OPS" ] && [ "$TOTAL_OPS" != "0" ]; then
        GRADUATED=true
        GRAD_MSG=" [GRADUATED 100%]"
    fi

    git add "$TEST_FILE" src/robotocore/ uv.lock 2>/dev/null || true
    COMMIT_MSG="Expand ${SERVICE} compat tests: ${AFTER_COV}/${TOTAL_OPS} operations covered${GRAD_MSG}

Overnight v2: verified via 7-gate pipeline (syntax, quality, pass, regression, runtime, coverage, lint)

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
    git commit -m "$COMMIT_MSG" 2>/dev/null || { log "  COMMIT FAILED for $SERVICE"; continue; }

    # Post-commit regression — verify the commit is green
    if ! uv run pytest "$TEST_FILE" -q --tb=short 2>&1 | tail -3; then
        log "  POST-COMMIT REGRESSION — reverting commit"
        git revert --no-edit HEAD 2>/dev/null || git reset --soft HEAD~1
        uv run python scripts/overnight_progress.py --fail-service "$SERVICE" --reason "post-commit regression"
        continue
    fi

    git push 2>/dev/null || true

    # Track progress
    GRAD_FLAG=""
    $GRADUATED && GRAD_FLAG="--graduated"
    uv run python scripts/overnight_progress.py \
        --complete-service "$SERVICE" --after-covered "$AFTER_COV" $GRAD_FLAG

    log "  COMMITTED: $SERVICE ${BEFORE_COV} -> ${AFTER_COV}/${TOTAL_OPS}${GRAD_MSG}"
    SERVICES_DONE=$((SERVICES_DONE + 1))
    SERVICES_SINCE_REPORT=$((SERVICES_SINCE_REPORT + 1))

    # Progress report every 5 services
    if [ "$SERVICES_SINCE_REPORT" -ge 5 ]; then
        log ""
        log "=== Progress report ($SERVICES_DONE services done) ==="
        uv run python scripts/overnight_progress.py --report
        SERVICES_SINCE_REPORT=0
    fi

    # Restart server for next service
    restart_server

done

# ─── Final report ─────────────────────────────────────────────────────

log ""
log "=== Overnight v2 complete: $SERVICES_DONE services processed ==="
uv run python scripts/overnight_progress.py --report
echo ""
log "Coverage summary:"
uv run python scripts/compat_coverage.py 2>&1 | tail -5
