#!/bin/bash
# Overnight v2 — Verification-first autonomous test expansion loop.
#
#   ./scripts/overnight_v2.sh
#   ./scripts/overnight_v2.sh --resume                 # skip completed services
#   SERVICES=sqs,dynamodb ./scripts/overnight_v2.sh    # specific services only
#   MAX_HOURS=4 ./scripts/overnight_v2.sh              # wall-clock limit
#
# For each service: probe -> chunk -> per-chunk Claude session -> verify -> commit -> push
# Verification: syntax + lint + pytest on modified file. Simple and robust.

set -euo pipefail
cd "$(dirname "$0")/.."

# Allow claude to be launched from within another claude session
unset CLAUDE_CODE 2>/dev/null || true
unset CLAUDECODE 2>/dev/null || true

MAX_HOURS="${MAX_HOURS:-8}"
START_TIME=$(date +%s)
WRITE_TIMEOUT=900  # 15 min for test writing
PROGRESS_FILE="logs/overnight/progress.json"

mkdir -p logs/overnight

# ─── Helpers ──────────────────────────────────────────────────────────

log() { echo "[$(date '+%H:%M:%S')] $*"; }

wall_clock_exceeded() {
    local now elapsed
    now=$(date +%s)
    elapsed=$(( (now - START_TIME) / 3600 ))
    [ "$elapsed" -ge "$MAX_HOURS" ]
}

server_healthy() {
    curl -sf http://localhost:4566/_robotocore/health > /dev/null 2>&1
}

restart_server() {
    log "Restarting server..."
    make stop 2>/dev/null || true
    rm -f .robotocore.pid 2>/dev/null || true
    pkill -f "robotocore.main" 2>/dev/null || true
    sleep 1
    make start
    sleep 2
    if ! server_healthy; then
        sleep 3
        server_healthy || { log "ERROR: Server failed to start"; return 1; }
    fi
}

find_test_file() {
    # Find the compat test file for a service. Handles naming mismatches.
    local svc="$1"
    local snake="${svc//-/_}"
    # Exact match first
    for f in \
        "tests/compatibility/test_${snake}_compat.py" \
        "tests/compatibility/test_${snake//_/}_compat.py"; do
        [ -f "$f" ] && echo "$f" && return
    done
    # Glob fallback: pick shortest match to avoid wrong files
    ls tests/compatibility/test_*${snake}*_compat.py 2>/dev/null \
        | awk '{print length, $0}' | sort -n | head -1 | cut -d' ' -f2-
}

get_coverage() {
    local svc="$1"
    uv run python scripts/compat_coverage.py --service "$svc" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['covered'])" \
        2>/dev/null || echo "0"
}

get_total_ops() {
    local svc="$1"
    uv run python scripts/compat_coverage.py --service "$svc" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['total_ops'])" \
        2>/dev/null || echo "0"
}

count_new_tests() {
    # Return names of test functions added since HEAD
    local file="$1"
    python3 -c "
import ast, subprocess
file = '$file'
try:
    old = subprocess.run(['git', 'show', 'HEAD:' + file],
                         capture_output=True, text=True)
    if old.returncode == 0:
        old_tree = ast.parse(old.stdout)
        old_tests = {n.name for n in ast.walk(old_tree)
                     if isinstance(n, ast.FunctionDef) and n.name.startswith('test_')}
    else:
        old_tests = set()
except Exception:
    old_tests = set()
try:
    with open(file) as f:
        new_tree = ast.parse(f.read())
    new_tests = {n.name for n in ast.walk(new_tree)
                 if isinstance(n, ast.FunctionDef) and n.name.startswith('test_')}
except Exception:
    new_tests = set()
for t in sorted(new_tests - old_tests):
    print(t)
" 2>/dev/null
}

verify_and_commit() {
    # Simple verification: syntax + lint + tests pass -> commit + push
    # Returns 0 on success, 1 on failure (reverts on failure)
    local svc="$1"
    local test_file="$2"
    local before_cov="$3"

    # 1. Syntax check
    if ! python3 -c "import py_compile; py_compile.compile('$test_file', doraise=True)" 2>/dev/null; then
        log "    FAIL: syntax error in $test_file"
        return 1
    fi

    # 2. Lint fix + check
    uv run ruff check --fix --unsafe-fixes --quiet "$test_file" src/robotocore/ 2>/dev/null || true
    uv run ruff format --quiet "$test_file" src/robotocore/ 2>/dev/null || true
    if ! uv run ruff check "$test_file" src/robotocore/ --quiet 2>/dev/null; then
        log "    FAIL: lint errors persist after auto-fix"
        return 1
    fi

    # 3. All tests in the file pass (the real gate)
    if ! uv run pytest "$test_file" -q --tb=short 2>&1 | tail -5; then
        log "    FAIL: tests don't pass"
        return 1
    fi

    # 4. Quality check (warn only, don't block)
    uv run python scripts/validate_test_quality.py --file "$test_file" 2>/dev/null | tail -3 || true

    # 5. Commit + push
    local after_cov total_ops grad_msg=""
    after_cov=$(get_coverage "$svc")
    total_ops=$(get_total_ops "$svc")
    if [ "$after_cov" = "$total_ops" ] && [ "$total_ops" != "0" ]; then
        grad_msg=" [GRADUATED 100%]"
    fi

    # Stage test file + any provider changes Claude made
    git add "$test_file" src/robotocore/ 2>/dev/null || true
    # Also pick up lockfile changes if Moto was updated
    git add uv.lock 2>/dev/null || true

    # Only commit if there are staged changes
    if git diff --cached --quiet 2>/dev/null; then
        log "    No staged changes to commit"
        return 1
    fi

    local commit_msg="Expand ${svc} compat tests: ${after_cov}/${total_ops} covered${grad_msg}

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

    if ! git commit -m "$commit_msg" 2>/dev/null; then
        log "    FAIL: commit failed"
        return 1
    fi

    git push 2>/dev/null || true
    log "    COMMITTED: $svc $before_cov -> $after_cov/$total_ops$grad_msg"

    # Track progress
    local grad_flag=""
    [ -n "$grad_msg" ] && grad_flag="--graduated"
    uv run python scripts/overnight_progress.py \
        --complete-service "$svc" --after-covered "$after_cov" $grad_flag 2>/dev/null || true

    return 0
}

revert_all_changes() {
    # Revert all uncommitted changes to tests and source
    git checkout tests/compatibility/ src/robotocore/ 2>/dev/null || true
    rm -f tests/compatibility/*.bak 2>/dev/null || true
}

# ─── Pre-flight ───────────────────────────────────────────────────────

log "=== Overnight v2 starting (max ${MAX_HOURS}h) ==="

restart_server

# Quick smoke test — don't run full suite, just check server works
log "Pre-flight: smoke test..."
if ! uv run pytest tests/compatibility/test_s3_compat.py -q --tb=line -x 2>&1 | tail -3; then
    log "ABORT: S3 smoke test failed. Server broken."
    exit 1
fi
log "Pre-flight passed"

# Initialize progress tracking
if [ "${1:-}" = "--resume" ] && [ -f "$PROGRESS_FILE" ]; then
    log "Resuming from existing progress file"
    RESUME_FLAG="--resume $PROGRESS_FILE"
else
    uv run python scripts/overnight_progress.py --init
    RESUME_FLAG=""
fi

# ─── Get service queue ────────────────────────────────────────────────

if [ -n "${SERVICES:-}" ]; then
    SERVICE_QUEUE=$(echo "$SERVICES" | tr ',' '\n')
    log "Using specified services: $SERVICES"
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

# ─── Main loop ────────────────────────────────────────────────────────

for SERVICE in $SERVICE_QUEUE; do
    if wall_clock_exceeded; then
        log "Wall clock limit (${MAX_HOURS}h) reached"
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

    uv run python scripts/overnight_progress.py --start-service "$SERVICE" 2>/dev/null || true

    # ─── Probe ────────────────────────────────────────────────────
    PROBE_FILE="logs/overnight/${TIMESTAMP}-${SERVICE}-probe.json"
    timeout 60 uv run python scripts/probe_service.py \
        --service "$SERVICE" --all --json > "$PROBE_FILE" 2>&1 || true

    # ─── Chunk ────────────────────────────────────────────────────
    CHUNKS_JSON=$(uv run python scripts/chunk_service.py \
        --service "$SERVICE" --untested-only \
        --probe-file "$PROBE_FILE" --json 2>/dev/null) || {
        log "  Could not chunk $SERVICE"
        uv run python scripts/overnight_progress.py \
            --fail-service "$SERVICE" --reason "chunking failed" 2>/dev/null || true
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
        uv run python scripts/overnight_progress.py \
            --fail-service "$SERVICE" --reason "no working untested ops" 2>/dev/null || true
        continue
    fi

    # ─── Find test file ──────────────────────────────────────────
    TEST_FILE=$(find_test_file "$SERVICE")
    if [ -z "$TEST_FILE" ]; then
        log "  No test file found for $SERVICE"
        uv run python scripts/overnight_progress.py \
            --fail-service "$SERVICE" --reason "no test file" 2>/dev/null || true
        continue
    fi
    log "  Test file: $TEST_FILE"

    BEFORE_COV=$(get_coverage "$SERVICE")
    FAILS=0
    COMMITTED=false

    # ─── Per-chunk loop ──────────────────────────────────────────
    while IFS='|' read -r NOUN OPS_CSV; do
        [ -z "$OPS_CSV" ] && continue
        [ "$FAILS" -ge 3 ] && { log "  3 consecutive chunk failures, next service"; break; }

        if wall_clock_exceeded; then
            log "  Wall clock limit reached mid-service"
            break
        fi

        OPS_LIST=$(echo "$OPS_CSV" | tr ',' '\n' | sed 's/^/    - /')
        CHUNK_LOG="logs/overnight/${TIMESTAMP}-${SERVICE}-${NOUN}.log"
        log "  chunk: $SERVICE / $NOUN ($(echo "$OPS_CSV" | tr ',' '\n' | wc -l | tr -d ' ') ops)"
        ln -sf "$(basename "$CHUNK_LOG")" logs/overnight/latest.log

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

        # ─── Check what Claude produced ───────────────────────
        NEW_TESTS=$(count_new_tests "$TEST_FILE")
        NEW_TEST_COUNT=$(echo "$NEW_TESTS" | grep -c . 2>/dev/null || echo "0")

        if [ "$NEW_TEST_COUNT" -eq 0 ]; then
            log "    No new tests added"
            # Revert any partial changes Claude left behind
            revert_all_changes
            FAILS=$((FAILS + 1))
            continue
        fi

        log "    $NEW_TEST_COUNT new tests detected"

        # ─── VERIFY + COMMIT ──────────────────────────────────
        if verify_and_commit "$SERVICE" "$TEST_FILE" "$BEFORE_COV"; then
            FAILS=0
            COMMITTED=true
            BEFORE_COV=$(get_coverage "$SERVICE")
            # Restart server after source changes
            if git diff HEAD~1 --name-only | grep -q "^src/"; then
                restart_server
            fi
        else
            log "    Verification failed, reverting chunk"
            revert_all_changes
            FAILS=$((FAILS + 1))
        fi

    done <<< "$CHUNK_LIST"

    # ─── Service summary ──────────────────────────────────────────
    if ! $COMMITTED; then
        uv run python scripts/overnight_progress.py \
            --fail-service "$SERVICE" --reason "no verified chunks" 2>/dev/null || true
    fi

    SERVICES_DONE=$((SERVICES_DONE + 1))

    # Progress report every 5 services
    if [ $((SERVICES_DONE % 5)) -eq 0 ]; then
        log ""
        log "=== Progress report ($SERVICES_DONE services) ==="
        uv run python scripts/overnight_progress.py --report 2>/dev/null || true
    fi

    # Restart server between services (code may have changed)
    restart_server

done

# ─── Final report ─────────────────────────────────────────────────────

log ""
log "=== Overnight v2 complete: $SERVICES_DONE services processed ==="
uv run python scripts/overnight_progress.py --report 2>/dev/null || true
echo ""
log "Coverage summary:"
uv run python scripts/compat_coverage.py 2>&1 | tail -5
