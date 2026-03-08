#!/bin/bash
# Overnight headless loop: expand compat test coverage chunk by chunk.
#
# Works like a fast human:
#   1. Breaks services into small resource-group chunks (3-8 ops each)
#   2. Writes + verifies ONE test at a time within each chunk
#   3. Validates quality after each chunk, commits only if green
#   4. Checks coverage delta after each commit (did it actually improve?)
#   5. Every N chunks, reflects on progress and adjusts strategy
#
# Usage:
#   ./scripts/overnight.sh                  # run all services
#   ./scripts/overnight.sh --max-chunks 50  # stop after 50 chunks
#   ./scripts/overnight.sh --service ec2    # focus on one service
#
# Prerequisites: make start (server must be running)

set -euo pipefail
cd "$(dirname "$0")/.."

MAX_CHUNKS=9999
TARGET_SERVICE=""
LOG_DIR="logs/overnight"
STATE_FILE="$LOG_DIR/state.json"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-chunks) MAX_CHUNKS="$2"; shift 2 ;;
        --service) TARGET_SERVICE="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

mkdir -p "$LOG_DIR"

# Ensure server is running
make status 2>/dev/null || make start

# Initialize state tracking
if [ ! -f "$STATE_FILE" ]; then
    echo '{"completed_chunks": [], "total_tests_added": 0, "services_done": []}' > "$STATE_FILE"
fi

CHUNK_NUM=0
CONSECUTIVE_FAILURES=0

# Get the service work queue
if [ -n "$TARGET_SERVICE" ]; then
    SERVICES="$TARGET_SERVICE"
else
    SERVICES=$(uv run python scripts/next_service.py --all --max-total 300 2>/dev/null \
        | awk '{print $1}' | head -40)
fi

for SERVICE in $SERVICES; do
    [ "$CHUNK_NUM" -ge "$MAX_CHUNKS" ] && break

    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    echo ""
    echo "================================================================"
    echo "=== SERVICE: $SERVICE at $(date) ==="
    echo "================================================================"

    # Step 1: Probe the service (real server contact)
    PROBE_FILE="$LOG_DIR/${TIMESTAMP}-${SERVICE}-probe.json"
    echo "  Probing $SERVICE..."
    uv run python scripts/probe_service.py \
        --service "$SERVICE" --all --json > "$PROBE_FILE" 2>&1 || true

    # Step 2: Get chunks with probe data
    CHUNKS_JSON=$(uv run python scripts/chunk_service.py \
        --service "$SERVICE" --untested-only --probe-file "$PROBE_FILE" --json 2>/dev/null) || {
        echo "  No chunks for $SERVICE, skipping"
        continue
    }

    # Filter to chunks that have working-untested operations
    READY_CHUNKS=$(echo "$CHUNKS_JSON" | python3 -c "
import json, sys
chunks = json.load(sys.stdin)
ready = [c for c in chunks if c.get('working_untested_count', 0) > 0]
# Sort by working_untested_count descending
ready.sort(key=lambda c: -c['working_untested_count'])
json.dump(ready, sys.stdout)
" 2>/dev/null) || continue

    NUM_READY=$(echo "$READY_CHUNKS" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
    echo "  $NUM_READY chunks with working-untested operations"

    [ "$NUM_READY" = "0" ] && continue

    # Step 3: Get before-coverage for this service
    BEFORE_COVERAGE=$(uv run python scripts/compat_coverage.py \
        --service "$SERVICE" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['covered'])" 2>/dev/null) || BEFORE_COVERAGE=0

    # Step 4: Process chunks
    CHUNK_LIST=$(echo "$READY_CHUNKS" | python3 -c "
import json, sys
chunks = json.load(sys.stdin)
for c in chunks:
    ops = ','.join(c.get('working_untested', []))
    print(f\"{c['noun']}|{ops}\")
")

    SERVICE_TESTS_ADDED=0

    while IFS='|' read -r NOUN OPS_CSV; do
        [ "$CHUNK_NUM" -ge "$MAX_CHUNKS" ] && break
        [ -z "$OPS_CSV" ] && continue
        CHUNK_NUM=$((CHUNK_NUM + 1))

        CHUNK_LOG="$LOG_DIR/${TIMESTAMP}-${SERVICE}-${NOUN}.log"
        echo ""
        echo "  --- Chunk $CHUNK_NUM: $SERVICE / $NOUN ---"
        echo "      Ops: $OPS_CSV"

        # Convert CSV to readable list
        OPS_LIST=$(echo "$OPS_CSV" | tr ',' '\n' | sed 's/^/    - /')

        # Launch claude for this one chunk
        claude --print -p "$(cat <<PROMPT
You are writing compat tests for the **${NOUN}** resource group in the **${SERVICE}** service.

## Operations to test (all confirmed working on server)

${OPS_LIST}

## Instructions

1. Read the existing test file:
   \`tests/compatibility/test_${SERVICE/_/-}_compat.py\`
   (also try: \`ls tests/compatibility/test_*${SERVICE}*\`)
   Understand the fixtures, imports, client setup, and class naming pattern.

2. For EACH operation above, write ONE test, then IMMEDIATELY run it:
   \`uv run pytest tests/compatibility/test_${SERVICE/_/-}_compat.py -k "test_<name>" -q --tb=short\`

   If it passes: keep it, move to next operation.
   If it fails with 501/not-implemented: DELETE the test entirely.
   If it fails with bad params: fix params and retry (max 2 retries, then skip).
   If it fails for unknown reason: check the error, try to fix, else skip.

3. Test patterns that work:

   **For operations needing a resource (Create/Get/Delete/Update):**
   \`\`\`python
   def test_describe_thing(self, client):
       # Setup
       resp = client.create_thing(Name="test-chunk")
       thing_id = resp["ThingId"]
       try:
           # Actual test
           result = client.describe_thing(ThingId=thing_id)
           assert result["ThingId"] == thing_id
       finally:
           client.delete_thing(ThingId=thing_id)
   \`\`\`

   **For operations on non-existent resources (proves implementation):**
   \`\`\`python
   def test_describe_nonexistent(self, client):
       with pytest.raises(ClientError) as exc:
           client.describe_thing(ThingId="nonexistent-id")
       err = exc.value.response["Error"]["Code"]
       assert err in ("ResourceNotFoundException", "NotFoundException",
                       "NoSuchEntity", "ValidationException")
   \`\`\`

   **For list operations (often work with no setup):**
   \`\`\`python
   def test_list_things(self, client):
       result = client.list_things()
       assert "Things" in result
   \`\`\`

4. After ALL operations are done:
   a) Run quality check:
      \`uv run python scripts/validate_test_quality.py --file tests/compatibility/test_${SERVICE/_/-}_compat.py\`
      If any test has no-server-contact: delete it.
   b) Run full file:
      \`uv run pytest tests/compatibility/test_${SERVICE/_/-}_compat.py -q --tb=short\`
      ALL must pass.

## Rules
- NEVER catch ParamValidationError
- NEVER write a test without an assertion
- Run each test individually RIGHT AFTER writing it
- If an operation is harder than expected, skip it — don't spend more than 2 minutes per op
- Add tests to an existing class if one matches, or create a new class named Test${SERVICE^}${NOUN}Operations

## When done
Print exactly: "CHUNK_RESULT: added=N failed=M skipped=K"
where N=tests added, M=tests that failed and were deleted, K=ops skipped.
PROMPT
)" > "$CHUNK_LOG" 2>&1

        CHUNK_EXIT=$?

        # Extract result from log
        RESULT=$(grep "CHUNK_RESULT:" "$CHUNK_LOG" 2>/dev/null | tail -1 || echo "")
        ADDED=$(echo "$RESULT" | grep -oP 'added=\K\d+' || echo "0")
        [ -z "$ADDED" ] && ADDED=0
        SERVICE_TESTS_ADDED=$((SERVICE_TESTS_ADDED + ADDED))

        echo "      Result: ${RESULT:-no result line found} (exit=$CHUNK_EXIT)"

        # Feedback: did this chunk actually work?
        if [ "$CHUNK_EXIT" -ne 0 ] || [ "$ADDED" = "0" ]; then
            CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
            echo "      WARNING: No tests added. Consecutive failures: $CONSECUTIVE_FAILURES"
            if [ "$CONSECUTIVE_FAILURES" -ge 3 ]; then
                echo "      STOPPING: 3 consecutive failures, moving to next service"
                break
            fi
        else
            CONSECUTIVE_FAILURES=0
        fi

    done <<< "$CHUNK_LIST"

    # Post-service verification
    echo ""
    echo "  === Service $SERVICE complete ==="

    if [ "$SERVICE_TESTS_ADDED" -gt 0 ]; then
        # Verify tests still pass
        echo "  Verifying all $SERVICE tests..."
        TEST_FILE=$(ls tests/compatibility/test_*${SERVICE}*_compat.py 2>/dev/null | head -1)
        if [ -n "$TEST_FILE" ]; then
            if uv run pytest "$TEST_FILE" -q --tb=short 2>&1 | tail -3; then
                # Commit
                AFTER_COVERAGE=$(uv run python scripts/compat_coverage.py \
                    --service "$SERVICE" --json 2>/dev/null \
                    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['covered'])" 2>/dev/null) || AFTER_COVERAGE=0
                TOTAL_OPS=$(uv run python scripts/compat_coverage.py \
                    --service "$SERVICE" --json 2>/dev/null \
                    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['total_ops'])" 2>/dev/null) || TOTAL_OPS="?"

                DELTA=$((AFTER_COVERAGE - BEFORE_COVERAGE))
                echo "  Coverage: $BEFORE_COVERAGE → $AFTER_COVERAGE / $TOTAL_OPS (+$DELTA ops)"

                git add tests/compatibility/ vendor/moto 2>/dev/null
                git commit -m "$(cat <<EOF
Expand ${SERVICE} compat tests: ${AFTER_COVERAGE}/${TOTAL_OPS} ops tested

Added ${SERVICE_TESTS_ADDED} tests (+${DELTA} new operations covered).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)" 2>/dev/null && git push 2>/dev/null
                echo "  Committed and pushed."
            else
                echo "  TESTS FAILED — not committing. Will fix in next iteration."
            fi
        fi
    else
        echo "  No tests added for $SERVICE."
    fi

    # Reset consecutive failure counter between services
    CONSECUTIVE_FAILURES=0

    # Periodic reflection: every 5 services, show overall progress
    SVC_COUNT=$(echo "$SERVICES" | head -n "$((CHUNK_NUM + 1))" | wc -w)
    if [ $((SVC_COUNT % 5)) -eq 0 ] && [ "$SVC_COUNT" -gt 0 ]; then
        echo ""
        echo "  ============================================"
        echo "  PROGRESS CHECK after $CHUNK_NUM chunks:"
        uv run python scripts/compat_coverage.py 2>&1 | tail -3
        echo "  ============================================"
    fi
done

echo ""
echo "================================================================"
echo "=== OVERNIGHT RUN COMPLETE ==="
echo "=== Chunks processed: $CHUNK_NUM ==="
echo "================================================================"
echo ""
echo "Final coverage:"
uv run python scripts/compat_coverage.py 2>&1 | tail -5
