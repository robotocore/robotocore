#!/bin/bash
# Overnight headless loop. Run it and go to bed.
#
#   ./scripts/overnight.sh
#
# Restarts the server after every commit (code may have changed).
# Breaks services into small resource-group chunks.
# Writes one test at a time, runs it, keeps or deletes.
# Commits per service, pushes, moves on.

set -euo pipefail
cd "$(dirname "$0")/.."

# Allow claude to be launched from within another claude session
unset CLAUDE_CODE 2>/dev/null || true
unset CLAUDECODE 2>/dev/null || true

mkdir -p logs/overnight

restart_server() {
    make stop 2>/dev/null || true
    sleep 1
    make start
    sleep 2
}

restart_server

SERVICES=$(uv run python scripts/next_service.py --all --max-total 300 2>/dev/null \
    | awk '{print $1}')

for SERVICE in $SERVICES; do
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    echo ""
    echo "================================================================"
    echo "=== $SERVICE at $(date)"
    echo "================================================================"

    # Probe against live server
    PROBE_FILE="logs/overnight/${TIMESTAMP}-${SERVICE}-probe.json"
    uv run python scripts/probe_service.py \
        --service "$SERVICE" --all --json > "$PROBE_FILE" 2>&1 || true

    # Get chunks with working-untested ops
    CHUNKS_JSON=$(uv run python scripts/chunk_service.py \
        --service "$SERVICE" --untested-only --probe-file "$PROBE_FILE" --json 2>/dev/null) || continue

    # Filter to chunks that have something to do
    CHUNK_LIST=$(echo "$CHUNKS_JSON" | python3 -c "
import json, sys
chunks = json.load(sys.stdin)
ready = [c for c in chunks if c.get('working_untested_count', 0) > 0]
ready.sort(key=lambda c: -c['working_untested_count'])
for c in ready:
    print(c['noun'] + '|' + ','.join(c['working_untested']))
" 2>/dev/null) || continue

    [ -z "$CHUNK_LIST" ] && { echo "  Nothing to do"; continue; }

    BEFORE=$(uv run python scripts/compat_coverage.py --service "$SERVICE" --json 2>/dev/null \
        | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['covered'])" 2>/dev/null) || BEFORE=0

    FAILS=0

    while IFS='|' read -r NOUN OPS_CSV; do
        [ -z "$OPS_CSV" ] && continue
        [ "$FAILS" -ge 3 ] && { echo "  3 consecutive failures, moving on"; break; }

        OPS_LIST=$(echo "$OPS_CSV" | tr ',' '\n' | sed 's/^/    - /')
        CHUNK_LOG="logs/overnight/${TIMESTAMP}-${SERVICE}-${NOUN}.log"
        echo "  chunk: $SERVICE / $NOUN"

        # Stream to chunk log and a "latest" symlink for easy tailing
        ln -sf "$(basename "$CHUNK_LOG")" logs/overnight/latest.log
        claude --output-format stream-json --verbose --permission-mode bypassPermissions -p "$(cat <<PROMPT
Write compat tests for the **${NOUN}** operations in **${SERVICE}**. These all work on the server (port 4566).

Operations to test:
${OPS_LIST}

## Steps

1. Read the test file: find it with \`ls tests/compatibility/test_*${SERVICE//-/_}*_compat.py\`
   Understand fixtures, imports, client name, class naming.

2. For EACH operation, write ONE test then IMMEDIATELY run it:
   \`uv run pytest <file> -k "test_<name>" -q --tb=short\`

   Pass → keep. 501/not-implemented → delete. Bad params → fix once, then skip.

3. Test patterns:

   CRUD (needs a resource):
   \`\`\`python
   def test_describe_thing(self, client):
       resp = client.create_thing(Name="test-chunk")
       thing_id = resp["ThingId"]
       try:
           result = client.describe_thing(ThingId=thing_id)
           assert "ThingId" in result
       finally:
           client.delete_thing(ThingId=thing_id)
   \`\`\`

   Non-existent resource (proves implementation):
   \`\`\`python
   def test_describe_nonexistent(self, client):
       with pytest.raises(ClientError) as exc:
           client.describe_thing(ThingId="does-not-exist")
       assert exc.value.response["Error"]["Code"] in (
           "ResourceNotFoundException", "NotFoundException", "NoSuchEntity")
   \`\`\`

   List (often needs no setup):
   \`\`\`python
   def test_list_things(self, client):
       result = client.list_things()
       assert "Things" in result
   \`\`\`

4. After all ops: run the full file, fix any failures.
   \`uv run pytest <file> -q --tb=short\`

5. Run quality check:
   \`uv run python scripts/validate_test_quality.py --file <file>\`
   Delete any test that doesn't contact the server.

## Rules
- NEVER catch ParamValidationError
- NEVER write a test without an assertion
- Run each test RIGHT AFTER writing it
- If stuck on params >2 min, skip the op
- If 501: delete the test, move on

Print exactly this when done: CHUNK_RESULT: added=N failed=M skipped=K
PROMPT
)" > "$CHUNK_LOG" 2>&1

        ADDED=$(grep "CHUNK_RESULT:" "$CHUNK_LOG" 2>/dev/null | sed -n 's/.*added=\([0-9]*\).*/\1/p' | tail -1)
        [ -z "$ADDED" ] && ADDED=0
        [ -z "$ADDED" ] && ADDED=0

        if [ "$ADDED" = "0" ]; then
            FAILS=$((FAILS + 1))
            echo "    nothing added (failures=$FAILS)"
        else
            FAILS=0
            echo "    +$ADDED tests"
        fi
    done <<< "$CHUNK_LIST"

    # Check if anything changed
    if git diff --quiet tests/compatibility/ 2>/dev/null; then
        echo "  No changes for $SERVICE"
        continue
    fi

    # Verify tests pass before committing
    TEST_FILE=$(ls tests/compatibility/test_*${SERVICE//-/_}*_compat.py 2>/dev/null | head -1)
    if [ -z "$TEST_FILE" ]; then
        echo "  No test file found"
        continue
    fi

    if ! uv run pytest "$TEST_FILE" -q --tb=short 2>&1 | tail -3; then
        echo "  TESTS FAILED — reverting"
        git checkout tests/compatibility/ 2>/dev/null
        continue
    fi

    # Commit, push, restart server (code may have changed)
    AFTER=$(uv run python scripts/compat_coverage.py --service "$SERVICE" --json 2>/dev/null \
        | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d[0]['covered']}/{d[0]['total_ops']}\")" 2>/dev/null) || AFTER="?"

    git add tests/compatibility/ vendor/moto
    git commit -m "$(cat <<EOF
Expand ${SERVICE} compat tests: ${AFTER} operations covered

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)" 2>/dev/null || continue
    git push 2>/dev/null || true

    echo "  Committed: $SERVICE $BEFORE → $AFTER"

    # Restart server — our code or Moto may have changed
    restart_server
done

echo ""
echo "=== Done ==="
uv run python scripts/compat_coverage.py 2>&1 | tail -5
