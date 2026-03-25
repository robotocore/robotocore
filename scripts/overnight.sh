#!/bin/bash
# Overnight headless loop. Run it and go to bed.
#
#   ./scripts/overnight.sh [--category test|strengthen_test|implement] [--service NAME]
#
# Uses drive.py (which reads data/operation_catalog.json) to select work.
# The catalog IS the state — running this script again picks up exactly where it left off.
#
# Work priority: fix_test → test → strengthen_test → implement
# Restarts the server after every commit (code may have changed).
# Commits and pushes per service. Moves on after 3 consecutive zero-result chunks.

set -euo pipefail
cd "$(dirname "$0")/.."

# Allow claude to be launched from within another claude session
unset CLAUDE_CODE 2>/dev/null || true
unset CLAUDECODE 2>/dev/null || true

mkdir -p logs/overnight

# Parse args to pass through to drive.py
DRIVE_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --category|--service|--batch)
            DRIVE_ARGS="$DRIVE_ARGS $1 $2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

restart_server() {
    make stop 2>/dev/null || true
    sleep 1
    make start
    sleep 2
}

restart_server

# Get work queue from catalog-aware driver
# Each line: JSON object with {category, service, ops, chunk_idx, total_chunks, prompt}
WORK_JSON=$(uv run python scripts/drive.py --json $DRIVE_ARGS 2>/dev/null) || {
    echo "Nothing to do — catalog shows no remaining work."
    exit 0
}

ITEM_COUNT=$(echo "$WORK_JSON" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))")
echo ""
echo "Work queue: $ITEM_COUNT items"
echo "$WORK_JSON" | python3 -c "
import json, sys
items = json.load(sys.stdin)
from collections import Counter
cats = Counter(i['category'] for i in items)
for cat, n in cats.most_common():
    print(f'  {cat}: {n} items')
"
echo ""

# Process each work item
echo "$WORK_JSON" | python3 -c "
import json, sys
items = json.load(sys.stdin)
for item in items:
    print(item['category'] + '|' + item['service'] + '|' + str(item['chunk_idx']) + '|' + str(item['total_chunks']) + '|' + item['prompt'].replace('\n', '\\\\n'))
" | while IFS='|' read -r CAT SERVICE CHUNK_IDX TOTAL_CHUNKS PROMPT_ESCAPED; do
    PROMPT=$(echo "$PROMPT_ESCAPED" | sed 's/\\n/\n/g')
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    CHUNK_LABEL=""
    if [ "$TOTAL_CHUNKS" -gt 1 ]; then
        CHUNK_LABEL="-chunk${CHUNK_IDX}"
    fi

    echo "================================================================"
    echo "=== $CAT / $SERVICE$CHUNK_LABEL at $(date)"
    echo "================================================================"

    CHUNK_LOG="logs/overnight/${TIMESTAMP}-${CAT}-${SERVICE}${CHUNK_LABEL}.log"
    ln -sf "$(basename "$CHUNK_LOG")" logs/overnight/latest.log

    # Find the service's test file (for commit and lint steps below)
    TEST_FILE=$(ls tests/compatibility/test_*${SERVICE//-/_}*_compat.py 2>/dev/null | head -1 || true)

    BEFORE=0
    if [ -n "$TEST_FILE" ]; then
        BEFORE=$(uv run python scripts/compat_coverage.py --service "$SERVICE" --json 2>/dev/null \
            | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['covered'])" 2>/dev/null) || BEFORE=0
    fi

    # Run the claude session
    claude --output-format stream-json --verbose --permission-mode bypassPermissions \
        -p "$PROMPT" > "$CHUNK_LOG" 2>&1 || true

    ADDED=$(grep "CHUNK_RESULT:" "$CHUNK_LOG" 2>/dev/null \
        | sed -n 's/.*added=\([0-9]*\).*/\1/p' | tail -1 || echo "0")
    [ -z "$ADDED" ] && ADDED=0
    echo "  Result: $ADDED items added"

    # Check if anything changed
    if git diff --quiet tests/compatibility/ src/robotocore/ 2>/dev/null; then
        echo "  No changes — skipping commit"
        continue
    fi

    # Re-find test file (may have been created in this session)
    TEST_FILE=$(ls tests/compatibility/test_*${SERVICE//-/_}*_compat.py 2>/dev/null | head -1 || true)

    # Verify tests pass before committing (only for test-writing categories)
    if [[ "$CAT" == "test" || "$CAT" == "strengthen_test" || "$CAT" == "fix_test" ]] && [ -n "$TEST_FILE" ]; then
        if ! uv run pytest "$TEST_FILE" -q --tb=short 2>&1 | tail -3; then
            echo "  TESTS FAILED — reverting $TEST_FILE only"
            git checkout "$TEST_FILE" 2>/dev/null || true
            continue
        fi
    fi

    # Fix lint/format before committing (pre-commit hook requires this)
    uv run ruff check --fix --unsafe-fixes --quiet tests/compatibility/ src/robotocore/ 2>/dev/null || true
    uv run ruff format --quiet tests/compatibility/ src/robotocore/ 2>/dev/null || true

    # Verify lint passes; if not, revert test file only
    if [ -n "$TEST_FILE" ] && ! uv run ruff check "$TEST_FILE" --quiet 2>/dev/null; then
        echo "  LINT FAILED after fixes — reverting $TEST_FILE"
        git checkout "$TEST_FILE" 2>/dev/null || true
        continue
    fi

    # Compute coverage delta (for commit message)
    AFTER="?"
    if [ -n "$TEST_FILE" ]; then
        AFTER=$(uv run python scripts/compat_coverage.py --service "$SERVICE" --json 2>/dev/null \
            | python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"{d[0]['covered']}/{d[0]['total_ops']}\")" \
            2>/dev/null) || AFTER="?"
    fi

    # Push any Moto fixes to the fork and update the lockfile
    (cd vendor/moto && git push jackdanger HEAD:master 2>/dev/null) || true
    uv lock 2>/dev/null || true

    # Create prompt log
    PROMPT_FILE="prompts/$(date -u +%Y%m%d-%H%M%S)-${CAT}-${SERVICE}${CHUNK_LABEL}.md"
    cat > "$PROMPT_FILE" <<PLOG
---
session: "overnight-$(date -u +%Y%m%d)"
timestamp: "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
model: claude-opus-4-6
reconstructed: true
---

## Human

Overnight automation: ${CAT} work for ${SERVICE}${CHUNK_LABEL}.

## Assistant

## Key decisions

Category: ${CAT}. Coverage: ${BEFORE} → ${AFTER}.
PLOG

    # Stage and commit
    FILES_TO_ADD="$PROMPT_FILE"
    [ -n "$TEST_FILE" ] && FILES_TO_ADD="$FILES_TO_ADD $TEST_FILE"

    git add $FILES_TO_ADD src/robotocore/ uv.lock 2>/dev/null || true
    git commit -m "$(cat <<EOF
${CAT}: ${SERVICE}${CHUNK_LABEL} — coverage ${AFTER}

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>
EOF
)" 2>/dev/null || { echo "  COMMIT FAILED for $SERVICE$CHUNK_LABEL"; continue; }
    git push 2>/dev/null || true

    echo "  Committed: $SERVICE$CHUNK_LABEL ($CAT) $BEFORE → $AFTER"

    # Rebuild catalog so subsequent items see updated coverage
    uv run python scripts/build_operation_catalog.py --json > data/operation_catalog.json 2>/dev/null || true

    # Restart server — code or Moto may have changed
    restart_server
done

echo ""
echo "=== Done ==="
uv run python scripts/drive.py --summary 2>/dev/null || uv run python scripts/compat_coverage.py 2>&1 | tail -5
