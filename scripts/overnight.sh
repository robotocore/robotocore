#!/bin/bash
# Overnight headless loop: expand compat test coverage one service at a time.
#
# Usage:
#   ./scripts/overnight.sh              # run all manageable services
#   ./scripts/overnight.sh --max 5      # run at most 5 iterations
#   ./scripts/overnight.sh --skip ec2   # skip specific services
#
# Prerequisites:
#   - Server running: make start
#   - claude CLI installed and authenticated
#
# Each iteration:
#   1. Picks the service with the biggest test gap (≤200 total ops)
#   2. Probes it to get working/not-implemented classification
#   3. Launches claude-code headless with probe results baked into prompt
#   4. Claude writes tests, validates quality, commits & pushes
#   5. Logs output to logs/overnight/

set -euo pipefail
cd "$(dirname "$0")/.."

MAX_ITERATIONS=999
SKIP_SERVICES=""
COMPLETED_FILE="logs/overnight/completed.txt"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max) MAX_ITERATIONS="$2"; shift 2 ;;
        --skip) SKIP_SERVICES="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

mkdir -p logs/overnight

# Ensure server is running
make status 2>/dev/null || make start

# Track completed services across iterations
touch "$COMPLETED_FILE"

for i in $(seq 1 "$MAX_ITERATIONS"); do
    # Build skip list from completed + explicit skips
    ALL_SKIPS=$(cat "$COMPLETED_FILE" | tr '\n' ' ')
    if [ -n "$SKIP_SERVICES" ]; then
        ALL_SKIPS="$ALL_SKIPS $SKIP_SERVICES"
    fi

    # Pick next service
    SKIP_ARGS=""
    if [ -n "$ALL_SKIPS" ]; then
        SKIP_ARGS="--skip $ALL_SKIPS"
    fi
    SERVICE=$(uv run python scripts/next_service.py $SKIP_ARGS 2>/dev/null) || {
        echo "=== ALL SERVICES DONE ==="
        break
    }

    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    LOGFILE="logs/overnight/${TIMESTAMP}-${SERVICE}.log"

    echo "=== [$i] Starting $SERVICE at $(date) ==="
    echo "    Log: $LOGFILE"

    # Pre-probe: get the real server status for this service
    PROBE_FILE="logs/overnight/${TIMESTAMP}-${SERVICE}-probe.json"
    echo "    Probing ${SERVICE}..."
    uv run python scripts/probe_service.py \
        --service "$SERVICE" --all --json > "$PROBE_FILE" 2>&1 || true

    # Get current coverage gaps
    COVERAGE_OUTPUT=$(uv run python scripts/compat_coverage.py \
        --service "$SERVICE" -v 2>&1) || true

    # Extract working ops from probe
    WORKING_OPS=$(python3 -c "
import json, sys
try:
    data = json.load(open('$PROBE_FILE'))
    working = [op['operation'] for op in data.get('operations', [])
               if op['status'] == 'working']
    print('\n'.join(working))
except: pass
" 2>/dev/null) || true

    NOT_IMPL=$(python3 -c "
import json, sys
try:
    data = json.load(open('$PROBE_FILE'))
    broken = [op['operation'] for op in data.get('operations', [])
              if op['status'] in ('not_implemented', '500_error')]
    print('\n'.join(broken))
except: pass
" 2>/dev/null) || true

    NEEDS_PARAMS=$(python3 -c "
import json, sys
try:
    data = json.load(open('$PROBE_FILE'))
    need = [op['operation'] for op in data.get('operations', [])
            if op['status'] == 'needs_params']
    print('\n'.join(need))
except: pass
" 2>/dev/null) || true

    # Run claude headless with probe results baked in
    claude --print -p "$(cat <<PROMPT
You are expanding compat test coverage for **${SERVICE}** in robotocore.

## Pre-computed probe results (from running server on port 4566)

These operations WORK (server responded, either 200 or a "resource not found" error proving implementation):
\`\`\`
${WORKING_OPS}
\`\`\`

These operations are NOT IMPLEMENTED (server returned 501 or crashed):
\`\`\`
${NOT_IMPL}
\`\`\`

These operations need complex params the probe couldn't auto-fill:
\`\`\`
${NEEDS_PARAMS}
\`\`\`

## Current coverage gaps

${COVERAGE_OUTPUT}

## Your task

Write compat tests ONLY for operations in the "WORK" list above that are NOT already tested (shown as missing in the coverage output). Do NOT write tests for not-implemented operations.

For the "needs complex params" operations: try to figure out the params by checking botocore shapes, but skip any that would take more than 2 minutes to figure out.

### How to write each test

1. Read the existing test file: \`tests/compatibility/test_${SERVICE}_compat.py\`
   (If the file uses hyphens differently, check: \`ls tests/compatibility/test_*${SERVICE}*\`)
2. Understand the fixtures, class structure, and import pattern
3. For each untested working operation:

   a) Many operations need a pre-existing resource. Create it in the test or use an existing fixture:
      - If the service has a "create" fixture (e.g., create_topic, create_stream), USE IT
      - If not, create the resource at the start of the test, use it, then clean up
      - Example pattern:
        \`\`\`python
        def test_get_topic_attributes(self, sns_client):
            # Setup: create the resource
            resp = sns_client.create_topic(Name="test-topic-attrs")
            topic_arn = resp["TopicArn"]
            try:
                # Test the actual operation
                result = sns_client.get_topic_attributes(TopicArn=topic_arn)
                assert "Attributes" in result
                assert result["Attributes"]["TopicArn"] == topic_arn
            finally:
                sns_client.delete_topic(TopicArn=topic_arn)
        \`\`\`

   b) For operations that take an ARN/ID of a non-existent resource, it's OK if they return
      ResourceNotFoundException — that PROVES the operation is implemented. Test like:
      \`\`\`python
      def test_describe_nonexistent(self, client):
          with pytest.raises(ClientError) as exc:
              client.describe_thing(ThingId="nonexistent")
          assert exc.value.response["Error"]["Code"] in (
              "ResourceNotFoundException", "NotFoundException"
          )
      \`\`\`
      This is a VALID test because the error comes from the SERVER, not boto3.

   c) Run each test immediately after writing it:
      \`uv run pytest tests/compatibility/test_${SERVICE}_compat.py -k "test_<name>" -q --tb=short\`

   d) If it fails, debug and fix. If the server returns 501/not-implemented, DELETE the test.

### Critical quality rules

- NEVER catch ParamValidationError — that's boto3 client-side, proves nothing
- NEVER write a test without an assertion on the response
- NEVER write a test for an operation in the "NOT IMPLEMENTED" list
- Every test must actually contact the server at localhost:4566
- If you're stuck on params for >2 minutes, skip the operation and move on

### After writing all tests

1. Validate quality:
   \`uv run python scripts/validate_test_quality.py --file tests/compatibility/test_${SERVICE}_compat.py\`
   Delete any tests with no-server-contact.

2. Run the full file:
   \`uv run pytest tests/compatibility/test_${SERVICE}_compat.py -q --tb=short\`
   All tests must pass.

3. Quick unit test check:
   \`uv run pytest tests/unit/ -q -n12 --tb=short 2>&1 | tail -5\`

4. Get final coverage:
   \`uv run python scripts/compat_coverage.py --service ${SERVICE}\`

5. Commit and push:
   \`\`\`
   git add tests/compatibility/test_${SERVICE}_compat.py
   git commit -m "Expand ${SERVICE} compat tests: X/Y operations (Z%)"
   git push
   \`\`\`

6. Print: "DONE: ${SERVICE} — X/Y operations tested (Z%), added N new tests"
PROMPT
)" > "$LOGFILE" 2>&1

    EXIT_CODE=$?

    # Mark service as completed regardless of exit code
    echo "$SERVICE" >> "$COMPLETED_FILE"

    # Extract summary from log
    SUMMARY=$(tail -5 "$LOGFILE" | grep -i "DONE:" || echo "No summary found")
    echo "    Result (exit=$EXIT_CODE): $SUMMARY"
    echo ""

    # Brief pause between services
    sleep 2
done

echo ""
echo "=== Overnight run complete ==="
echo "Completed services:"
cat "$COMPLETED_FILE"
echo ""
echo "Final coverage:"
uv run python scripts/compat_coverage.py 2>&1 | tail -5
