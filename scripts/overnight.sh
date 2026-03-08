#!/bin/bash
# Overnight headless loop: expand compat test coverage one service at a time.
#
# Usage:
#   ./scripts/overnight.sh              # run until all services done
#   ./scripts/overnight.sh --max 5      # run at most 5 iterations
#   ./scripts/overnight.sh --skip ec2   # skip specific services
#
# Prerequisites:
#   - Server running: make start
#   - claude CLI installed and authenticated
#
# Each iteration:
#   1. Picks the service with the biggest test gap
#   2. Launches claude-code headless with a detailed prompt
#   3. Claude probes, writes tests, fixes Moto if needed, commits & pushes
#   4. Logs output to logs/overnight/
#   5. Moves to next service

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

    # Run claude headless
    claude --print -p "$(cat <<PROMPT
You are expanding compat test coverage for the **${SERVICE}** service in robotocore. Follow the headless overnight workflow from CLAUDE.md exactly. Here is your step-by-step plan:

## Step 1: Verify server is running
Run: \`make status\`
If not running, run: \`make start\`

## Step 2: Probe the service
Run: \`uv run python scripts/probe_service.py --service ${SERVICE} --all --json\`
This gives you the allowlist. Save the output mentally — you'll need it.

## Step 3: Check current coverage
Run: \`uv run python scripts/compat_coverage.py --service ${SERVICE} -v\`
This tells you which operations are already tested and which are gaps.

## Step 4: Identify the work
Cross-reference Steps 2 and 3. The work items are operations that:
- Probe shows as "working" or "needs_params" (server can handle them)
- compat_coverage shows as untested (no test exists)
Skip operations where probe shows "not_implemented" or "500_error" unless it's an easy Moto fix.

## Step 5: Read the existing test file
Read: \`tests/compatibility/test_${SERVICE}_compat.py\`
Understand: fixtures, imports, class structure, naming conventions, endpoint URL.

## Step 6: Write tests for each gap operation
For EACH untested-but-working operation:
a) Check botocore for required parameters: \`python3 -c "import botocore.session; s=botocore.session.get_session(); m=s.get_service_model('${SERVICE}'); op=m.operation_model('<OpName>'); print([(k,v) for k,v in op.input_shape.members.items()])"\`
b) Write a test with valid parameters that contacts the server
c) Every test MUST have at least one assert on a response field (HTTPStatusCode, or a domain field)
d) Run JUST that test immediately: \`uv run pytest tests/compatibility/test_${SERVICE}_compat.py -k "test_<name>" -q --tb=short\`
e) If it fails because the operation isn't implemented server-side, DELETE the test and move on
f) If it fails because of bad params, fix the params and retry

CRITICAL RULES:
- NEVER catch ParamValidationError and call it a passing test
- NEVER write a test without an assertion
- If you can't figure out valid params in ~2 minutes, skip the operation
- Run each test individually as you write it — don't batch

## Step 7: Handle 500-error operations (OPTIONAL, max 1 per service)
If probe showed a 500_error for an important operation, check the Moto source:
- \`vendor/moto/moto/${SERVICE}/responses.py\` and \`models.py\`
- If it's a simple missing handler (like the S3 metadata tables fix), add a stub
- Restart server: \`make stop && make start\`
- Write a test for the fixed operation
- If the fix is complex, skip it entirely

## Step 8: Validate quality
Run: \`uv run python scripts/validate_test_quality.py --file tests/compatibility/test_${SERVICE}_compat.py\`
If no-contact rate > 5%, find and fix or delete the bad tests.

## Step 9: Run full test suite
Run: \`uv run pytest tests/compatibility/test_${SERVICE}_compat.py -q --tb=short\`
ALL tests must pass. Fix any regressions.

## Step 10: Run unit tests (sanity check)
Run: \`uv run pytest tests/unit/ -q -n12 --tb=short 2>&1 | tail -5\`
Must still pass.

## Step 11: Commit and push
Run: \`uv run python scripts/compat_coverage.py --service ${SERVICE}\` to get final stats.
Stage and commit:
\`\`\`
git add tests/compatibility/test_${SERVICE}_compat.py vendor/moto
git commit -m "Expand ${SERVICE} compat tests: X/Y operations (Z%)"
git push
\`\`\`
Include actual numbers in the commit message.

## Step 12: Check CI
Run: \`gh run list --limit 1\`
If the latest run failed, investigate briefly. If it's your fault, fix and push again.

## Final output
Print a summary line: "DONE: ${SERVICE} — X/Y operations tested (Z%), added N new tests"
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
