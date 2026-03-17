# Moto Operations Pilot — Progress Tracker

Run this to see where things stand at any point:
```bash
make start
uv run pytest tests/moto_impl/test_connect_lifecycle.py tests/moto_impl/test_iot_lifecycle.py tests/moto_impl/test_glue_lifecycle.py -q --tb=no
```

## Baseline (2026-03-17)
- Connect: 48 passed, 10 failed
- IoT:     54 passed, 12 failed
- Glue:    52 passed, 26 failed
- **Total: 154 passed, 48 failed**

## Final (2026-03-17)
- Connect: **58/58 passed** ✓
- IoT:     **66/66 passed** ✓
- Glue:    **78/78 passed** ✓
- **Total: 202/202 passed ✓ GOAL ACHIEVED**

## Goal
~~All 202 tests passing. Then run the same tool on more services.~~
**COMPLETE.** All 202 tests pass.

## Moto Workflow
```bash
cd vendor/moto
git checkout -b fix/<service>-<noun>   # or add to existing fix branch
# edit moto/<service>/models.py + responses.py
git add -A && git commit -m "feat(<service>): implement <noun> ops"
git push jackdanger master
cd ../..
uv lock  # pin new commit
```
