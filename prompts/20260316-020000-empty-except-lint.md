---
role: human+assistant
timestamp: "2026-03-16T02:00:00-07:00"
session: ci-optimization
sequence: 2
---

## Human prompt

There are some github-code-quality comments that keep happening around 'except'
blocks. Check them and make some kind of sophisticated (but deterministic,
non-llm-using) lint that can fix this before future commits. Implement that, run
it, commit the fixes it makes, and then get the CI passing on this PR #168.

Perhaps we use this linter as an opportunity to always get error details out of
the inner workings of robotocore in CI? That might help us avoid overconfidence.

## Work done

Created `scripts/fix_empty_except.py` — a deterministic AST-based linter that:

1. Finds all `except ...: pass` blocks without comments or logging
2. Classifies each by context:
   - Cleanup verbs in the try body → "best-effort cleanup"
   - Fixture/generator context → "best-effort cleanup"
   - Specific exception types (ClientError, FileNotFoundError, etc.) → targeted comments
   - Test methods → "best-effort cleanup"
3. Applies context-aware fixes:
   - **src/robotocore/**: Replaces `pass` with `logger.debug("context: %s", exc)`.
     Adds `import logging` and `logger = logging.getLogger(__name__)` to files
     that need them. Binds exceptions with `as exc` if not already bound.
   - **tests/**: Adds explanatory comments (e.g., `# best-effort cleanup`)

**Result**: 131 src blocks now log to debug (visible in CI server logs), 1057
test blocks now have explanatory comments. Zero empty except:pass remain.

Added to CI (`test-quality` job) and `.pre-commit-config.yaml` as `--check` gate.
