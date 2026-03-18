---
session: "a86d371f"
timestamp: "2026-03-17T07:00:00Z"
model: claude-sonnet-4-6
sequence: 3
---

## Human

I still see issues with swallowed exceptions. We should never do this without at
least logging something so an agent can tell whether the changes are correct.
Ensure it's impossible for an exception to be silently swallowed. Make it so these
github-code-quality comments stop happening because the code is good.

## Assistant

## Key decisions

**Problem**: Even after narrowing `except Exception:` to `except ClientError:`,
the blocks still contained only `pass`. The GitHub code-quality bot flags any
`except X: pass` with no comment or logging as an "empty except" — the type
narrowing is necessary but not sufficient.

**Rule established**: Every `except` block that doesn't re-raise or meaningfully
handle the exception MUST call `logging.debug(...)`. This gives agents and devs
a trace they can enable with `--log-level=DEBUG` to understand why a teardown
step was skipped.

**Pattern applied in tests/moto_impl/**:
```python
except ClientError as e:
    logging.debug("pre-cleanup skipped: %s", e)
```
These are pytest fixture teardown blocks where the resource may already be absent.
The `pass` was intentional (don't fail the test on teardown failure) but the log
makes the decision visible.

**Pattern applied in src/robotocore/**:
Uses each module's existing `logger = logging.getLogger(__name__)`. The old
`except Exception:  # noqa: BLE001` + `pass  # comment` was replaced with
`except Exception as e:  # noqa: BLE001` + `logger.debug("message: %s", e)`.

**Retained**: `except KeyboardInterrupt: pass` in cli.py signal handling — this
is correct behavior for a clean shutdown, not an error condition.
