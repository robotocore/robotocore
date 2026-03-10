---
role: assistant
timestamp: 2026-03-10T16:00:00Z
session: pages-story-and-debranding
sequence: 2
---

# Fix flaky rdsdata compat test

The `test_execute_statement` test in `test_rdsdata_compat.py` was failing in CI because it called `execute_statement` against a non-existent RDS cluster. In parallel test execution, there's no guarantee another test creates the cluster first.

**Fix**: Added an `rds_cluster` fixture that creates an Aurora MySQL cluster with HTTP endpoint enabled before the test runs, and cleans it up afterward. This ensures the test is self-contained and doesn't depend on execution order.
