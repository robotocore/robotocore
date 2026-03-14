---
session: "c71759e9"
timestamp: "2026-03-13T12:00:00Z"
model: claude-opus-4-6
---

## Human

Close every compat test gap across all 102 AWS services. Started at
83.3% coverage (7,658/9,196 ops). Plan: probe all services, chunk by
resource noun, fan out to 37 parallel worktree agents across 5 waves.

## Assistant

Executed the full plan:

1. **Wave 0 (prep)**: Probed all 102 gap services with probe_service.py,
   generated chunks with chunk_service.py. Server already running.

2. **Wave 1 (Tier 5)**: 4 agents covering 27 services with 1-7 gaps each.
   Yield: ~30 new tests. Most "gaps" were either already tested or 501.

3. **Wave 2 (Tier 4)**: 7 agents covering 21 services with 5-10 gaps.
   Yield: ~35 new tests. Many ops not implemented in Moto.

4. **Wave 3 (Tier 3)**: 14 agents covering 41 services with 10-20 gaps.
   Yield: ~100 new tests. Bedrock, securityhub, neptune, inspector2 had
   the most implementable ops.

5. **Wave 4 (Tier 2)**: 9 agents covering 9 big services (rds, iot, glue,
   cloudfront, etc). Most RDS ops already had coverage; IoT/Glue had some.

6. **Wave 5 (Tier 1)**: 12 agents for EC2 (354 ops, 8 agents) and
   Connect (195 ops, 4 agents).

Key finding: The majority of "missing" ops fall into two categories:
- Already tested but gap tool couldn't match test names to op names
- Return 501 NotImplemented (Moto gaps, not robotocore gaps)

Collected results from all worktrees, fixed lint (line length, duplicate
class names, unused variables), formatted with ruff, committed.
