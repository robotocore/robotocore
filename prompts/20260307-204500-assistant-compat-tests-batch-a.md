---
role: assistant
model: claude-opus-4-6
timestamp: "2026-03-07T20:45:00Z"
session: "c02d92d0"
sequence: 7
tools: [subagent, probe_service]
---

## Compat tests for 22 newly registered services

Probed all high-impact Moto-backed services against live server, then fanned out 22 parallel agents to write compat tests. Each agent ran tests against the live server and only included passing operations.

**Services covered**: RDS, ELBv2, Organizations, CloudFront, EFS, Autoscaling, EKS, Glue, CloudTrail, WAFv2, Athena, CodePipeline, Neptune, ServiceDiscovery, ElastiCache, EMR, Backup, GuardDuty, MSK, MQ, X-Ray, CodeBuild.

**Key findings**:
- ElastiCache: most operations fail (only describe_replication_groups, describe_cache_clusters, subnet groups work)
- GuardDuty: detector CRUD works, but list_findings/get_invitations_count return 501 (service not routed to Moto properly for some REST endpoints)
- Moto bug: Glue tagged resources leak through `get_resources` TagFilters — fixed test with ResourceTypeFilters

**Test counts**: 1799 → 2080 compat tests (281 new), 2520 unit tests, 0 failures.
