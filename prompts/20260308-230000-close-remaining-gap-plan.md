---
session: "4526be9f"
timestamp: "2026-03-08T23:00:00Z"
model: claude-opus-4-6
---

## Human

Once this is all done, make a similar plan but for closing the rest of the gap.

## Assistant

# Plan: Close the Remaining Coverage Gap (35% → 100%)

## Current State
- **3,222 / 9,196 ops tested (35.0%)**
- **5,974 ops remaining** across 122 services
- 10 commits pushed today, adding +286 tested ops

## Gap Analysis

| Tier | Services | Gap Ops | Strategy |
|------|----------|---------|----------|
| Mega (>200 gap) | ec2, connect, sagemaker, quicksight | 1,299 | Chunk by resource noun, parallel agents |
| Large (50-200) | 32 services | 2,650 | CRUD patterns, 5 agents per wave |
| Medium (20-50) | 44 services | 1,546 | Batch 3-4 services per agent |
| Small (5-20) | 32 services | 450 | One agent handles 5-8 services |
| Tiny (<5) | 10 services | 29 | Direct manual addition |

## Phase 1: Quick Wins (~30 min, +29 ops → 35.3%)

**Tiny services** (10 services, 29 ops): Do these directly without agents.
- servicediscovery (5), sns (5), support (5), pipes (3), polly (3), sts (3),
  acm (2), resourcegroupstaggingapi (1), s3 (1), secretsmanager (1)

## Phase 2: Small Services (~2 hours, +450 ops → 40.2%)

**32 services, 5-20 ops each.** Batch into 6 agents, each handling 5-6 services.

Agent allocation:
1. batch, organizations, textract, budgets, lambda (94 ops)
2. xray, ses, synthetics, appmesh, connectcampaigns (85 ops)
3. mwaa, timestream-write, emr-serverless, ce, mediapackage (74 ops)
4. stepfunctions, route53resolver, scheduler, applicationautoscaling (65 ops)
5. amp, cloudcontrol, clouddirectory, codecommit, managedblockchain (68 ops)
6. fis, ivs, ram, signer, waf, apprunner (64 ops)

## Phase 3: Medium Services (~4 hours, +1,546 ops → 57.0%)

**44 services, 20-50 ops each.** Batch into 11 agents, 4 services per agent.

Priority ordering (by likely Moto coverage):
1. logs, autoscaling, eks, emr (194 ops)
2. neptune, athena, cloudtrail, lakeformation (194 ops)
3. resiliencehub, datasync, imagebuilder, mq (176 ops)
4. route53domains, elbv2, wafv2, apigateway (155 ops)
5. ssm, kinesis, dynamodb, firehose (133 ops)
6. kms, s3control, acm-pca, codepipeline (128 ops)
7. cloudformation, appsync, apigatewayv2, cognito-idp (126 ops)
8. rds, backup, opensearch, elasticache (119 ops)
9. ecr, ecs, docdb, mediastore (109 ops)
10. sqs, sns, events, secretsmanager (109 ops)
11. comprehend, codebuild, kafka, greengrass (103 ops)

## Phase 4: Large Services (~6 hours, +2,650 ops → 85.8%)

**32 services, 50-200 ops each.** One agent per service, use chunk_service.py.

High-confidence (likely Moto-backed): glue, iot, cloudfront, dms, securityhub,
backup, bedrock, redshift, sesv2, ds, macie2, rekognition, transfer, elasticache,
guardduty, inspector2, workspaces, codecommit, servicecatalog

Lower-confidence (may need Moto fixes): medialive, pinpoint, networkmanager,
mediaconnect, quicksight (partial)

Each agent:
1. `chunk_service.py --service X --with-probe --untested-only`
2. For each chunk: write CRUD tests, run immediately, keep passing, delete 501s
3. For ops returning 500: check if Moto fix is feasible, implement in vendor/moto
4. Commit after each chunk

## Phase 5: Mega Services (~8 hours, +1,299 ops → 100%)

### EC2 (417 remaining ops)
- Already chunked into ~294 resource groups
- Use `chunk_service.py --service ec2 --untested-only` to break into 50-60 chunks
- Fan out 10 agents at a time, each handling 5-6 chunks
- 4 rounds of 10 agents = 40 agents total

### Connect (361 ops, 1.6% coverage)
- Moto has limited Connect support; most ops will be 501
- Probe first to find working ops (likely 20-40)
- Write tests for working ops, mark rest as "needs Moto implementation"
- Implement high-value Connect ops in Moto if needed

### SageMaker (306 ops, 19.3% coverage)
- Already expanded from 35→73 in this session
- Many CRUD patterns still untested
- 6 agent rounds, each covering a resource noun group

### QuickSight (215 ops, 6.5% coverage)
- Very limited Moto support
- Probe to find working ops, test those
- Implement dashboard/analysis/dataset CRUD in Moto

## Infrastructure Improvements Needed

1. **Server restart automation**: After Moto fixes, auto-restart server and re-run failed tests
2. **Flaky test detection**: Some tests fail due to shared server state (e.g., subnet conflicts)
   - Add retry logic or better cleanup in fixtures
3. **Coverage tracking CI**: Add `compat_coverage.py --all` to CI, track over time
4. **Moto fix pipeline**: Batch upstream contributions to getmoto/moto

## Timeline Estimate

| Phase | Duration | Ops Added | Cumulative |
|-------|----------|-----------|------------|
| Phase 1 (Tiny) | 30 min | +29 | 35.3% |
| Phase 2 (Small) | 2 hours | +450 | 40.2% |
| Phase 3 (Medium) | 4 hours | +1,546 | 57.0% |
| Phase 4 (Large) | 6 hours | +2,650 | 85.8% |
| Phase 5 (Mega) | 8 hours | +1,299 | 100% |
| **Total** | **~20 hours** | **+5,974** | **100%** |

Note: The 100% target assumes all ops are implementable in Moto. Realistically,
some ops (especially in Connect, QuickSight, MediaLive) may require new Moto
backends. The achievable target without Moto work is ~70-80%.

## Key Lessons from Today's Session

1. **CRUD patterns are the real yield**: Probe-based testing only finds no-param ops
   (already tested). The big gains come from create→describe→modify→delete patterns.
2. **Parallel worktree agents work well**: 20 simultaneous agents, each in its own
   worktree, produced ~5,000 lines of test code with minimal conflicts.
3. **Merge-then-test is efficient**: Copy files from worktree → run pytest → fix
   any issues → commit. Faster than waiting for agents to commit.
4. **Branch hygiene matters**: Worktree agents can switch the main repo's branch
   unexpectedly. Always verify `git branch --show-current` before committing.
5. **Wave approach**: Launch 5-10 agents → wait 3-5 min → merge results → push →
   launch next wave. Keeps git history clean and provides checkpoints.
