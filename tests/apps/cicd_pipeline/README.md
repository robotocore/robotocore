# CI/CD Pipeline Application

A CI/CD build pipeline system, similar to a simplified AWS CodePipeline. Demonstrates a multi-service application pattern using only boto3.

## Architecture

```
Developer pushes code
        │
        ▼
Pipeline triggered (queue build)
        │
        ▼
SSM Parameter Store ──► Read pipeline config (repo URL, build commands, deploy target)
        │
        ▼
DynamoDB ──► Record build (QUEUED → BUILDING → TESTING → DEPLOYING → SUCCESS/FAILED)
        │
        ▼
CloudWatch Logs ──► Stream build output (log group per repo, log stream per build)
        │
        ▼
S3 ──► Upload build artifacts (versioned, tagged with git SHA, branch, build number)
        │
        ▼
Step Functions ──► Orchestrate pipeline stages (Checkout → Build → Test → Deploy)
        │
        ▼
SNS → SQS ──► Notify subscribers (build started, succeeded, failed)
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| **S3** | Build artifact storage with metadata tags |
| **DynamoDB** | Build history tracking with GSIs (by-repo, by-status) |
| **SSM Parameter Store** | Pipeline configuration (hierarchical paths) |
| **SNS + SQS** | Build event notifications |
| **CloudWatch Logs** | Build log streaming |
| **Step Functions** | Pipeline stage orchestration |
| **IAM** | Role for Step Functions execution |

## How to Run

```bash
# Against robotocore (localhost:4566)
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/cicd_pipeline/ -v

# Against real AWS (requires credentials)
pytest tests/apps/cicd_pipeline/ -v
```
