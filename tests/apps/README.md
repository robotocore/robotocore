# Real-World Application Integration Tests

These tests simulate real applications using AWS services. They are **completely isolated** from robotocore internals -- the only dependencies are `boto3`, `pytest`, and the Python standard library.

## What's tested

| App directory | Services | What it simulates |
|---------------|----------|-------------------|
| `file_processing/` | S3, DynamoDB | Document management: uploads, versioning, search, lifecycle, presigned URLs |
| `message_queue/` | SQS | Distributed message broker: standard/FIFO queues, DLQ, routing, batch ops |
| `event_driven/` | EventBridge, SNS, SQS, DynamoDB | Event-driven microservices: routing, fan-out, schemas, pattern matching |
| `secrets_rotation/` | Secrets Manager, DynamoDB | Secrets management: rotation, audit logging, schema validation, namespaces |
| `serverless_api/` | API Gateway, Lambda, DynamoDB, IAM, Step Functions | Serverless REST API: CRUD, workflows, deployments |
| `cicd_pipeline/` | S3, DynamoDB, SSM, SNS, SQS, CloudWatch Logs, Step Functions | CI/CD build pipeline: artifacts, build history, notifications, orchestration |
| `content_mgmt/` | S3, DynamoDB, SQS, SNS, CloudWatch Logs, EventBridge | Headless CMS: content lifecycle, media, publishing, versioning, audit |
| `data_pipeline/` | Kinesis, S3, DynamoDB, SSM, Secrets Manager, CloudWatch | IoT sensor pipeline: ingestion, storage, processing, monitoring |
| `ecommerce_order/` | SQS FIFO, DynamoDB, S3, SNS, Secrets Manager | E-commerce orders: queue processing, payments, inventory, receipts, coupons |
| `multitenant_saas/` | DynamoDB, S3, SSM, Secrets Manager, SQS, CloudWatch | Multi-tenant SaaS: tenant isolation, quotas, onboarding, usage tracking |
| `notification_dispatch/` | SNS, SQS, S3, DynamoDB, CloudWatch | Multi-channel notifications: templates, preferences, delivery tracking, scheduling |
| `scheduled_tasks/` | EventBridge, DynamoDB, S3, SNS, CloudWatch | Task scheduler: cron jobs, execution tracking, dependencies, retry, alerts |
| `user_auth/` | DynamoDB, S3, Secrets Manager, SSM, CloudWatch | Auth & identity: registration, sessions, passwords, MFA, audit logging |

## Running

Against robotocore:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/ -v
```

Against real AWS (with credentials configured):

```bash
unset AWS_ENDPOINT_URL
pytest tests/apps/ -v
```

With explicit credentials:

```bash
AWS_ENDPOINT_URL=http://localhost:4566 \
AWS_ACCESS_KEY_ID=testing \
AWS_SECRET_ACCESS_KEY=testing \
AWS_DEFAULT_REGION=us-east-1 \
pytest tests/apps/ -v
```

## Design principles

- **Zero internal imports**: No `robotocore` or `moto` imports. These tests don't know what backend they're running against.
- **Realistic naming**: Resource names use app-domain language (`user-uploads-bucket`, `order-processing-queue`) with UUID suffixes to avoid collisions.
- **Self-cleaning**: Every test cleans up its resources via fixtures.
- **Portable**: Works against robotocore or real AWS without modification.
