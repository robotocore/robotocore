# Serverless API Application

A user management service built on a serverless stack: API Gateway for HTTP routing, Lambda for compute, DynamoDB for storage, IAM for permissions, and Step Functions for workflow orchestration.

## Architecture

```
                    ┌──────────────┐
  HTTP Request ───► │ API Gateway  │
                    │  (REST API)  │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐       ┌────────────┐
                    │   Lambda     │──────►│  DynamoDB   │
                    │  (handler)   │◄──────│  (storage)  │
                    └──────┬───────┘       └────────────┘
                           │
                    ┌──────▼───────┐
                    │    Step      │
                    │  Functions   │
                    │ (workflows)  │
                    └──────────────┘
```

## AWS Services and Roles

| Service | Role |
|---------|------|
| **API Gateway** | HTTP routing — REST API with resources, methods, stages, API keys |
| **Lambda** | Compute — CRUD handler, authorizer, workflow processor |
| **DynamoDB** | Storage — users table with GSI on email, batch operations |
| **IAM** | Permissions — execution roles for Lambda and Step Functions |
| **Step Functions** | Orchestration — user signup, validation, activation workflows |

## Patterns

- **Lambda proxy integration**: API Gateway forwards the full HTTP request to Lambda as-is
- **CRUD operations**: Create, read, update, delete via DynamoDB with expression syntax
- **Workflow orchestration**: Step Functions chains Pass/Choice/Parallel states
- **CORS configuration**: OPTIONS method with MOCK integration and response headers
- **API key management**: Usage plans with throttle and quota limits
- **Function versioning**: Publish versions, create aliases for canary deployments

## How to Run

```bash
# Start the emulator
make start

# Run all serverless API tests
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/serverless_api/ -v

# Run a specific test file
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/serverless_api/test_dynamodb.py -v
```
