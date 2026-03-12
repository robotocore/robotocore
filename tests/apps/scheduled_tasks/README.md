# Scheduled Tasks Application

A distributed task scheduler, similar to AWS Data Pipeline or a simplified Airflow,
built entirely on managed AWS services.

## Architecture

```
EventBridge (cron/rate rules)
    |
    v
TaskScheduler (orchestrator)
    |
    +---> DynamoDB (task definitions + execution history)
    +---> SSM Parameter Store (per-task configuration)
    +---> S3 (execution output / artifacts)
    +---> SNS -> SQS (execution alerts)
    +---> CloudWatch Logs (detailed execution logs)
    +---> CloudWatch Metrics (execution stats)
```

## Task Lifecycle

1. **Define** -- `TaskDefinition` stored in DynamoDB, EventBridge rule created
2. **Configure** -- SSM parameters hold per-task timeouts, retries, payloads
3. **Execute** -- Execution records track status: PENDING -> RUNNING -> SUCCESS / FAILED / TIMED_OUT
4. **Output** -- Results written to S3 (`output/{task_id}/{execution_id}/result.json`)
5. **Alert** -- SNS notifications on failure, timeout, or success (opt-in)
6. **Metric** -- CloudWatch metrics per task and per group

## Dependency Model (DAG)

Tasks can declare dependencies on other tasks:

- **SUCCESS** condition: upstream must have succeeded
- **COMPLETED** condition: upstream must be in any terminal state

`execute_with_dependencies()` checks all upstream tasks and records SKIPPED if blocked.

## Retry

Failed executions can be retried up to `max_retries` times. Each retry is a new
execution record with an incremented `attempt` number.

## How to Run

```bash
# Against robotocore (default)
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/scheduled_tasks/ -v

# Against real AWS
AWS_ENDPOINT_URL= pytest tests/apps/scheduled_tasks/ -v
```
