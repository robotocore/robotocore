# Event Chain

Multi-service event-driven chain tests proving robotocore handles real AWS triggers.

## Architecture

```
S3 PutObject ──► Lambda A (writes DDB) ──► DDB Stream ──► Lambda B (SNS) ──► SQS
EventBridge PutEvents ──► Rule ──► SQS/Lambda targets
CloudWatch Alarm ──► SNS ──► SQS
```

## Event Flows Tested

1. S3 notification → Lambda → DynamoDB
2. S3 notification → SQS (no Lambda)
3. S3 prefix filter (matching + non-matching)
4. EventBridge → SQS target
5. EventBridge → Lambda target → DynamoDB
6. EventBridge pattern filter
7. SQS → Lambda ESM → DynamoDB
8. SQS ESM batch processing
9. SQS message deletion after ESM success
10. DynamoDB Streams → Lambda ESM
11. DynamoDB OLD_AND_NEW_IMAGES capture
12. CloudWatch Alarm → SNS → SQS
13. Alarm OK transition
14. Full chain: S3 → Lambda → DDB → Stream → Lambda → SNS → SQS
15. Full chain: EventBridge → SQS → ESM → Lambda → DynamoDB

## Services Used

S3, Lambda, DynamoDB, DynamoDB Streams, SQS, SNS, EventBridge, CloudWatch, IAM
