# S3 Connectivity Matrix

| Feature | Edge | Required operations | Current status | Semantic assertions |
|---|---|---|---|---|
| Amazon S3 Event Notifications | `s3->sqs` | `PutBucketNotificationConfiguration, GetBucketNotificationConfiguration, PutObject` | `pass` | Catalog evidence files present |
| Amazon S3 Event Notifications | `s3->sns` | `PutBucketNotificationConfiguration, GetBucketNotificationConfiguration, PutObject` | `pass` | Catalog evidence files present |
| Amazon S3 Event Notifications | `s3->lambda` | `PutBucketNotificationConfiguration, GetBucketNotificationConfiguration, PutObject` | `pass` | Catalog evidence files present |
| Amazon S3 Replication | `s3->iam` | `PutBucketReplication, GetBucketReplication, DeleteBucketReplication` | `pass` | Catalog evidence files present |
| Amazon S3 Replication | `s3->kms` | `PutBucketReplication, GetBucketReplication, DeleteBucketReplication` | `pass` | Catalog evidence files present |
| Amazon S3 Object Lambda WriteGetObjectResponse | `s3->lambda` | `WriteGetObjectResponse` | `pass` | Catalog evidence files present |
| Amazon S3 Event Notifications to EventBridge | `s3->eventbridge` | `PutBucketNotificationConfiguration, GetBucketNotificationConfiguration` | `pass` | Catalog evidence files present |
