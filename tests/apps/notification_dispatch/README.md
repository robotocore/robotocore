# Notification Dispatch System

A multi-channel notification dispatch service, similar to Twilio SendGrid combined with push notification infrastructure.

## What it does

Sends notifications to users across multiple channels (email, SMS, push, webhook, in-app) with:
- **Template management**: Store and render notification templates with variable substitution (S3)
- **Channel routing**: Per-channel SNS topics with SQS subscriber queues for reliable delivery
- **User preferences**: Per-user opt-in/opt-out per channel, quiet hours (DynamoDB)
- **Delivery tracking**: Full audit trail of every notification: who, what channel, when, status (DynamoDB)
- **Priority levels**: CRITICAL, HIGH, NORMAL, LOW — critical bypasses opt-out and quiet hours
- **Bulk send**: Send to multiple users with per-user preference checking
- **Retry**: Re-queue failed deliveries with attempt tracking
- **Scheduled notifications**: Store future notifications for later dispatch
- **Metrics**: CloudWatch metrics for sent/delivered/failed/bounced per channel
- **Audit logs**: CloudWatch Logs for detailed delivery audit trail

## Architecture

```
app → NotificationService
  ├── SNS topics (one per channel type)
  │     └── SQS queues (subscriber/consumer per channel)
  ├── S3 bucket (notification templates)
  ├── DynamoDB tables
  │     ├── delivery tracking (notification_id + channel, GSIs by user/channel)
  │     ├── user preferences (user_id → channel opt-in/opt-out + quiet hours)
  │     └── scheduled notifications (schedule_id, GSI by user)
  └── CloudWatch
        ├── Metrics (NotificationSent, Delivered, Failed per channel)
        └── Logs (audit trail of all dispatch events)
```

## AWS services used

- **SNS**: One topic per channel type — the fan-out mechanism
- **SQS**: Consumer queues subscribed to SNS topics — reliable message delivery
- **S3**: Template storage with JSON payloads and metadata
- **DynamoDB**: Delivery records, user preferences, and scheduled notifications
- **CloudWatch**: Metrics (put_metric_data) and Logs (put_log_events)

## How to run

```bash
# Against robotocore (localhost:4566)
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/notification_dispatch/ -v

# Against real AWS
AWS_PROFILE=my-profile pytest tests/apps/notification_dispatch/ -v
```
