# Content Management System

A headless CMS for a publishing platform, similar to Contentful or Strapi,
built entirely on AWS primitives.

## Architecture

```
Editors / API clients
        │
        ▼
┌──────────────────┐
│  CMS API Layer   │  (ContentManagementSystem class)
└──────┬───────────┘
       │
       ├──▶ DynamoDB  — content metadata (title, slug, status, author, tags)
       │                 GSIs: by-category, by-status, by-author, by-publish-date
       │                 versions table for edit history
       │                 media metadata table
       │
       ├──▶ S3        — media assets (images, files) with versioning enabled
       │
       ├──▶ SQS       — publish queue (workers drain to perform publication)
       │
       ├──▶ SNS       — webhook fan-out (notify subscribers on publish/update)
       │
       ├──▶ CloudWatch Logs — immutable audit trail of every content mutation
       │
       └──▶ EventBridge     — scheduled publishing rules
```

## AWS Services Used

| Service          | Purpose                                      |
|------------------|----------------------------------------------|
| S3               | Media asset storage with versioning           |
| DynamoDB         | Content metadata, versions, media metadata    |
| SQS              | Publish queue for async publication workflow   |
| SNS              | Webhook notifications to external subscribers  |
| CloudWatch Logs  | Immutable audit trail                         |
| EventBridge      | Scheduled publishing rules                    |

## Content Lifecycle

```
DRAFT ──▶ REVIEW ──▶ PUBLISHED ──▶ ARCHIVED
  │          │           │              │
  │          ▼           │              ▼
  ├──▶ SCHEDULED ───────┘           DRAFT (re-draft)
  │
  └──▶ ARCHIVED
```

## Publishing Workflow

1. Author creates content (DRAFT)
2. Editor reviews and approves (REVIEW)
3. Content is queued for publication (SQS)
4. Publish worker drains queue, transitions to PUBLISHED
5. SNS notification sent to all webhook subscribers
6. Audit entry logged to CloudWatch

## How to Run

```bash
# Start the server
make start

# Run these tests
uv run pytest tests/apps/content_mgmt/ -v

# Or with explicit endpoint
AWS_ENDPOINT_URL=http://localhost:4566 uv run pytest tests/apps/content_mgmt/ -v
```
