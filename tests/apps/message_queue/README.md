# Message Queue Application

Simulates a distributed order processing system built on AWS SQS. Demonstrates
production patterns for reliable message delivery, FIFO ordering, dead-letter
queue handling, message routing, and batch operations.

## Architecture

```
                            ┌──────────────────┐
                            │   Message Router  │
                            │  (attribute-based)│
                            └────────┬─────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                      │
              ▼                      ▼                      ▼
   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │  Orders Queue    │   │  Returns Queue   │   │  FIFO Queue      │
   │  (standard)      │   │  (standard)      │   │  (ordered msgs)  │
   └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘
            │                      │                      │
            ▼                      ▼                      ▼
   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
   │  Consumer        │   │  Consumer        │   │  Group Consumer  │
   │  (batch receive) │   │  (filtered)      │   │  (per-group)     │
   └────────┬─────────┘   └────────┬─────────┘   └──────────────────┘
            │                      │
            │  (on failure)        │
            ▼                      ▼
   ┌──────────────────┐   ┌──────────────────┐
   │  Dead Letter Q   │   │  Delay Queue     │
   │  (failed msgs)   │   │  (scheduled)     │
   └──────────────────┘   └──────────────────┘
```

## AWS Services Used

- **SQS Standard Queues** — at-least-once delivery, best-effort ordering
- **SQS FIFO Queues** — exactly-once processing, strict ordering per message group
- **Dead-Letter Queues** — automatic redrive for messages exceeding maxReceiveCount
- **Delay Queues** — queue-level and per-message delivery delay

## Patterns Demonstrated

- **Fan-out routing**: Route messages to different queues based on message attributes
- **Exactly-once processing**: FIFO queues with deduplication (explicit ID and content-based)
- **Dead-letter handling**: Automatic DLQ redrive after N failed processing attempts
- **Message routing**: Attribute-based filtering and routing to target queues
- **Queue-to-queue forwarding**: Consume from one queue, transform, produce to another
- **Batch operations**: Send/receive/delete up to 10 messages per API call
- **Visibility timeout**: Control message re-delivery window for failed consumers
- **Long polling**: Efficient polling that reduces empty responses

## How to Run

```bash
# Start robotocore (or point at real AWS)
make start

# Run all message queue tests
uv run pytest tests/apps/message_queue/ -v

# Run a specific test file
uv run pytest tests/apps/message_queue/test_fifo.py -v

# Against real AWS
AWS_ENDPOINT_URL=https://sqs.us-east-1.amazonaws.com pytest tests/apps/message_queue/ -v
```
