# Event-Driven Microservices Architecture

Simulates a production event-driven system where multiple microservices communicate
through AWS event primitives. Three core services — Order, Inventory, and Notification —
publish and consume events through a shared event bus.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│ Order Service│    │Inventory Svc │    │Notification Svc  │
│  (publisher) │    │  (publisher) │    │   (publisher)    │
└──────┬───────┘    └──────┬───────┘    └────────┬─────────┘
       │                   │                     │
       ▼                   ▼                     ▼
┌─────────────────────────────────────────────────────────┐
│                  EventBridge Custom Bus                  │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │ Order Rules │  │Inventory    │  │ Notification    │ │
│  │ (source +   │  │Rules        │  │ Rules           │ │
│  │  detail)    │  │(prefix,     │  │ (multi-target)  │ │
│  └──────┬──────┘  │ numeric)    │  └───────┬─────────┘ │
│         │         └──────┬──────┘          │           │
└─────────┼────────────────┼─────────────────┼───────────┘
          │                │                 │
          ▼                ▼                 ▼
    ┌──────────┐    ┌──────────┐    ┌───────────────┐
    │SQS Queue │    │SQS Queue │    │  SNS Topic    │
    │(consumer)│    │(consumer)│    │  (fan-out)    │
    └──────────┘    └──────────┘    └───────┬───────┘
                                      ┌─────┼─────┐
                                      ▼     ▼     ▼
                                    SQS   SQS   SQS
                                   Email  SMS  Webhook
```

## AWS Services Used

| Service      | Role                                         |
|-------------|----------------------------------------------|
| EventBridge | Event routing with pattern-based rules        |
| SNS         | Fan-out to multiple consumers                 |
| SQS         | Durable event consumption                     |
| DynamoDB    | Event schema registry (versioned)             |

## Patterns Demonstrated

- **Content-based routing**: EventBridge rules match on source, detail-type, and detail fields
- **Fan-out**: Single SNS topic delivers to multiple SQS queues
- **Filter policies**: SNS subscription filters for selective delivery
- **Event sourcing**: Events archived for replay
- **Schema registry**: JSON Schema validation in DynamoDB with versioning
- **Dead-letter queues**: Failed delivery handling
- **CQRS**: Different consumers process the same event for different purposes

## Test Files

| File               | What it tests                                    |
|-------------------|--------------------------------------------------|
| `test_routing.py` | EventBridge rule matching and SQS delivery       |
| `test_fan_out.py` | SNS topic fan-out, filter policies, raw delivery |
| `test_schemas.py` | DynamoDB schema registry CRUD and validation     |
| `test_patterns.py`| Pattern matching edge cases (prefix, AND, nested)|

## How to Run

```bash
# Against robotocore (default)
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/event_driven/ -v

# Against real AWS (with credentials configured)
unset AWS_ENDPOINT_URL && pytest tests/apps/event_driven/ -v
```
