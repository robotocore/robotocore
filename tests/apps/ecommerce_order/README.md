# E-Commerce Order Processing Platform

Simulates a Shopify-like order processing backend using AWS services.

## Architecture

```
Storefront (tests)
    │
    ▼
SQS FIFO Queue ──────► Order Processor ──────► DynamoDB (orders, inventory, coupons)
 (per-customer           │                        │
  ordering)              ├──► Secrets Manager      ├── by-status GSI
                         │    (payment creds)      └── by-customer GSI
                         │
                         ├──► SNS (confirmations)
                         │
                         └──► S3 (receipts)
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| **SQS FIFO** | Order ingestion queue with per-customer ordering and DLQ |
| **DynamoDB** | Orders table (with GSIs), inventory table, coupons table |
| **Secrets Manager** | Payment gateway credentials storage and rotation |
| **SNS** | Order confirmation notifications |
| **S3** | Receipt document archive |

## Order Lifecycle

```
SUBMITTED ──► PROCESSING ──► PAYMENT_PENDING ──► PAID ──► SHIPPED ──► DELIVERED ──► COMPLETED
    │              │                                                                    │
    └── CANCELLED ─┘                                                               REFUNDED
```

## Test Files

- `test_order_lifecycle.py` — Status transitions, cancellation, end-to-end
- `test_queue_processing.py` — FIFO ordering, DLQ, batch processing
- `test_payment.py` — Credentials, payment success/failure, refunds
- `test_inventory.py` — Stock tracking, reservation, insufficient stock
- `test_receipts.py` — S3 receipt generation and retrieval
- `test_queries.py` — GSI queries, date ranges, statistics
- `test_discounts.py` — Coupon application, usage limits, expiration

## How to Run

```bash
# Start the server
make start

# Run just these tests
uv run pytest tests/apps/ecommerce_order/ -v

# Run against real AWS
AWS_ENDPOINT_URL=https://... uv run pytest tests/apps/ecommerce_order/ -v
```
