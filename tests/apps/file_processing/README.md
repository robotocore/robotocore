# File Processing Service

A simulated **document management system** for a SaaS product. Users upload files (reports, invoices, images, CSVs), which are stored in S3 with rich metadata indexed in DynamoDB for fast searching and lifecycle management.

## Architecture

```
                        ┌──────────────────────┐
                        │   FileProcessing     │
                        │      Service         │
                        │                      │
                        │  - upload_file()     │
                        │  - search()          │
                        │  - set_status()      │
                        │  - archive_file()    │
                        │  - list_versions()   │
                        └───────┬──────┬───────┘
                                │      │
                 ┌──────────────┘      └──────────────┐
                 │                                    │
                 ▼                                    ▼
    ┌────────────────────┐             ┌──────────────────────┐
    │        S3          │             │      DynamoDB        │
    │                    │             │                      │
    │  Bucket:           │             │  Table: file-meta-*  │
    │   /docs/           │             │                      │
    │   /images/         │             │  PK: file_id         │
    │   /reports/        │             │                      │
    │   /archive/        │             │  GSIs:               │
    │                    │             │   content-type-index  │
    │  Versioning: ON    │             │   status-index        │
    │  Presigned URLs    │             │                      │
    └────────────────────┘             └──────────────────────┘
```

## AWS services used

| Service | Purpose |
|---------|---------|
| **S3** | File storage with versioning, prefix-based organization, presigned URLs for direct browser access, multipart upload for large files |
| **DynamoDB** | Metadata index with GSIs on `content_type` and `status` for flexible querying; scan filters for tag search, date ranges, compound queries |

## Why DynamoDB for metadata?

S3 object tags are limited (10 per object, 256-char values) and not efficiently queryable. DynamoDB gives us:

- **GSIs**: Query by content type or lifecycle status without scanning all objects
- **Rich attributes**: Unlimited tags, SHA-256 checksums, upload timestamps, processing status
- **Atomic updates**: Status transitions are single UpdateItem calls
- **Pagination**: Built-in support for large result sets

## Test modules

| File | What it tests |
|------|---------------|
| `test_uploads.py` | Single/bulk uploads, content-type detection, deduplication, SHA-256, prefix organization |
| `test_versioning.py` | S3 versioning, version listing, retrieval by ID, deletion, restore |
| `test_search.py` | Prefix listing, tag search, content-type/status/date-range filters, compound queries |
| `test_lifecycle.py` | Status transitions, archival, bulk delete, copy, storage stats, presigned URLs |

## Running

Against robotocore (localhost):

```bash
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/file_processing/ -v
```

Against real AWS:

```bash
unset AWS_ENDPOINT_URL
pytest tests/apps/file_processing/ -v
```
