---
session: "migration-parity"
timestamp: "2026-03-11T22:00:00Z"
model: claude-opus-4-6
sequence: 1
---

## Human

How does our implementation of kinesis compare to kinesalite? What other open
source projects do we implement that folks might want to migrate off of, toward
robotocore? For each, do the same thing — create a migration guide.

## Assistant

## Key decisions

**Researched 8 competing emulators** via parallel agents: kinesalite, dynalite,
DynamoDB Local, ElasticMQ, goaws, s3rver, Adobe S3Mock, moto standalone server.
Each fills a niche (single-service, language-specific) that robotocore replaces
with a single container covering 147 services.

**Organized guides by service directory**: Per-service emulators (kinesalite,
dynalite, ElasticMQ, etc.) got README.md files in `src/robotocore/services/<service>/`.
Multi-service emulators (LocalStack, moto server) got project-level docs. This
way the migration guide lives next to the code it describes.

**Operation parity tables**: Each guide includes a table mapping every operation
the legacy emulator supports to robotocore's equivalent, with status indicators.
This gives migrators confidence about coverage gaps (there are almost none).

**Config mapping sections**: Port numbers, env vars, SDK endpoint URLs — the
practical details someone needs to `s/old-emulator/robotocore/` in their
docker-compose and test configs.
