---
role: assistant
timestamp: 2026-03-19T18:00:00Z
session: register-new-services
sequence: 1
---

# Register 10 new Moto-backed services

## Human prompt
Register unregistered Moto services, probe for working operations,
write compat tests.

## Services registered
directconnect (63 ops), ebs (6), forecast (63), personalize (71),
sdb (10), service-quotas (26), sagemaker-runtime (3), mediastore-data (5),
bedrock-runtime (10), kinesis-video-archived-media (6)

## Probe results
Most services are entirely 501 (not implemented in Moto). Working ops:
- forecast: ListDatasetGroups, DescribeDatasetGroup
- personalize: ListSchemas, DescribeSchema
- ebs: StartSnapshot

## Tests written
5 new compat tests across 3 services (ebs, forecast, personalize).
Other services registered but no tests (all 501).
