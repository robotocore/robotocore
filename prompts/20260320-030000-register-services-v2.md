---
role: assistant
timestamp: 2026-03-20T03:00:00Z
session: register-services-v2
sequence: 1
---

# Re-register 10 new services + update CLAUDE.md

Previous registration PR was lost. Re-registered directconnect, ebs,
forecast, personalize, sdb, service-quotas, sagemaker-runtime,
mediastore-data, bedrock-runtime, kinesis-video-archived-media.

Updated CLAUDE.md service count from 147 to 157 and test count to 20,000+.
Added 5 compat tests for services with working operations.
