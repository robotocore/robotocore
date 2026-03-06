---
role: human
timestamp: "2026-03-06T19:39:45Z"
session: "449754"
sequence: 7
reconstructed: true
---

Fix the critical kinesis bug in Moto, send it upstream in a PR, and then update the version of moto we're using to reference that branch. Make sure there are failing tests that exercise this bug and that those tests are then passing after. Do nothing without 100% test coverage.
