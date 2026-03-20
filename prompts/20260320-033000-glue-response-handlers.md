---
role: assistant
timestamp: 2026-03-20T03:30:00Z
session: glue-response-handlers
sequence: 1
---

# Fix Glue response handlers

The Glue gap agent added 15+ model methods but forgot to add matching
response handlers in responses.py. Added 19 response handlers for
ML task runs, schema version operations, entity operations, and stubs.
Also added 7 compat tests for the newly-working operations.
