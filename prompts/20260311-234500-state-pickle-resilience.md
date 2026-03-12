---
role: assistant
timestamp: 2026-03-11T23:45:00Z
session: ci-fix-continuation
sequence: 1
---

# State Save Pickle Resilience

## Problem
Nightly CI revealed that `state/save` crashes when Moto backends contain unpicklable objects. ACM backends hold `cryptography.hazmat.bindings._rust.openssl.rsa.RSAPrivateKey` objects and some backends have `WeakValueDictionary` callbacks — both fail `pickle.dumps()`.

## Fix
Added per-service pickle verification in both `_save_moto_state` and `_capture_state`. Before adding a service's state to the snapshot dict, we trial-pickle it. If it fails, the existing `except Exception` handler logs a debug message and skips that service gracefully.

## Decision
Chose per-service granularity (skip one bad service) over per-account or per-region granularity. Most users care about S3/DynamoDB/SQS state — losing ACM from a snapshot is acceptable and better than losing everything.
