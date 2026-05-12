---
session: "20260511-open-issues"
timestamp: "2026-05-11T00:01:00Z"
model: claude-sonnet-4-6
sequence: 2
---

## Human

One by one, solve each open GitHub issue with a PR. (Continuation — working on issue #222.)

## Assistant

## Key decisions

**Issue #222: SES SendEmail API emails not appearing in /_robotocore/ses/messages**

Root cause: `EmailStore` is exclusively fed by `RobotocoreSMTPHandler.handle_DATA` in the SMTP server. When boto3/AWS SDK calls `ses.send_email()`, the request goes through the native SES provider → Moto, and Moto stores emails in `backend.sent_messages` (a separate, internal data structure). The management endpoint at `/_robotocore/ses/messages` only reads from `EmailStore`, so API-sent emails never appear.

**Approach chosen: intercept after Moto succeeds**

Three approaches considered:

1. **Bridge Moto's store** — query `backend.sent_messages` in the management endpoint and translate Moto's `Message`/`RawMessage`/`TemplateMessage`/`BulkTemplateMessage` types. Rejected: fragile against Moto version changes; Moto's internal message types differ between SES v1 and v2 paths.

2. **Intercept before Moto** — extract email data from params, add to store, then forward to Moto. Rejected: if Moto rejects the email (unverified sender, invalid address), we'd capture a failed send.

3. **Intercept after Moto** (chosen) — forward to Moto as usual, check HTTP 200, then extract email data from params and add to EmailStore. Only successful sends are captured. Capture errors are logged as debug and never surface to callers.

**Scope of changes:**

- `email_store.py`: Added `source: Literal["smtp", "api"]` field (default "smtp") to `StoredEmail` and `add_message()`. Exposed in `to_dict()`. Backward-compatible — existing SMTP code works unchanged.
- `provider.py` (SES v1): Added `_SEND_ACTIONS` set; after Moto returns 200, calls `_capture_send()` dispatcher. Capture helpers: `_capture_send_email` (extracts Source + Destination.*.member.N params), `_capture_send_raw_email` (decodes base64, parses email headers), `_capture_send_templated_email` (records template name as subject).
- `sesv2_provider.py` (SES v2): Added `_OUTBOUND_EMAILS_PATH` regex; intercepts POST `/v2/email/outbound-emails`, reads body bytes, forwards to Moto, then captures on success. `_capture_sesv2_send_email` parses the JSON body's `Content.Simple`/`Content.Template` structure.

**Tests**: 16 new unit tests covering `StoredEmail.source`, `EmailStore` with source parameter, all SES v1 capture helpers (SendEmail, SendRawEmail, SendTemplatedEmail, indexed param collection), and SES v2 capture (simple email, template email, empty body).
