---
session: "20260511-open-issues"
timestamp: "2026-05-11T00:00:00Z"
model: claude-sonnet-4-6
sequence: 1
---

## Human

One by one, solve each open GitHub issue with a PR. Spend time thinking about various tradeoffs before selecting a solution. Solve problems categorically, don't just address individual symptoms.

## Assistant

## Key decisions

Three open issues to address:

**Issue #220: Error starting container in GitHub Actions**

Two symptoms reported: (1) wrong image name in README examples, (2) Starlette `on_startup` error.

The `on_startup` bug was already fixed in commit `b1efff7a` (migrated to `lifespan=` context manager). The remaining problem is that docs still reference `robotocore/robotocore:latest` (Docker Hub format) after Docker Hub publishing was removed in `a5b21e5a`.

**Categorical fix chosen**: Update ALL documentation files (README.md, LOCALSTACK.md, AGENTS.md, .claude/skills/use-in-tests.md, all service READMEs) to use `ghcr.io/robotocore/robotocore:latest`. 50+ occurrences across 10 files.

Did NOT add Docker Hub publishing back — that would require Docker Hub credentials not in the repo secrets, and GHCR is the canonical registry going forward. The correct fix is accurate docs, not adding infrastructure complexity.

Used `perl -pi -e` with negative lookbehind to replace image references while preserving GitHub repo URLs (`github.com/robotocore/robotocore`) and GitHub Actions references (`uses: robotocore/robotocore/`).

**Issue #222: SES SendEmail API emails not appearing in /_robotocore/ses/messages**

Root cause: `EmailStore` is only fed by the SMTP server path. SES API calls (`SendEmail`, `SendRawEmail`, `SendTemplatedEmail`, `SendBulkTemplatedEmail`) fall through to Moto's backend, which stores emails in `backend.sent_messages`, a completely separate data structure.

Three approaches considered:
1. **Bridge Moto's store**: Query `backend.sent_messages` at the management endpoint. Con: Moto has 4+ message types (Message, RawMessage, TemplateMessage, BulkTemplateMessage) with different field names; translation is fragile against Moto version changes.
2. **Intercept before Moto**: Extract email data from request params and add to EmailStore before forwarding. Con: If Moto rejects the email (unverified sender), we'd capture failed sends.
3. **Intercept after Moto** (chosen): Let Moto process the email normally, capture it into EmailStore only on HTTP 200 response. Covers all send operations. Correct (only captures successful sends). Doesn't require touching Moto internals.

Applied the same pattern to SES v2 (`SendEmail` at POST `/v2/email/outbound-emails`).

Also added `source` field to `StoredEmail` ("smtp" vs "api") so callers can distinguish how the email was sent.

**Issue #212: Node.js lambda support**

This is a larger feature. Node.js runtimes (nodejs18.x, nodejs20.x, nodejs22.x) require executing JavaScript in a Node.js subprocess rather than in Python. The existing Lambda executor already handles Python by executing code in a subprocess with the right runtime environment. Node.js support requires: (1) ensuring Node.js is installed in the Docker image, (2) adding a Node.js function executor path alongside the Python one, (3) handling the Lambda runtime API in a way compatible with Node.js Lambda bootstrap.

Key decision: Node.js Lambdas use the Lambda Runtime API (`/2018-06-01/runtime/...` endpoints) for a pull-based invocation model. The existing Python executor simulates this. The same approach works for Node.js by launching `node` with a compatible bootstrap.
