---
session: "agent-af649aff"
timestamp: "2026-03-10T12:00:00Z"
model: claude-opus-4-6
---

## Human

Implement S3 virtual-hosted-style routing and static website hosting for robotocore. Requirements include parsing Host headers like `mybucket.s3.localhost.localstack.cloud` and `mybucket.s3-website-us-east-1.amazonaws.com`, rewriting to path-style internally, serving index/error documents, handling redirect rules, and configurable hostname via S3_HOSTNAME env var. Write tests first as failing tests, then implement.

## Assistant

Implemented in three layers:

1. **s3_routing.py** — Host header parsing for virtual-hosted S3 requests. Handles `<bucket>.s3.<hostname>`, `<bucket>.s3.<region>.amazonaws.com`, dualstack patterns, and custom S3_HOSTNAME env var. Rewrites ASGI scope to path-style while preserving query strings and headers.

2. **website.py** — S3 static website serving. Parses `<bucket>.s3-website[-.]<rest>` Host headers. Reads website configuration from Moto's S3 backend (stored as XML bytes), serves index documents for directory-like paths, error documents on 404s, and evaluates redirect rules (prefix-based, key-based, error-code-based, with custom status codes and hostname redirects). Content-Type detection via mimetypes + custom map.

3. **app.py integration** — Added S3 routing checks in AWSRoutingMiddleware.__call__ BEFORE normal AWS routing. Website requests are served directly; virtual-hosted requests get their scope rewritten to path-style and continue through normal routing. Added `/_robotocore/s3/routing` management endpoint.

Key decisions:
- Moto stores website_configuration as raw XML bytes — parsed with ElementTree, handling both namespaced and non-namespaced tags
- S3 backend in Moto uses 'aws' region key (global), not actual region names — backend lookup tries multiple keys
- Redirect rules checked both before serving (prefix-based) and after 404 (error-code-based)
- 67 tests across 3 files: 22 routing unit tests, 34 website unit tests, 11 integration tests using mock_aws
