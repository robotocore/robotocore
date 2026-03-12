---
session: "a168fa9f"
timestamp: "2026-03-12T07:00:00Z"
model: claude-opus-4-6
---

## Human

Implement Lambda Function URL routing in the gateway. Function URLs are created via the Lambda API but the gateway never actually routes HTTP requests to them.

Research the existing function URL CRUD in provider.py and urls.py, then add routing in the gateway middleware to detect function URL requests and invoke the target Lambda with the API Gateway v2 payload format.

## Assistant

## Key decisions

**Routing approach**: Added path-based routing (`/lambda-url/{url-id}/...`) in `AWSRoutingMiddleware`, placed after the S3 website check and before the S3 vhost rewrite. Also supports Host header matching (`{url-id}.lambda-url.{region}.on.aws`). Path-based is the primary mechanism for local testing since Host header manipulation is harder without DNS.

**Event format**: Builds the API Gateway v2 payload format with `version: "2.0"`, `requestContext.http.method`, `rawPath`, `rawQueryString`, `queryStringParameters`, `headers`, `body`, and `isBase64Encoded`. This matches what AWS sends to Lambda functions invoked via function URLs.

**Response handling**: Supports both structured responses (with `statusCode`, `body`, `headers`) and simple return values. Structured responses map directly to HTTP responses; simple values are JSON-serialized with 200 status.

**CORS**: Reads the CORS config from the function URL config and applies the appropriate `access-control-*` headers. Supports wildcard and specific origin matching.

**Architecture**: Created `src/robotocore/gateway/lambda_url.py` as a self-contained module (like `s3_routing.py` and the API Gateway execution handlers). Reads directly from the `_url_configs` store in `services/lambda_/urls.py` to find configs by URL ID.

**Files changed**:
- `src/robotocore/gateway/lambda_url.py` (new) — function URL detection, event building, invocation
- `src/robotocore/gateway/app.py` — import + routing in AWSRoutingMiddleware
- `tests/compatibility/test_lambda_function_urls_compat.py` (new) — 9 compat tests, 100% effective
