# Worker 1 — Test Harness (scripts/harness + golden capture/diff tooling)

You are W1 of 3 parallel workers porting robotocore (Python, src/robotocore/) to Rust.
Read /Users/jackdanger/www/robotocore-rust/tooling/briefs-golden-2026-08-19.md first.

## Your job
Build the tooling that makes the Rust port verifiable, in a NEW directory `scripts/harness/` (do not modify existing scripts). Python 3.14 via `.venv/bin/python` or `uv run python`.

### 1. `scripts/harness/capture.py`
Golden capture tool. CLI: `capture.py --services sts,s3,sqs [--extra op=op] [--out FILE]`
- Uses boto3 against http://localhost:4566 (session: access key 123456789012 = account, secret "test", region us-east-1)
- For each service, run a curated op list: happy paths (create/get/list/delete one resource end-to-end) + 1-2 error cases (not-found, validation error)
- Records for each call: operation, HTTP method, path, query, request headers (normalize AWS Signature headers to literal `SigV4: present`), request body (bytes -> base64), status code, response body (raw, via botocore `before-call`/`after-call` events or a custom `botocore.awsrequest.AWSHTTPConnection` — capture RAW response bytes, not boto3-parsed), response headers
- To capture raw request+response, the cleanest approach: register `before-send` for request and `after-call` for parsed response, AND subclass/patch `botocore.httpsession` or use `after-send` event with the raw response object. `after-send` gives you (request, response) where response has .status_code, .headers, .content. Use that.
- Output: JSON file, stable order, deterministic (strip volatile headers: Date, x-amz-request-id, x-amz-id-2; strip volatile response fields: RequestId, hostId, and any *Arn/*Id containing uuids is fine to keep — but normalize `Expiry`/timestamps? NO — keep them, the diff tool handles volatility)
- Include `volatile` markers in a sidecar: each capture entry may list `volatile_response_paths` (list of JSON paths like `response.Expires`) that the differ should ignore.

### 2. `scripts/harness/diff.py`
`diff.py BASELINE CANDIDATE [--ignore path1,path2]` — compares two capture files:
- Match entries by (service, op, seq)
- For each: compare HTTP method/path/status; compare response bodies with JSON-path-aware diff (S3 responses are XML — compare as XML: parse both, canonicalize, diff; if either unparseable, fall back to text diff). Normalize volatile paths first.
- Print human-readable report: PASS/FAIL per op with first differing path. Exit code 1 if any FAIL.
- `--report FILE` writes a machine-readable JSON summary.

### 3. `scripts/harness/golden.sh`
`golden.sh capture|diff [--candidate URL]`
- capture: run capture.py for the service set, save to `golden/<svc>/` under repo (add `golden/` to .gitignore)
- diff: capture against candidate URL (default http://localhost:4567) and diff vs stored golden
### 4. `scripts/harness/README.md` — usage.

### 5. Verify end-to-end RIGHT NOW:
- Run capture against the live Python server for sts,s3,sqs
- Then diff the file against itself (must be 0 failures)
- Then diff against a mutated copy (change one status code; must detect exactly that op)

Report file: /Users/jackdanger/www/robotocore-rust/tooling/report-w1.md (what you built, how to use, verification output)
