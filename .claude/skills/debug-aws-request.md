# Skill: Debug an AWS API Request

Use this skill when a request to robotocore is failing, returning wrong responses, or not routing correctly.

## Process

1. **Reproduce the issue with verbose logging:**
   ```bash
   # Run the rust server with elevated log level
   cargo run -p robotocore-rust -- --host 127.0.0.1 --port 4566 --log-level debug
   ```
   ```bash
   # In another terminal, make the failing request with boto3 debug:
   import boto3
   import logging
   logging.basicConfig(level=logging.DEBUG)
   boto3.set_stream_logger('')
   # ... make the call
   ```

2. **Check routing first:**
   - Is the request reaching the gateway? The server logs every request via `tracing`.
   - Is `route_to_service()` (src/router.rs) returning the correct service name?
   - Look at the Authorization header, X-Amz-Target, and URL path
   - Unit-test routing directly: the router and `extract_service` in src/core/server.rs are pure functions with harness tests

3. **Check the dispatch path:**
   - After routing, the gateway answers from (a) the core handler table `src/core/services/`, (b) rotocore backends via `rotocore::dispatch` (rotocore/src/services/), or (c) the generic 501 `NotImplemented` path
   - `rotocore::handles_service(name)` tells you whether (b) is armed
   - Unknown operations inside a service are that module's `match` fallthrough

4. **Check the audit log (what the twin actually received):**
   ```bash
   curl -s 'http://localhost:4566/_robotocore/audit?limit=50' | python3 -m json.tool | tail -50
   ```
   Every call is logged with service, operation, status code, account and region.

5. **Compare with the ground truth:**
   - Run the same request against a LocalStack/real AWS reference and diff the responses
   - `docker run --rm -p 4567:4566 localstack/localstack`
   - `probes/<service>.json` records which operations were verified working locally

6. **Check botocore's service spec (the wire contract):**
   - The protocol type matters (query, json, rest-json, rest-xml, ec2)
   - Different protocols encode requests differently
   ```python
   import botocore.session
   s = botocore.session.get_session()
   m = s.get_service_model('SERVICE_NAME')
   print(m.protocol)  # Shows the protocol type
   ```

7. **Fix and test:**
   - Write a failing test first (rust unit test next to the handler, or a compatibility test)
   - Fix the issue
   - `cargo test -p robotocore-rust --lib -p rotocore --lib`
   - Run the affected compatibility suite: `make compat-test`
