# Skill: Debug an AWS API Request

Use this skill when a request to robotocore is failing, returning wrong responses, or not routing correctly.

## Process

1. **Reproduce the issue with verbose logging:**
   ```bash
   # Enable debug logging
   ROBOTOCORE_DEBUG=1 uv run python -m robotocore.main
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
   - Is the request reaching the gateway? Check uvicorn logs.
   - Is `route_to_service()` returning the correct service name?
   - Look at the Authorization header, X-Amz-Target, and URL path
   - Add temporary debug logging to `src/robotocore/gateway/router.py` if needed

3. **Check the Moto bridge:**
   - Is `forward_to_moto()` finding the right backend?
   - Is the region/account extraction correct?
   - Add logging to `src/robotocore/providers/moto_bridge.py`

4. **Compare with LocalStack:**
   - Run the same request against LocalStack to see the expected response
   - `docker run --rm -p 4567:4566 localstack/localstack`
   - Diff the responses (headers, body, status code)

5. **Compare with Moto standalone:**
   - Run the same request against Moto's server mode
   - `uv run moto_server -p 5555`
   - This isolates whether the issue is in our gateway/routing or in Moto itself

6. **Check botocore's service spec:**
   - The protocol type matters (query, json, rest-json, rest-xml, ec2)
   - Different protocols encode requests differently
   ```python
   import botocore.session
   s = botocore.session.get_session()
   m = s.get_service_model('SERVICE_NAME')
   print(m.protocol)  # Shows the protocol type
   ```

7. **Fix and test:**
   - Write a failing test first
   - Fix the issue
   - Verify the test passes
   - Run the full test suite to check for regressions
