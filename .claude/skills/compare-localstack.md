# Skill: Compare robotocore Behavior with LocalStack

Use this skill when verifying that robotocore produces identical responses to LocalStack for a given AWS API operation.

## Process

1. **Start both services:**
   ```bash
   # Terminal 1: robotocore
   uv run python -m robotocore.main  # port 4566

   # Terminal 2: localstack (on different port)
   docker run --rm -p 4567:4566 localstack/localstack
   ```

2. **Capture responses from both:**
   ```python
   import boto3
   import json

   def make_client(service, port):
       return boto3.client(
           service,
           endpoint_url=f"http://localhost:{port}",
           region_name="us-east-1",
           aws_access_key_id="testing",
           aws_secret_access_key="testing",
       )

   roboto = make_client("SERVICE", 4566)
   localstack = make_client("SERVICE", 4567)

   # Make identical calls and compare
   r1 = roboto.OPERATION(...)
   r2 = localstack.OPERATION(...)

   # Remove metadata for comparison
   del r1["ResponseMetadata"]
   del r2["ResponseMetadata"]

   print(json.dumps(r1, indent=2, default=str))
   print(json.dumps(r2, indent=2, default=str))
   ```

3. **Key things to compare:**
   - Response body structure (field names, nesting)
   - Data types (strings vs numbers, date formats)
   - Error responses (error codes, messages, HTTP status)
   - HTTP headers (especially Content-Type, x-amzn-RequestId)
   - Pagination behavior (NextToken, MaxResults)
   - Default values for optional fields

4. **Check the vendor source for differences:**
   - Read `services/{service}/` for LocalStack's provider
   - Read `vendor/moto/moto/{service}/` for Moto's implementation
   - Look for places where LocalStack wraps or overrides Moto behavior

5. **Write compatibility tests for any differences found:**
   - Add tests to `tests/compatibility/test_{service}_compat.py`
   - Tests should capture the exact expected behavior
   - Use `pytest.mark.parametrize` for testing multiple cases

6. **Document significant behavioral differences:**
   - If Moto and LocalStack genuinely differ, document which behavior robotocore follows and why
