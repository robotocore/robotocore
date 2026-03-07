# Skill: Add Compatibility Tests for an AWS Service

Use this skill when writing tests that verify robotocore matches LocalStack behavior for a specific AWS service.

## Process

1. **Identify the full API surface:**
   ```python
   import botocore.session
   s = botocore.session.get_session()
   m = s.get_service_model('SERVICE_NAME')
   for op in sorted(m.operation_names):
       print(op)
   ```

2. **Check what LocalStack community supports:**
   - Read `services/{service}/provider.py`
   - Methods that raise `NotImplementedError` or are missing are not supported
   - Methods decorated with `@handler` are implemented

3. **Create the test file:**
   - File: `tests/compatibility/test_{service}_compat.py`
   - Use `ENDPOINT_URL` env var so tests run against both robotocore and LocalStack
   - Use boto3 clients, not raw HTTP — we care about SDK-level compatibility

4. **Test structure:**
   - One test class per logical group of operations
   - Each test should be independent (create its own resources, clean up after)
   - Use fixtures for shared setup (client creation, resource creation)
   - Test both success and error paths

5. **Priority order for test coverage:**
   - CRUD operations (Create, Describe/Get, Update, Delete)
   - List operations with pagination
   - Error cases (resource not found, invalid input, duplicate creation)
   - Tagging operations
   - Cross-service integration (if applicable in LocalStack community)

6. **Run against both targets:**
   ```bash
   # Against robotocore
   uv run pytest tests/compatibility/test_{service}_compat.py -v

   # Against LocalStack
   ENDPOINT_URL=http://localhost:4567 uv run pytest tests/compatibility/test_{service}_compat.py -v
   ```

7. **Mark tests by implementation status:**
   ```python
   @pytest.mark.skip(reason="Not yet implemented in robotocore")
   def test_advanced_feature(self):
       pass
   ```
