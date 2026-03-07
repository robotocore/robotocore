# Skill: Implement an AWS Service Provider

Use this skill when implementing a new AWS service in robotocore or extending an existing one.

## Process

1. **Research the service in both vendor submodules:**
   - Read `vendor/moto/moto/{service}/models.py` to understand Moto's implementation
   - Read `vendor/moto/moto/{service}/responses.py` to understand request/response handling
   - Read `vendor/moto/moto/{service}/urls.py` to understand URL routing
   - Read `services/{service}/` to understand LocalStack's implementation
   - Note any places where LocalStack extends beyond what Moto provides

2. **Check botocore specs:**
   - Look at the botocore service model for the full API surface
   - Run: `python -c "import botocore.session; s=botocore.session.get_session(); m=s.get_service_model('{service}'); print(sorted(m.operation_names))"`

3. **Update the gateway router if needed:**
   - If the service uses a unique routing mechanism (special headers, path patterns), add it to `src/robotocore/gateway/router.py`
   - Add tests for the new routing rules in `tests/unit/gateway/test_router.py`

4. **Create a service extension if Moto is insufficient:**
   - Only create `src/robotocore/services/{service}/` if Moto's implementation is missing behavior that LocalStack provides
   - The moto_bridge should handle most services automatically
   - Extensions should wrap Moto, not replace it
   - Register native providers in `src/robotocore/gateway/app.py` NATIVE_PROVIDERS dict

5. **Access Moto backends correctly:**
   ```python
   from moto.backends import get_backend
   from moto.core import DEFAULT_ACCOUNT_ID
   backend = get_backend("service_name")[DEFAULT_ACCOUNT_ID][region]
   # For global services (IAM, S3):
   backend = get_backend("service_name")[DEFAULT_ACCOUNT_ID]["global"]
   ```
   **NEVER** use `from moto.backends import moto_backends` — it's been removed.

6. **Write compatibility tests:**
   - Create `tests/compatibility/test_{service}_compat.py`
   - Use the shared conftest: `from tests.compatibility.conftest import make_client`
   - Use unique resource names with `uuid.uuid4().hex[:8]` to avoid conflicts
   - Tests must be runnable against both robotocore AND LocalStack (use ENDPOINT_URL env var)

7. **Update the service registry:**
   - Add entry to `src/robotocore/services/registry.py`

8. **Add CloudFormation resource type if applicable:**
   - Add create/delete handlers in `src/robotocore/services/cloudformation/resources.py`
   - Check Moto method signatures with `inspect.signature()` — they change between versions

9. **Run the full test suite:**
   - `uv run pytest tests/unit/` must pass
   - `uv run pytest tests/compatibility/test_{service}_compat.py` must pass

## Template for compatibility tests

```python
"""${SERVICE} compatibility tests."""
import uuid
import pytest
from tests.compatibility.conftest import make_client

@pytest.fixture
def client():
    return make_client("${service}")

def _uid():
    return uuid.uuid4().hex[:8]

class Test${Service}Operations:
    def test_basic_crud(self, client):
        # Create, Read, Update, Delete
        pass
```
