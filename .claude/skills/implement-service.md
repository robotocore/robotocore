# Skill: Implement an AWS Service Provider (Rust)

Use this skill when porting an AWS service to rotocore or extending an existing backend.

## Process

1. **Check the wire contract:**
   - Read the botocore service model for the API surface and shapes:
     ```python
     python -c "import botocore.session; s=botocore.session.get_session(); m=s.get_service_model('{service}'); print(m.protocol); print(sorted(m.operation_names))"
     ```
   - `probes/{service}.json` records which operations were already verified working

2. **Write the backend in rotocore:**
   - New module: `rotocore/src/services/{service}/` (mod.rs with a `dispatch(req: &Request) -> Option<Response>`)
   - Wire names must match botocore's service-2.json exactly; state lives per account + per region

3. **Arm it in the dispatch:**
   - Add the arm to `rotocore/src/services/mod.rs::dispatch`
   - Add the same name to `handles_service` (the two lists must stay in sync — spot-check test exists)
   - If the service needs a gateway-level handler instead, add it to `src/core/services/` and `ServiceRegistry`

4. **Write compatibility tests:**
   - Create or extend `tests/compatibility/test_{service}_compat.py`
   - Use shared fixtures: `from tests.compatibility.conftest import make_client`
   - Use unique resource names with `uuid.uuid4().hex[:8]` to avoid conflicts
   - Tests must be runnable against robotocore and a reference implementation (use `ENDPOINT_URL` env var)

5. **Run the gates:**
   ```bash
   cargo test --workspace --lib          # rust unit tests
   cargo clippy --workspace --all-targets -- -D warnings
   cargo fmt
   make compat-test                      # compatibility suite (auto-starts the server)
   ```

## Rules (learned from experience)

- **Never write tests for unverified operations.** Probe first (`uv run python scripts/probe_service.py --service {service}`), then write tests only for operations that actually work.
- **Every test MUST contact the server.** A test catching client-side `ParamValidationError` proves nothing.
- **Every test MUST assert something.** At minimum assert on a response key.
- **501 = not implemented.** Call an op, get `501 NotImplemented`, and the operation is absent — delete the test for now; the server is the thing to fix.
- **Port state becomes wrong when you see old labels:** `/_robotocore/services` reports `native` (rust backend armed) or `absent` (would 501) — trust it over comments in the registry table.
