# Multi-Tenant SaaS Platform

A simulated B2B SaaS analytics platform (think Mixpanel or Amplitude) where
each customer is an isolated **tenant** with their own data, configuration,
credentials, file storage, and usage quotas.

## Architecture

```
Tenants ──► SaaSPlatform ──┬── DynamoDB   (tenant data, partition-key isolation)
                           ├── S3          (per-tenant file storage, prefix isolation)
                           ├── SSM         (per-tenant config & feature flags)
                           ├── Secrets Mgr (per-tenant DB credentials / API keys)
                           ├── SQS         (async onboarding task queue)
                           └── CloudWatch  (per-tenant usage metrics & quotas)
```

## Tenant isolation model

Every DynamoDB item is keyed by `(tenant_id, entity_key)`.  Queries always
scope on the partition key so tenant A's data is invisible to tenant B.

S3 uses prefix isolation: all of tenant A's files live under `tenant-a/`.
SSM parameters live under `/saas/<namespace>/tenant-a/`.

## Plan tiers & quotas

| Plan       | Max Storage | Max Users | API calls/day | Features                                      |
|------------|-------------|-----------|---------------|-----------------------------------------------|
| free       | 100 MB      | 3         | 1,000         | billing                                       |
| starter    | 1 GB        | 10        | 10,000        | billing, reports                               |
| pro        | 10 GB       | 50        | 100,000       | billing, reports, api_access, sso             |
| enterprise | 100 GB      | 500       | 1,000,000     | billing, reports, api_access, sso, audit, custom_branding |

## How to run

```bash
# Start the emulator
make start

# Run just these tests
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/multitenant_saas/ -v

# Or run the full app test suite
make compat-test -k multitenant_saas
```

## File layout

| File                      | Purpose                                      |
|---------------------------|----------------------------------------------|
| `app.py`                  | `SaaSPlatform` class (all business logic)    |
| `models.py`               | Dataclasses: Tenant, TenantConfig, etc.      |
| `conftest.py`             | Shared fixtures (platform, tenant_a/b)       |
| `test_provisioning.py`    | Provision / deprovision lifecycle             |
| `test_isolation.py`       | Cross-tenant data isolation                  |
| `test_data_operations.py` | CRUD, queries, bulk writes                   |
| `test_configuration.py`   | SSM config, feature flags, plan migration    |
| `test_usage_and_quotas.py`| CloudWatch metrics & quota enforcement       |
| `test_onboarding.py`      | SQS onboarding task queue                    |
