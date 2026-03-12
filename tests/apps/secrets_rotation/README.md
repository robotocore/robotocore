# Secrets Rotation Application

A secrets management platform for a microservices deployment, handling database credentials, API keys, and TLS certificates across multiple environments.

## What

`SecretsVault` provides a high-level SDK for managing secrets across dev/staging/prod namespaces with:

- **Multi-environment isolation**: Secrets are namespaced (dev/, staging/, prod/) and isolated from each other
- **Schema validation**: Secret values are validated against templates (db_credentials, api_key, certificate, or custom)
- **Rotation management**: Scheduled and emergency rotation with full history tracking
- **Audit logging**: Every secret access is logged with accessor identity, timestamp, and version accessed
- **Resource policies**: Simulated IAM-style allow/deny policies per secret
- **Bulk operations**: Rotate all secrets in a namespace in one call
- **Cross-environment copy**: Promote secrets from dev to staging to prod

## Architecture

```
Applications
     |
     v
SecretsVault (app.py)
     |
     +---> AWS Secrets Manager
     |       - Secret storage
     |       - Versioning (AWSCURRENT / AWSPREVIOUS)
     |       - Tag-based organization
     |       - Scheduled deletion & restoration
     |
     +---> AWS DynamoDB
             - Audit log (who accessed what, when)
             - Rotation history (old version -> new version)
             - Resource policies (allow/deny principals)
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| **Secrets Manager** | Secret storage, versioning, tag management, deletion/restoration |
| **DynamoDB** | Audit trail, rotation history, policy storage |

## Security Patterns

- **Rotation scheduling**: Track last rotation, calculate next due date, detect overdue secrets
- **Audit logging**: Every `get_secret()` call writes an audit entry with accessor identity
- **Schema validation**: Secrets must match their type's template before storage
- **Namespace isolation**: dev/staging/prod secrets are fully separate
- **Resource policies**: Principals can be explicitly allowed or denied access
- **Emergency rotation**: Force-rotate with new credentials, tag secret with incident metadata

## How to Run

```bash
# Start the server
make start

# Run all secrets rotation tests
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/secrets_rotation/ -v

# Run specific test module
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/secrets_rotation/test_rotation.py -v
```
