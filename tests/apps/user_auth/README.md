# User Authentication & Identity Service

A realistic multi-service AWS application simulating a user authentication platform
(like Auth0 or Firebase Auth), built entirely with boto3 against an AWS-compatible endpoint.

## Architecture

```
Client → AuthService → DynamoDB (users, sessions, reset tokens)
                     → S3 (avatars)
                     → Secrets Manager (JWT keys, OAuth credentials)
                     → SSM Parameter Store (auth config)
                     → CloudWatch (login metrics)
                     → CloudWatch Logs (audit events)
```

## AWS Services Used

| Service | Purpose |
|---------|---------|
| **DynamoDB** | User profiles (with email GSI), sessions (with TTL + user GSI), password reset tokens (with TTL) |
| **S3** | Avatar image storage with presigned URL generation |
| **Secrets Manager** | JWT signing keys (with rotation), OAuth client credentials per provider |
| **SSM Parameter Store** | Auth configuration (password policy, rate limits, token expiry) |
| **CloudWatch** | Login success/failure metrics with dimensions |
| **CloudWatch Logs** | Audit trail for login, logout, password change, and security events |

## Security Model

- **Password hashing**: SHA-256 with per-user random salt (simplified bcrypt pattern)
- **Session TTL**: Sessions expire via DynamoDB TTL, configurable via SSM
- **Rate limiting**: Failed login tracking with automatic account lockout after N failures
- **Password policy**: Minimum length + special character requirements, enforced at registration and reset
- **Audit logging**: All security-relevant events logged to CloudWatch Logs
- **Soft delete**: Users are marked as deleted, not removed (for audit trail)

## How to Run

```bash
# Start the server
make start

# Run these tests
AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/user_auth/ -v

# Or against real AWS (with credentials configured)
AWS_ENDPOINT_URL= pytest tests/apps/user_auth/ -v
```

## Test Modules

| File | Coverage |
|------|----------|
| `test_registration.py` | User signup, email uniqueness, password hashing, defaults |
| `test_login.py` | Authentication, metrics, rate limiting, lockout |
| `test_sessions.py` | Session CRUD, TTL expiry, revocation, concurrency |
| `test_profiles.py` | Profile CRUD, avatar management, search by email/role |
| `test_passwords.py` | Policy enforcement, change flow, reset token lifecycle |
| `test_secrets.py` | JWT key management, rotation, OAuth credentials |
| `test_audit.py` | CloudWatch Logs audit trail for security events |
