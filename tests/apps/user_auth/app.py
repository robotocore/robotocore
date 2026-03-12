"""
AuthService — User Authentication & Identity Service.

A realistic multi-service AWS application that manages user registration,
login, sessions, avatars, secrets, configuration, metrics, and audit logging.

Uses: DynamoDB (users, sessions, reset tokens), S3 (avatars),
Secrets Manager (JWT keys, OAuth creds), SSM (auth config),
CloudWatch (metrics), CloudWatch Logs (audit).

NO robotocore/moto imports — only boto3 and stdlib.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import AuthConfig, PasswordResetToken, Session, User, UserStats


class AuthServiceError(Exception):
    """Base exception for AuthService errors."""


class DuplicateEmailError(AuthServiceError):
    """Raised when a user tries to register with an already-used email."""


class InvalidCredentialsError(AuthServiceError):
    """Raised when login credentials are wrong."""


class AccountLockedError(AuthServiceError):
    """Raised when the account is locked due to too many failed attempts."""


class PasswordPolicyError(AuthServiceError):
    """Raised when a password doesn't meet the policy requirements."""


class InvalidEmailError(AuthServiceError):
    """Raised when an email address is malformed."""


class SessionExpiredError(AuthServiceError):
    """Raised when a session has expired."""


class TokenExpiredError(AuthServiceError):
    """Raised when a password reset token has expired or been used."""


class UserNotFoundError(AuthServiceError):
    """Raised when a user lookup fails."""


# ---------------------------------------------------------------------------
# Email validation
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


def _validate_email(email: str) -> None:
    if not _EMAIL_RE.match(email):
        raise InvalidEmailError(f"Invalid email format: {email}")


# ---------------------------------------------------------------------------
# Password hashing (SHA-256 + salt, bcrypt-style but simpler)
# ---------------------------------------------------------------------------


def _generate_salt() -> str:
    return uuid.uuid4().hex


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()


# ---------------------------------------------------------------------------
# AuthService
# ---------------------------------------------------------------------------


class AuthService:
    """
    Orchestrates user auth workflows across five AWS services.

    Parameters
    ----------
    dynamodb : boto3 DynamoDB client
    s3 : boto3 S3 client
    secretsmanager : boto3 Secrets Manager client
    ssm : boto3 SSM client
    cloudwatch : boto3 CloudWatch client
    logs : boto3 CloudWatch Logs client
    users_table : DynamoDB table name for user profiles
    sessions_table : DynamoDB table name for sessions
    reset_tokens_table : DynamoDB table name for password reset tokens
    avatar_bucket : S3 bucket for avatars
    secrets_prefix : Secrets Manager name prefix (e.g. "auth/keys-xxx")
    ssm_prefix : SSM parameter path prefix (e.g. "/auth/xxx")
    metrics_namespace : CloudWatch metrics namespace
    audit_log_group : CloudWatch Logs log group for audit events
    """

    def __init__(
        self,
        *,
        dynamodb: Any,
        s3: Any,
        secretsmanager: Any,
        ssm: Any,
        cloudwatch: Any,
        logs: Any,
        users_table: str,
        sessions_table: str,
        reset_tokens_table: str,
        avatar_bucket: str,
        secrets_prefix: str,
        ssm_prefix: str,
        metrics_namespace: str,
        audit_log_group: str,
    ) -> None:
        self.dynamodb = dynamodb
        self.s3 = s3
        self.secretsmanager = secretsmanager
        self.ssm = ssm
        self.cloudwatch = cloudwatch
        self.logs = logs

        self.users_table = users_table
        self.sessions_table = sessions_table
        self.reset_tokens_table = reset_tokens_table
        self.avatar_bucket = avatar_bucket
        self.secrets_prefix = secrets_prefix
        self.ssm_prefix = ssm_prefix
        self.metrics_namespace = metrics_namespace
        self.audit_log_group = audit_log_group

        # In-memory cache for failed login tracking (would be Redis in prod)
        self._failed_attempts: dict[str, list[float]] = {}

    # -----------------------------------------------------------------------
    # Configuration
    # -----------------------------------------------------------------------

    def load_auth_config(self) -> AuthConfig:
        """Load auth configuration from SSM Parameter Store."""
        resp = self.ssm.get_parameters_by_path(Path=self.ssm_prefix, Recursive=True)
        raw: dict[str, str] = {}
        for p in resp["Parameters"]:
            key = p["Name"].split("/")[-1]
            raw[key] = p["Value"]

        return AuthConfig(
            token_expiry_hours=int(raw.get("token_expiry_hours", "24")),
            max_failed_attempts=int(raw.get("max_failed_attempts", "5")),
            lockout_duration_minutes=int(raw.get("lockout_duration_minutes", "30")),
            min_password_length=int(raw.get("min_password_length", "12")),
            require_special_chars=raw.get("require_special_chars", "true").lower() == "true",
        )

    def update_auth_config(self, key: str, value: str) -> None:
        """Update a single auth config parameter in SSM."""
        param_name = f"{self.ssm_prefix}/{key}"
        self.ssm.put_parameter(Name=param_name, Value=value, Type="String", Overwrite=True)

    # -----------------------------------------------------------------------
    # User Registration
    # -----------------------------------------------------------------------

    def register_user(
        self,
        email: str,
        password: str,
        name: str = "",
        bio: str = "",
        role: str = "user",
    ) -> User:
        """
        Register a new user.

        1. Validate email format
        2. Enforce password policy
        3. Check email uniqueness via GSI
        4. Hash password with salt
        5. Store in DynamoDB
        6. Publish signup metric
        7. Audit log the registration
        """
        _validate_email(email)

        config = self.load_auth_config()
        self._enforce_password_policy(password, config)

        # Check email uniqueness via GSI query
        existing = self.dynamodb.query(
            TableName=self.users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": email}},
        )
        if existing["Count"] > 0:
            raise DuplicateEmailError(f"Email already registered: {email}")

        user_id = f"user-{uuid.uuid4().hex[:12]}"
        salt = _generate_salt()
        password_hash = _hash_password(password, salt)
        now = datetime.now(UTC).isoformat()

        user = User(
            user_id=user_id,
            email=email,
            password_hash=password_hash,
            salt=salt,
            name=name,
            bio=bio,
            role=role,
            status="active",
            created_at=now,
            updated_at=now,
        )

        self.dynamodb.put_item(
            TableName=self.users_table,
            Item={
                "user_id": {"S": user.user_id},
                "email": {"S": user.email},
                "password_hash": {"S": user.password_hash},
                "salt": {"S": user.salt},
                "name": {"S": user.name},
                "bio": {"S": user.bio},
                "role": {"S": user.role},
                "status": {"S": user.status},
                "created_at": {"S": user.created_at},
                "updated_at": {"S": user.updated_at},
            },
            ConditionExpression="attribute_not_exists(user_id)",
        )

        self._publish_metric("Signups", 1)
        self._audit("registration", user_id, {"email": email})

        return user

    # -----------------------------------------------------------------------
    # Login
    # -----------------------------------------------------------------------

    def login(
        self,
        email: str,
        password: str,
        ip_address: str = "",
        user_agent: str = "",
    ) -> Session:
        """
        Authenticate user and create a session.

        1. Look up user by email (GSI)
        2. Check account not locked
        3. Check rate limiting
        4. Verify password hash
        5. Create session with TTL
        6. Publish login metric
        7. Audit log
        """
        # Look up user by email
        resp = self.dynamodb.query(
            TableName=self.users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": email}},
        )
        if resp["Count"] == 0:
            self._publish_metric("LoginFailures", 1)
            raise InvalidCredentialsError("Invalid email or password")

        item = resp["Items"][0]
        user_id = item["user_id"]["S"]
        status = item.get("status", {}).get("S", "active")

        # Check if account is locked
        if status == "locked":
            self._publish_metric("LoginFailures", 1)
            raise AccountLockedError(f"Account is locked: {user_id}")

        # Check rate limiting
        config = self.load_auth_config()
        if self._is_rate_limited(user_id, config):
            # Lock the account
            self._lock_account(user_id)
            self._publish_metric("LoginFailures", 1)
            self._audit("account_locked", user_id, {"reason": "too_many_failures"})
            raise AccountLockedError(f"Account locked due to too many failed attempts: {user_id}")

        # Verify password
        stored_hash = item["password_hash"]["S"]
        salt = item["salt"]["S"]
        candidate_hash = _hash_password(password, salt)

        if candidate_hash != stored_hash:
            self._record_failed_attempt(user_id)
            self._publish_metric("LoginFailures", 1)
            self._audit(
                "login_failed",
                user_id,
                {"ip_address": ip_address, "reason": "wrong_password"},
            )
            raise InvalidCredentialsError("Invalid email or password")

        # Clear failed attempts on success
        self._failed_attempts.pop(user_id, None)

        # Create session
        session = self._create_session(user_id, config, ip_address, user_agent)

        self._publish_metric("LoginSuccess", 1)
        self._audit("login", user_id, {"session_id": session.session_id, "ip_address": ip_address})

        return session

    # -----------------------------------------------------------------------
    # Session Management
    # -----------------------------------------------------------------------

    def _create_session(
        self,
        user_id: str,
        config: AuthConfig,
        ip_address: str = "",
        user_agent: str = "",
    ) -> Session:
        """Create a new session in DynamoDB with TTL."""
        session_id = f"sess-{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC)
        expires_at = int(now.timestamp()) + (config.token_expiry_hours * 3600)

        session = Session(
            session_id=session_id,
            user_id=user_id,
            created_at=now.isoformat(),
            expires_at=expires_at,
            ip_address=ip_address,
            user_agent=user_agent,
        )

        self.dynamodb.put_item(
            TableName=self.sessions_table,
            Item={
                "session_id": {"S": session.session_id},
                "user_id": {"S": session.user_id},
                "created_at": {"S": session.created_at},
                "expires_at": {"N": str(session.expires_at)},
                "ip_address": {"S": session.ip_address},
                "user_agent": {"S": session.user_agent},
            },
        )

        return session

    def validate_session(self, session_id: str) -> Session:
        """
        Validate that a session exists and hasn't expired.

        Returns the Session if valid, raises SessionExpiredError otherwise.
        """
        resp = self.dynamodb.get_item(
            TableName=self.sessions_table,
            Key={"session_id": {"S": session_id}},
        )
        if "Item" not in resp:
            raise SessionExpiredError(f"Session not found: {session_id}")

        item = resp["Item"]
        expires_at = int(item["expires_at"]["N"])
        now = int(time.time())

        if now >= expires_at:
            # Clean up expired session
            self.dynamodb.delete_item(
                TableName=self.sessions_table,
                Key={"session_id": {"S": session_id}},
            )
            raise SessionExpiredError(f"Session expired: {session_id}")

        return Session(
            session_id=item["session_id"]["S"],
            user_id=item["user_id"]["S"],
            created_at=item["created_at"]["S"],
            expires_at=expires_at,
            ip_address=item.get("ip_address", {}).get("S", ""),
            user_agent=item.get("user_agent", {}).get("S", ""),
        )

    def revoke_session(self, session_id: str, user_id: str | None = None) -> None:
        """Revoke (delete) a session. Optionally verify it belongs to user_id."""
        if user_id:
            resp = self.dynamodb.get_item(
                TableName=self.sessions_table,
                Key={"session_id": {"S": session_id}},
            )
            if "Item" in resp and resp["Item"]["user_id"]["S"] != user_id:
                raise AuthServiceError("Session does not belong to user")

        self.dynamodb.delete_item(
            TableName=self.sessions_table,
            Key={"session_id": {"S": session_id}},
        )
        if user_id:
            self._audit("logout", user_id, {"session_id": session_id})

    def list_user_sessions(self, user_id: str) -> list[Session]:
        """List all active sessions for a user via GSI."""
        resp = self.dynamodb.query(
            TableName=self.sessions_table,
            IndexName="by-user",
            KeyConditionExpression="user_id = :uid",
            ExpressionAttributeValues={":uid": {"S": user_id}},
        )
        sessions = []
        now = int(time.time())
        for item in resp.get("Items", []):
            expires_at = int(item["expires_at"]["N"])
            if now < expires_at:
                sessions.append(
                    Session(
                        session_id=item["session_id"]["S"],
                        user_id=item["user_id"]["S"],
                        created_at=item["created_at"]["S"],
                        expires_at=expires_at,
                        ip_address=item.get("ip_address", {}).get("S", ""),
                        user_agent=item.get("user_agent", {}).get("S", ""),
                    )
                )
        return sessions

    def revoke_all_sessions(self, user_id: str) -> int:
        """Revoke all sessions for a user. Returns count of revoked sessions."""
        sessions = self.list_user_sessions(user_id)
        for s in sessions:
            self.dynamodb.delete_item(
                TableName=self.sessions_table,
                Key={"session_id": {"S": s.session_id}},
            )
        self._audit("revoke_all_sessions", user_id, {"count": len(sessions)})
        return len(sessions)

    # -----------------------------------------------------------------------
    # Profile CRUD
    # -----------------------------------------------------------------------

    def get_user(self, user_id: str) -> User:
        """Get a user profile by user_id."""
        resp = self.dynamodb.get_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
        )
        if "Item" not in resp:
            raise UserNotFoundError(f"User not found: {user_id}")
        return self._item_to_user(resp["Item"])

    def get_user_by_email(self, email: str) -> User:
        """Look up a user by email via GSI."""
        resp = self.dynamodb.query(
            TableName=self.users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": email}},
        )
        if resp["Count"] == 0:
            raise UserNotFoundError(f"User not found with email: {email}")
        return self._item_to_user(resp["Items"][0])

    def update_profile(self, user_id: str, **fields: str) -> User:
        """
        Update user profile fields (name, bio, email).

        Email changes are checked for uniqueness.
        """
        allowed = {"name", "bio", "email"}
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not updates:
            return self.get_user(user_id)

        # If email is changing, check uniqueness
        if "email" in updates:
            _validate_email(updates["email"])
            existing = self.dynamodb.query(
                TableName=self.users_table,
                IndexName="by-email",
                KeyConditionExpression="email = :e",
                ExpressionAttributeValues={":e": {"S": updates["email"]}},
            )
            # Allow if no results or if the only result is the same user
            for item in existing.get("Items", []):
                if item["user_id"]["S"] != user_id:
                    raise DuplicateEmailError(f"Email already in use: {updates['email']}")

        now = datetime.now(UTC).isoformat()
        updates["updated_at"] = now

        expr_parts = []
        attr_values: dict[str, dict[str, str]] = {}
        attr_names: dict[str, str] = {}
        for i, (k, v) in enumerate(updates.items()):
            placeholder_val = f":v{i}"
            placeholder_name = f"#n{i}"
            expr_parts.append(f"{placeholder_name} = {placeholder_val}")
            attr_values[placeholder_val] = {"S": v}
            attr_names[placeholder_name] = k

        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET " + ", ".join(expr_parts),
            ExpressionAttributeValues=attr_values,
            ExpressionAttributeNames=attr_names,
            ConditionExpression="attribute_exists(user_id)",
        )

        return self.get_user(user_id)

    def soft_delete_user(self, user_id: str) -> None:
        """Soft-delete a user by setting status to DELETED."""
        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET #s = :status, updated_at = :now",
            ExpressionAttributeValues={
                ":status": {"S": "deleted"},
                ":now": {"S": now},
            },
            ExpressionAttributeNames={"#s": "status"},
        )
        self.revoke_all_sessions(user_id)
        self._audit("user_deleted", user_id, {})

    def search_users_by_email(self, email: str) -> list[User]:
        """Search users by exact email match via GSI."""
        resp = self.dynamodb.query(
            TableName=self.users_table,
            IndexName="by-email",
            KeyConditionExpression="email = :e",
            ExpressionAttributeValues={":e": {"S": email}},
        )
        return [self._item_to_user(item) for item in resp.get("Items", [])]

    def search_users_by_role(self, role: str) -> list[User]:
        """Search users by role via table scan with filter."""
        resp = self.dynamodb.scan(
            TableName=self.users_table,
            FilterExpression="#r = :role",
            ExpressionAttributeValues={":role": {"S": role}},
            ExpressionAttributeNames={"#r": "role"},
        )
        return [self._item_to_user(item) for item in resp.get("Items", [])]

    def search_users_by_status(self, status: str) -> list[User]:
        """Search users by status via table scan with filter."""
        resp = self.dynamodb.scan(
            TableName=self.users_table,
            FilterExpression="#s = :status",
            ExpressionAttributeValues={":status": {"S": status}},
            ExpressionAttributeNames={"#s": "status"},
        )
        return [self._item_to_user(item) for item in resp.get("Items", [])]

    # -----------------------------------------------------------------------
    # Avatar Management
    # -----------------------------------------------------------------------

    def upload_avatar(
        self,
        user_id: str,
        image_data: bytes,
        content_type: str = "image/jpeg",
    ) -> str:
        """Upload an avatar image to S3. Returns the S3 key."""
        key = f"avatars/{user_id}/profile.jpg"
        self.s3.put_object(
            Bucket=self.avatar_bucket,
            Key=key,
            Body=image_data,
            ContentType=content_type,
        )
        self._audit("avatar_uploaded", user_id, {"key": key, "size": len(image_data)})
        return key

    def get_avatar(self, user_id: str) -> bytes:
        """Download a user's avatar from S3."""
        key = f"avatars/{user_id}/profile.jpg"
        resp = self.s3.get_object(Bucket=self.avatar_bucket, Key=key)
        return resp["Body"].read()

    def get_avatar_presigned_url(self, user_id: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for the user's avatar."""
        key = f"avatars/{user_id}/profile.jpg"
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.avatar_bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def delete_avatar(self, user_id: str) -> None:
        """Delete a user's avatar from S3."""
        key = f"avatars/{user_id}/profile.jpg"
        self.s3.delete_object(Bucket=self.avatar_bucket, Key=key)
        self._audit("avatar_deleted", user_id, {"key": key})

    # -----------------------------------------------------------------------
    # JWT / OAuth Secret Management
    # -----------------------------------------------------------------------

    def get_jwt_secret(self) -> str:
        """Retrieve the JWT signing key from Secrets Manager."""
        secret_name = f"{self.secrets_prefix}/jwt"
        resp = self.secretsmanager.get_secret_value(SecretId=secret_name)
        data = json.loads(resp["SecretString"])
        return data["signing_key"]

    def rotate_jwt_secret(self, new_key: str) -> None:
        """Rotate the JWT signing key in Secrets Manager."""
        secret_name = f"{self.secrets_prefix}/jwt"
        resp = self.secretsmanager.get_secret_value(SecretId=secret_name)
        data = json.loads(resp["SecretString"])
        data["previous_key"] = data.get("signing_key", "")
        data["signing_key"] = new_key
        data["rotated_at"] = datetime.now(UTC).isoformat()
        self.secretsmanager.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(data),
        )
        self._audit("jwt_secret_rotated", "system", {})

    def store_jwt_secret(self, signing_key: str) -> None:
        """Store initial JWT signing key in Secrets Manager."""
        secret_name = f"{self.secrets_prefix}/jwt"
        data = {"signing_key": signing_key, "created_at": datetime.now(UTC).isoformat()}
        self.secretsmanager.create_secret(
            Name=secret_name,
            SecretString=json.dumps(data),
        )

    def store_oauth_credentials(self, provider: str, client_id: str, client_secret: str) -> None:
        """Store OAuth client credentials for a provider."""
        secret_name = f"{self.secrets_prefix}/oauth/{provider}"
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "provider": provider,
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.secretsmanager.create_secret(
            Name=secret_name,
            SecretString=json.dumps(data),
        )

    def get_oauth_credentials(self, provider: str) -> dict[str, str]:
        """Retrieve OAuth client credentials for a provider."""
        secret_name = f"{self.secrets_prefix}/oauth/{provider}"
        resp = self.secretsmanager.get_secret_value(SecretId=secret_name)
        return json.loads(resp["SecretString"])

    # -----------------------------------------------------------------------
    # Password Management
    # -----------------------------------------------------------------------

    def change_password(self, user_id: str, old_password: str, new_password: str) -> None:
        """
        Change a user's password.

        Verifies old password, enforces policy on new password, updates hash.
        """
        user = self.get_user(user_id)

        # Verify old password
        old_hash = _hash_password(old_password, user.salt)
        if old_hash != user.password_hash:
            raise InvalidCredentialsError("Current password is incorrect")

        # Enforce policy on new password
        config = self.load_auth_config()
        self._enforce_password_policy(new_password, config)

        # Generate new salt and hash
        new_salt = _generate_salt()
        new_hash = _hash_password(new_password, new_salt)
        now = datetime.now(UTC).isoformat()

        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET password_hash = :h, salt = :s, updated_at = :now",
            ExpressionAttributeValues={
                ":h": {"S": new_hash},
                ":s": {"S": new_salt},
                ":now": {"S": now},
            },
        )

        # Revoke all sessions on password change for security
        self.revoke_all_sessions(user_id)
        self._audit("password_changed", user_id, {})

    def generate_reset_token(self, email: str) -> PasswordResetToken:
        """
        Generate a password reset token for the user with the given email.

        Token is stored in the reset_tokens_table with a TTL.
        """
        # Verify user exists
        user = self.get_user_by_email(email)

        token_str = uuid.uuid4().hex
        now = datetime.now(UTC)
        self.load_auth_config()
        # Reset tokens expire in 1 hour (or config-driven)
        expires_at = int(now.timestamp()) + 3600

        token = PasswordResetToken(
            token=token_str,
            user_id=user.user_id,
            created_at=now.isoformat(),
            expires_at=expires_at,
            used=False,
        )

        self.dynamodb.put_item(
            TableName=self.reset_tokens_table,
            Item={
                "token": {"S": token.token},
                "user_id": {"S": token.user_id},
                "created_at": {"S": token.created_at},
                "expires_at": {"N": str(token.expires_at)},
                "used": {"BOOL": False},
            },
        )

        self._audit("reset_token_generated", user.user_id, {"email": email})
        return token

    def verify_reset_token(self, token_str: str) -> PasswordResetToken:
        """Verify a password reset token is valid (exists, not expired, not used)."""
        resp = self.dynamodb.get_item(
            TableName=self.reset_tokens_table,
            Key={"token": {"S": token_str}},
        )
        if "Item" not in resp:
            raise TokenExpiredError(f"Reset token not found: {token_str}")

        item = resp["Item"]
        expires_at = int(item["expires_at"]["N"])
        used = item["used"]["BOOL"]

        if used:
            raise TokenExpiredError("Reset token has already been used")

        if int(time.time()) >= expires_at:
            raise TokenExpiredError("Reset token has expired")

        return PasswordResetToken(
            token=item["token"]["S"],
            user_id=item["user_id"]["S"],
            created_at=item["created_at"]["S"],
            expires_at=expires_at,
            used=used,
        )

    def reset_password(self, token_str: str, new_password: str) -> None:
        """
        Reset a user's password using a valid reset token.

        Verifies token, enforces policy, updates password, marks token as used.
        """
        token = self.verify_reset_token(token_str)

        config = self.load_auth_config()
        self._enforce_password_policy(new_password, config)

        new_salt = _generate_salt()
        new_hash = _hash_password(new_password, new_salt)
        now = datetime.now(UTC).isoformat()

        # Update password
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": token.user_id}},
            UpdateExpression="SET password_hash = :h, salt = :s, updated_at = :now",
            ExpressionAttributeValues={
                ":h": {"S": new_hash},
                ":s": {"S": new_salt},
                ":now": {"S": now},
            },
        )

        # Mark token as used
        self.dynamodb.update_item(
            TableName=self.reset_tokens_table,
            Key={"token": {"S": token_str}},
            UpdateExpression="SET used = :u",
            ExpressionAttributeValues={":u": {"BOOL": True}},
        )

        self.revoke_all_sessions(token.user_id)
        self._audit("password_reset", token.user_id, {})

    def _enforce_password_policy(self, password: str, config: AuthConfig) -> None:
        """Raise PasswordPolicyError if password doesn't meet requirements."""
        if len(password) < config.min_password_length:
            raise PasswordPolicyError(
                f"Password must be at least {config.min_password_length} characters"
            )
        if config.require_special_chars:
            if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>/?]", password):
                raise PasswordPolicyError("Password must contain at least one special character")

    # -----------------------------------------------------------------------
    # Multi-Factor Auth (TOTP)
    # -----------------------------------------------------------------------

    def store_totp_secret(self, user_id: str, totp_secret: str) -> None:
        """Store a TOTP secret for a user in DynamoDB."""
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET totp_secret = :s, mfa_enabled = :m",
            ExpressionAttributeValues={
                ":s": {"S": totp_secret},
                ":m": {"BOOL": True},
            },
        )
        self._audit("mfa_enabled", user_id, {})

    def verify_totp(self, user_id: str, code: str) -> bool:
        """
        Verify a TOTP code for a user.

        In a real implementation this would compute the expected TOTP from the
        stored secret and compare. Here we store the expected code alongside
        the secret for testing purposes.
        """
        resp = self.dynamodb.get_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
        )
        if "Item" not in resp:
            raise UserNotFoundError(f"User not found: {user_id}")
        item = resp["Item"]
        stored_secret = item.get("totp_secret", {}).get("S", "")
        if not stored_secret:
            return False
        # Simplified: the "code" is valid if it matches the first 6 chars of
        # a SHA-256 of the secret (deterministic for testing)
        expected = hashlib.sha256(stored_secret.encode()).hexdigest()[:6]
        return code == expected

    # -----------------------------------------------------------------------
    # Role-Based Access
    # -----------------------------------------------------------------------

    def set_user_role(self, user_id: str, role: str) -> None:
        """Update a user's role (admin, user, readonly)."""
        valid_roles = {"admin", "user", "readonly"}
        if role not in valid_roles:
            raise AuthServiceError(f"Invalid role: {role}. Must be one of {valid_roles}")
        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET #r = :role, updated_at = :now",
            ExpressionAttributeValues={
                ":role": {"S": role},
                ":now": {"S": now},
            },
            ExpressionAttributeNames={"#r": "role"},
        )
        self._audit("role_changed", user_id, {"new_role": role})

    # -----------------------------------------------------------------------
    # Rate Limiting
    # -----------------------------------------------------------------------

    def _is_rate_limited(self, user_id: str, config: AuthConfig) -> bool:
        """Check if a user is rate-limited based on recent failed attempts."""
        attempts = self._failed_attempts.get(user_id, [])
        cutoff = time.time() - (config.lockout_duration_minutes * 60)
        recent = [t for t in attempts if t > cutoff]
        self._failed_attempts[user_id] = recent
        return len(recent) >= config.max_failed_attempts

    def _record_failed_attempt(self, user_id: str) -> None:
        """Record a failed login attempt timestamp."""
        if user_id not in self._failed_attempts:
            self._failed_attempts[user_id] = []
        self._failed_attempts[user_id].append(time.time())

    def _lock_account(self, user_id: str) -> None:
        """Lock a user account."""
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET #s = :status",
            ExpressionAttributeValues={":status": {"S": "locked"}},
            ExpressionAttributeNames={"#s": "status"},
        )

    def unlock_account(self, user_id: str) -> None:
        """Unlock a user account and clear failed attempts."""
        self.dynamodb.update_item(
            TableName=self.users_table,
            Key={"user_id": {"S": user_id}},
            UpdateExpression="SET #s = :status",
            ExpressionAttributeValues={":status": {"S": "active"}},
            ExpressionAttributeNames={"#s": "status"},
        )
        self._failed_attempts.pop(user_id, None)
        self._audit("account_unlocked", user_id, {})

    # -----------------------------------------------------------------------
    # Metrics
    # -----------------------------------------------------------------------

    def _publish_metric(self, metric_name: str, value: float, unit: str = "Count") -> None:
        """Publish a metric to CloudWatch."""
        self.cloudwatch.put_metric_data(
            Namespace=self.metrics_namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Dimensions": [
                        {"Name": "Service", "Value": "AuthService"},
                    ],
                }
            ],
        )

    def get_login_metrics(self) -> dict[str, float]:
        """Get login success and failure counts from CloudWatch."""
        result = {}
        for metric_name in ("LoginSuccess", "LoginFailures"):
            resp = self.cloudwatch.get_metric_statistics(
                Namespace=self.metrics_namespace,
                MetricName=metric_name,
                StartTime=datetime(2020, 1, 1, tzinfo=UTC),
                EndTime=datetime(2030, 1, 1, tzinfo=UTC),
                Period=86400 * 365,
                Statistics=["Sum"],
                Dimensions=[
                    {"Name": "Service", "Value": "AuthService"},
                ],
            )
            total = sum(dp["Sum"] for dp in resp.get("Datapoints", []))
            result[metric_name] = total
        return result

    # -----------------------------------------------------------------------
    # Audit Logging
    # -----------------------------------------------------------------------

    def _ensure_log_group(self) -> None:
        """Create the CloudWatch Logs log group if it doesn't exist."""
        try:
            self.logs.create_log_group(logGroupName=self.audit_log_group)
        except Exception:
            pass  # Already exists

    def _ensure_log_stream(self, stream_name: str) -> None:
        """Create a log stream if it doesn't exist."""
        try:
            self.logs.create_log_stream(
                logGroupName=self.audit_log_group,
                logStreamName=stream_name,
            )
        except Exception:
            pass  # Already exists

    def _audit(self, event_type: str, user_id: str, details: dict[str, Any]) -> None:
        """Write an audit event to CloudWatch Logs."""
        self._ensure_log_group()
        stream_name = "auth-events"
        self._ensure_log_stream(stream_name)

        event = {
            "event_type": event_type,
            "user_id": user_id,
            "timestamp": datetime.now(UTC).isoformat(),
            "details": details,
        }

        try:
            self.logs.put_log_events(
                logGroupName=self.audit_log_group,
                logStreamName=stream_name,
                logEvents=[
                    {
                        "timestamp": int(time.time() * 1000),
                        "message": json.dumps(event),
                    }
                ],
            )
        except Exception:
            pass  # Best-effort audit logging

    def get_audit_events(self, user_id: str | None = None) -> list[dict[str, Any]]:
        """
        Retrieve audit events from CloudWatch Logs.

        Optionally filter by user_id.
        """
        self._ensure_log_group()
        try:
            resp = self.logs.get_log_events(
                logGroupName=self.audit_log_group,
                logStreamName="auth-events",
                startFromHead=True,
            )
        except Exception:
            return []

        events = []
        for event in resp.get("events", []):
            try:
                parsed = json.loads(event["message"])
                if user_id is None or parsed.get("user_id") == user_id:
                    events.append(parsed)
            except (json.JSONDecodeError, KeyError):
                continue
        return events

    # -----------------------------------------------------------------------
    # Stats
    # -----------------------------------------------------------------------

    def get_user_stats(self) -> UserStats:
        """Get aggregate user statistics."""
        # Count total users
        scan_resp = self.dynamodb.scan(
            TableName=self.users_table,
            Select="COUNT",
        )
        total_users = scan_resp["Count"]

        # Count active sessions
        sess_resp = self.dynamodb.scan(
            TableName=self.sessions_table,
            Select="COUNT",
        )
        active_sessions = sess_resp["Count"]

        return UserStats(
            total_users=total_users,
            active_sessions=active_sessions,
        )

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _item_to_user(self, item: dict) -> User:
        """Convert a DynamoDB item dict to a User model."""
        return User(
            user_id=item["user_id"]["S"],
            email=item["email"]["S"],
            password_hash=item.get("password_hash", {}).get("S", ""),
            salt=item.get("salt", {}).get("S", ""),
            name=item.get("name", {}).get("S", ""),
            bio=item.get("bio", {}).get("S", ""),
            role=item.get("role", {}).get("S", "user"),
            status=item.get("status", {}).get("S", "active"),
            created_at=item.get("created_at", {}).get("S", ""),
            updated_at=item.get("updated_at", {}).get("S", ""),
        )
