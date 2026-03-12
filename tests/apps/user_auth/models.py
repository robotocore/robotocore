"""
Data models for the User Authentication & Identity Service.

Pure Python dataclasses — no AWS SDK imports. These represent the domain
objects that AuthService persists to DynamoDB, S3, Secrets Manager, SSM,
and CloudWatch.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class User:
    """A registered user in the system."""

    user_id: str
    email: str
    password_hash: str
    salt: str
    name: str = ""
    bio: str = ""
    role: str = "user"  # admin | user | readonly
    status: str = "active"  # active | locked | deleted
    created_at: str = ""
    updated_at: str = ""


@dataclass
class Session:
    """An active login session."""

    session_id: str
    user_id: str
    created_at: str
    expires_at: int  # epoch seconds (DynamoDB TTL)
    ip_address: str = ""
    user_agent: str = ""


@dataclass
class LoginAttempt:
    """A single login attempt (success or failure)."""

    user_id: str
    timestamp: str
    success: bool
    ip_address: str = ""
    failure_reason: str = ""


@dataclass
class PasswordResetToken:
    """A one-time password reset token."""

    token: str
    user_id: str
    created_at: str
    expires_at: int  # epoch seconds (DynamoDB TTL)
    used: bool = False


@dataclass
class AuthConfig:
    """Authentication configuration loaded from SSM."""

    token_expiry_hours: int = 24
    max_failed_attempts: int = 5
    lockout_duration_minutes: int = 30
    min_password_length: int = 12
    require_special_chars: bool = True


@dataclass
class UserStats:
    """Aggregate statistics for the auth system."""

    total_users: int = 0
    active_sessions: int = 0
    logins_today: int = 0
    failed_logins_today: int = 0
