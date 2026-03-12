"""
Data models for the serverless API application.

These are plain dataclasses — no AWS SDK imports. They describe the desired
state of the infrastructure and the shape of deployment outputs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class ApiEndpoint:
    """A single REST API route backed by a Lambda function."""

    method: str  # GET, POST, PUT, DELETE, etc.
    path: str  # e.g. "/users", "/users/{id}"
    handler_name: str  # Lambda function name that handles this route
    authorization_type: str = "NONE"  # NONE, AWS_IAM, CUSTOM, COGNITO_USER_POOLS


@dataclass
class LambdaConfig:
    """Configuration for deploying a single Lambda function."""

    function_name: str
    handler: str  # e.g. "index.handler"
    runtime: str  # e.g. "python3.12"
    code: str  # Python source code for the handler
    env_vars: dict[str, str] = field(default_factory=dict)
    timeout: int = 30
    memory: int = 128


@dataclass
class TableSchema:
    """DynamoDB table definition with optional GSIs."""

    table_name: str
    key_schema: list[dict]  # [{"AttributeName": "pk", "KeyType": "HASH"}, ...]
    attributes: list[dict]  # [{"AttributeName": "pk", "AttributeType": "S"}, ...]
    gsis: list[dict] = field(default_factory=list)  # GlobalSecondaryIndexes


@dataclass
class WorkflowStep:
    """A single state in a Step Functions state machine."""

    name: str
    type: str  # Pass, Task, Choice, Parallel, Wait, Succeed, Fail
    resource: str | None = None  # Lambda ARN for Task states
    next: str | None = None
    end: bool = False
    result: dict | None = None
    retry: list[dict] = field(default_factory=list)
    catch: list[dict] = field(default_factory=list)
    choices: list[dict] = field(default_factory=list)  # For Choice states
    default: str | None = None  # Default next state for Choice
    branches: list[dict] = field(default_factory=list)  # For Parallel states


@dataclass
class ApiDeployment:
    """Record of a deployed API Gateway stage."""

    rest_api_id: str
    stage_name: str
    deployment_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class DeployedStack:
    """Complete output of a deployed serverless stack."""

    api_url: str
    functions: dict[str, str]  # function_name -> ARN
    tables: list[str]  # table names
    state_machines: dict[str, str]  # name -> ARN
    roles: dict[str, str]  # role_name -> ARN
