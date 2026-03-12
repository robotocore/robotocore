"""Data models for the CI/CD pipeline application."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Build:
    """Represents a single build execution."""

    build_id: str
    repo: str
    branch: str
    commit_sha: str
    status: str  # QUEUED, BUILDING, TESTING, DEPLOYING, SUCCESS, FAILED, CANCELLED
    started_at: str
    finished_at: str | None = None
    artifact_key: str | None = None
    build_number: int = 0
    error_message: str | None = None

    def to_dynamodb_item(self) -> dict[str, dict[str, str]]:
        item: dict[str, dict[str, str]] = {
            "build_id": {"S": self.build_id},
            "repo_name": {"S": self.repo},
            "branch": {"S": self.branch},
            "commit_sha": {"S": self.commit_sha},
            "status": {"S": self.status},
            "started_at": {"S": self.started_at},
            "build_number": {"N": str(self.build_number)},
        }
        if self.finished_at:
            item["finished_at"] = {"S": self.finished_at}
        if self.artifact_key:
            item["artifact_key"] = {"S": self.artifact_key}
        if self.error_message:
            item["error_message"] = {"S": self.error_message}
        return item

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, dict[str, str]]) -> Build:
        return cls(
            build_id=item["build_id"]["S"],
            repo=item["repo_name"]["S"],
            branch=item["branch"]["S"],
            commit_sha=item["commit_sha"]["S"],
            status=item["status"]["S"],
            started_at=item["started_at"]["S"],
            finished_at=item.get("finished_at", {}).get("S"),
            artifact_key=item.get("artifact_key", {}).get("S"),
            build_number=int(item.get("build_number", {}).get("N", "0")),
            error_message=item.get("error_message", {}).get("S"),
        )


@dataclass
class PipelineConfig:
    """Configuration for a CI/CD pipeline."""

    repo_url: str
    build_commands: list[str]
    deploy_target: str
    notification_topic: str
    branch_filter: str = "main"

    def to_ssm_params(self, prefix: str) -> dict[str, str]:
        return {
            f"{prefix}/repo_url": self.repo_url,
            f"{prefix}/build_commands": ",".join(self.build_commands),
            f"{prefix}/deploy_target": self.deploy_target,
            f"{prefix}/notification_topic": self.notification_topic,
            f"{prefix}/branch_filter": self.branch_filter,
        }

    @classmethod
    def from_ssm_params(cls, params: dict[str, str], prefix: str) -> PipelineConfig:
        return cls(
            repo_url=params[f"{prefix}/repo_url"],
            build_commands=params[f"{prefix}/build_commands"].split(","),
            deploy_target=params[f"{prefix}/deploy_target"],
            notification_topic=params[f"{prefix}/notification_topic"],
            branch_filter=params.get(f"{prefix}/branch_filter", "main"),
        )


@dataclass
class BuildLog:
    """A single log entry from a build."""

    build_id: str
    timestamp: int  # epoch millis
    level: str  # INFO, WARN, ERROR
    message: str


@dataclass
class Artifact:
    """A build artifact stored in S3."""

    key: str
    bucket: str
    size: int
    sha: str
    tags: dict[str, str] = field(default_factory=dict)
    uploaded_at: str = ""


@dataclass
class BuildNotification:
    """A notification about a build event."""

    build_id: str
    repo: str
    status: str
    message: str
    timestamp: str


@dataclass
class PipelineMetrics:
    """Aggregated pipeline metrics."""

    total_builds: int
    success_count: int
    failure_count: int
    avg_duration_seconds: float
