"""Tool runner classes for executing IaC tools as subprocesses."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Default env vars for all AWS-targeting tools.
DEFAULT_AWS_ENV = {
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ENDPOINT_URL": "http://localhost:4566",
}


class ToolRunner:
    """Base class for running IaC tools as subprocesses."""

    def run(
        self,
        cmd: list[str],
        cwd: Path,
        env: dict | None = None,
        timeout: int = 120,
    ) -> subprocess.CompletedProcess:
        run_env = {**os.environ, **DEFAULT_AWS_ENV, **(env or {})}
        logger.info("Running: %s (cwd=%s)", " ".join(cmd), cwd)
        return subprocess.run(
            cmd,
            cwd=cwd,
            env=run_env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


class TerraformRunner(ToolRunner):
    """Run Terraform CLI commands."""

    def init(self, cwd: Path) -> subprocess.CompletedProcess:
        return self.run(["terraform", "init", "-input=false"], cwd)

    def plan(self, cwd: Path) -> subprocess.CompletedProcess:
        return self.run(["terraform", "plan", "-input=false"], cwd)

    def apply(self, cwd: Path, auto_approve: bool = True) -> subprocess.CompletedProcess:
        cmd = ["terraform", "apply", "-input=false"]
        if auto_approve:
            cmd.append("-auto-approve")
        return self.run(cmd, cwd)

    def destroy(self, cwd: Path, auto_approve: bool = True) -> subprocess.CompletedProcess:
        cmd = ["terraform", "destroy", "-input=false"]
        if auto_approve:
            cmd.append("-auto-approve")
        return self.run(cmd, cwd)

    def output(self, cwd: Path) -> dict:
        """Parse ``terraform output -json`` and return as a dict."""
        result = self.run(["terraform", "output", "-json"], cwd)
        if result.returncode != 0:
            raise RuntimeError(f"terraform output failed: {result.stderr}")
        return json.loads(result.stdout)


class CloudFormationRunner:
    """Deploy/delete CloudFormation stacks via boto3 (no subprocess needed)."""

    def __init__(self, client):
        self._client = client

    def deploy_stack(
        self,
        stack_name: str,
        template_body: str,
        params: dict | None = None,
        timeout: int = 300,
    ) -> dict:
        kwargs: dict = {
            "StackName": stack_name,
            "TemplateBody": template_body,
            "Capabilities": ["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"],
        }
        if params:
            kwargs["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v} for k, v in params.items()
            ]
        self._client.create_stack(**kwargs)
        return self.wait_for_stack(stack_name, "CREATE_COMPLETE", timeout)

    def delete_stack(self, stack_name: str, timeout: int = 300) -> None:
        self._client.delete_stack(StackName=stack_name)
        self.wait_for_stack(stack_name, "DELETE_COMPLETE", timeout)

    def wait_for_stack(self, stack_name: str, target_status: str, timeout: int = 300) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                resp = self._client.describe_stacks(StackName=stack_name)
                stacks = resp.get("Stacks", [])
                if not stacks:
                    if target_status == "DELETE_COMPLETE":
                        return {}
                    raise RuntimeError(f"Stack {stack_name} not found")
                status = stacks[0]["StackStatus"]
                if status == target_status:
                    return stacks[0]
                if status.endswith("_FAILED") or status.endswith("ROLLBACK_COMPLETE"):
                    reason = stacks[0].get("StackStatusReason", "unknown")
                    raise RuntimeError(f"Stack {stack_name} reached {status}: {reason}")
            except self._client.exceptions.ClientError as exc:
                if "does not exist" in str(exc) and target_status == "DELETE_COMPLETE":
                    return {}
                raise
            time.sleep(2)
        raise TimeoutError(f"Stack {stack_name} did not reach {target_status} within {timeout}s")


class CdkRunner(ToolRunner):
    """Run AWS CDK CLI commands."""

    def synth(self, cwd: Path) -> str:
        result = self.run(["cdk", "synth", "--no-staging"], cwd)
        if result.returncode != 0:
            raise RuntimeError(f"cdk synth failed: {result.stderr}")
        # Return path to synthesised template
        template_dir = cwd / "cdk.out"
        return str(template_dir)

    def deploy(self, cwd: Path, stack_name: str | None = None) -> subprocess.CompletedProcess:
        cmd = ["cdk", "deploy", "--require-approval=never"]
        if stack_name:
            cmd.append(stack_name)
        return self.run(cmd, cwd)

    def destroy(self, cwd: Path, stack_name: str | None = None) -> subprocess.CompletedProcess:
        cmd = ["cdk", "destroy", "--force"]
        if stack_name:
            cmd.append(stack_name)
        return self.run(cmd, cwd)


class PulumiRunner(ToolRunner):
    """Run Pulumi CLI commands."""

    _PULUMI_ENV = {
        "PULUMI_CONFIG_PASSPHRASE": "",
        "PULUMI_BACKEND_URL": "file://~",
    }

    def up(self, cwd: Path, stack: str = "test") -> subprocess.CompletedProcess:
        return self.run(
            ["pulumi", "up", "--yes", "--stack", stack],
            cwd,
            env=self._PULUMI_ENV,
        )

    def destroy(self, cwd: Path, stack: str = "test") -> subprocess.CompletedProcess:
        return self.run(
            ["pulumi", "destroy", "--yes", "--stack", stack],
            cwd,
            env=self._PULUMI_ENV,
        )

    def stack_output(self, cwd: Path, stack: str = "test") -> dict:
        result = self.run(
            ["pulumi", "stack", "output", "--json", "--stack", stack],
            cwd,
            env=self._PULUMI_ENV,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pulumi stack output failed: {result.stderr}")
        return json.loads(result.stdout)


class ServerlessRunner(ToolRunner):
    """Run Serverless Framework CLI commands."""

    def deploy(self, cwd: Path, stage: str = "test") -> subprocess.CompletedProcess:
        return self.run(["serverless", "deploy", "--stage", stage], cwd)

    def remove(self, cwd: Path, stage: str = "test") -> subprocess.CompletedProcess:
        return self.run(["serverless", "remove", "--stage", stage], cwd)


class SamRunner(ToolRunner):
    """Run AWS SAM CLI commands."""

    def build(self, cwd: Path) -> subprocess.CompletedProcess:
        return self.run(["sam", "build"], cwd)

    def deploy(self, cwd: Path, stack_name: str | None = None) -> subprocess.CompletedProcess:
        cmd = ["sam", "deploy", "--no-confirm-changeset", "--no-fail-on-empty-changeset"]
        if stack_name:
            cmd.extend(["--stack-name", stack_name])
        return self.run(cmd, cwd)

    def delete(self, cwd: Path, stack_name: str | None = None) -> subprocess.CompletedProcess:
        cmd = ["sam", "delete", "--no-prompts"]
        if stack_name:
            cmd.extend(["--stack-name", stack_name])
        return self.run(cmd, cwd)
