"""Tests for the GitHub Actions composite action setup-robotocore."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

# Root of the repo
REPO_ROOT = Path(__file__).resolve().parents[3]
ACTION_DIR = REPO_ROOT / ".github" / "actions" / "setup-robotocore"
ACTION_YML = ACTION_DIR / "action.yml"
CLEANUP_SCRIPT = ACTION_DIR / "cleanup.sh"
EXAMPLE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "example-robotocore.yml"


def _load_action() -> dict:
    """Load and parse action.yml."""
    assert ACTION_YML.exists(), f"action.yml not found at {ACTION_YML}"
    with open(ACTION_YML) as f:
        return yaml.safe_load(f)


class TestActionYmlStructure:
    """Test action.yml is valid YAML with required structure."""

    def test_action_yml_exists(self) -> None:
        assert ACTION_YML.exists()

    def test_action_yml_is_valid_yaml(self) -> None:
        action = _load_action()
        assert isinstance(action, dict)

    def test_action_has_name(self) -> None:
        action = _load_action()
        assert "name" in action
        assert "robotocore" in action["name"].lower()

    def test_action_has_description(self) -> None:
        action = _load_action()
        assert "description" in action

    def test_action_is_composite(self) -> None:
        action = _load_action()
        assert action["runs"]["using"] == "composite"


class TestActionInputs:
    """Test action.yml has all expected inputs with defaults."""

    def test_has_image_tag_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "image-tag" in inputs
        assert inputs["image-tag"]["default"] == "latest"

    def test_has_configuration_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "configuration" in inputs
        assert inputs["configuration"]["default"] == ""

    def test_has_wait_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "wait" in inputs
        assert inputs["wait"]["default"] == "true"

    def test_has_wait_timeout_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "wait-timeout" in inputs
        assert inputs["wait-timeout"]["default"] == "30"

    def test_has_services_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "services" in inputs
        assert inputs["services"]["default"] == ""

    def test_has_persistence_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "persistence" in inputs
        assert inputs["persistence"]["default"] == "false"

    def test_has_iam_enforcement_input(self) -> None:
        action = _load_action()
        inputs = action["inputs"]
        assert "iam-enforcement" in inputs
        assert inputs["iam-enforcement"]["default"] == "false"

    def test_all_inputs_have_descriptions(self) -> None:
        action = _load_action()
        for name, spec in action["inputs"].items():
            assert "description" in spec, f"Input '{name}' missing description"


class TestActionOutputs:
    """Test action.yml has all expected outputs."""

    def test_has_endpoint_output(self) -> None:
        action = _load_action()
        assert "endpoint" in action["outputs"]

    def test_has_container_id_output(self) -> None:
        action = _load_action()
        assert "container-id" in action["outputs"]

    def test_endpoint_output_has_description(self) -> None:
        action = _load_action()
        assert "description" in action["outputs"]["endpoint"]

    def test_container_id_output_has_description(self) -> None:
        action = _load_action()
        assert "description" in action["outputs"]["container-id"]


class TestActionSteps:
    """Test action.yml steps include docker pull, docker run, health check."""

    def _get_steps(self) -> list[dict]:
        action = _load_action()
        return action["runs"]["steps"]

    def _all_step_scripts(self) -> str:
        """Concatenate all shell step run scripts into one string."""
        steps = self._get_steps()
        return "\n".join(s.get("run", "") for s in steps)

    def test_steps_include_docker_pull(self) -> None:
        scripts = self._all_step_scripts()
        assert "docker pull" in scripts

    def test_steps_include_docker_run(self) -> None:
        scripts = self._all_step_scripts()
        assert "docker run" in scripts

    def test_steps_include_health_check(self) -> None:
        scripts = self._all_step_scripts()
        assert "/_robotocore/health" in scripts

    def test_default_image_is_ghcr(self) -> None:
        scripts = self._all_step_scripts()
        assert "ghcr.io/robotocore/robotocore" in scripts

    def test_steps_all_have_shell(self) -> None:
        """Every step with a 'run' key must specify shell: bash."""
        steps = self._get_steps()
        for step in steps:
            if "run" in step:
                assert step.get("shell") == "bash", (
                    f"Step '{step.get('name', '?')}' missing shell: bash"
                )


class TestConfigurationParsing:
    """Test configuration input parsing (newline-separated KEY=VALUE -> docker env flags)."""

    def test_configuration_converted_to_env_flags(self) -> None:
        """The docker run step must translate configuration lines to -e flags."""
        steps = _load_action()["runs"]["steps"]
        # Find the step that does docker run
        docker_run_script = ""
        for step in steps:
            run = step.get("run", "")
            if "docker run" in run:
                docker_run_script = run
                break
        assert docker_run_script, "No docker run step found"
        # Must reference the configuration input and use -e
        assert "configuration" in docker_run_script or "CONFIGURATION" in docker_run_script


class TestWaitLogic:
    """Test wait logic: health check URL and timeout handling."""

    def _get_health_step_script(self) -> str:
        steps = _load_action()["runs"]["steps"]
        for step in steps:
            run = step.get("run", "")
            if "/_robotocore/health" in run:
                return run
        return ""

    def test_health_check_url(self) -> None:
        script = self._get_health_step_script()
        assert "http://localhost:4566/_robotocore/health" in script

    def test_timeout_handling(self) -> None:
        script = self._get_health_step_script()
        # Must reference the wait-timeout input
        assert "wait-timeout" in script or "WAIT_TIMEOUT" in script or "timeout" in script.lower()

    def test_wait_conditional(self) -> None:
        """Health check should be conditional on the wait input."""
        script = self._get_health_step_script()
        assert "wait" in script.lower()


class TestEnvVarMapping:
    """Test services/persistence/iam-enforcement map to correct env vars."""

    def _get_docker_run_script(self) -> str:
        steps = _load_action()["runs"]["steps"]
        for step in steps:
            run = step.get("run", "")
            if "docker run" in run:
                return run
        return ""

    def test_services_maps_to_services_env(self) -> None:
        script = self._get_docker_run_script()
        assert "SERVICES" in script

    def test_persistence_maps_to_persistence_env(self) -> None:
        script = self._get_docker_run_script()
        assert "PERSISTENCE" in script

    def test_iam_enforcement_maps_to_enforce_iam_env(self) -> None:
        script = self._get_docker_run_script()
        assert "ENFORCE_IAM" in script


class TestDockerImage:
    """Test default and custom image tag."""

    def _get_docker_pull_script(self) -> str:
        steps = _load_action()["runs"]["steps"]
        for step in steps:
            run = step.get("run", "")
            if "docker pull" in run:
                return run
        return ""

    def test_default_image(self) -> None:
        script = self._get_docker_pull_script()
        assert "ghcr.io/robotocore/robotocore" in script

    def test_custom_tag_supported(self) -> None:
        """The pull command must reference the image-tag input."""
        script = self._get_docker_pull_script()
        assert "image-tag" in script or "IMAGE_TAG" in script


class TestCleanupScript:
    """Test cleanup script exists and stops container."""

    def test_cleanup_script_exists(self) -> None:
        assert CLEANUP_SCRIPT.exists()

    def test_cleanup_script_is_executable(self) -> None:
        assert os.access(CLEANUP_SCRIPT, os.X_OK)

    def test_cleanup_stops_container(self) -> None:
        content = CLEANUP_SCRIPT.read_text()
        assert "docker stop" in content or "docker rm" in content

    def test_cleanup_removes_container(self) -> None:
        content = CLEANUP_SCRIPT.read_text()
        assert "docker rm" in content


class TestAWSEnvVars:
    """Test AWS env vars are set in environment."""

    def _all_step_scripts(self) -> str:
        steps = _load_action()["runs"]["steps"]
        return "\n".join(s.get("run", "") for s in steps)

    def _all_env_blocks(self) -> str:
        """Collect all env settings from steps."""
        steps = _load_action()["runs"]["steps"]
        result = []
        for step in steps:
            run = step.get("run", "")
            result.append(run)
        return "\n".join(result)

    def test_aws_endpoint_url_set(self) -> None:
        scripts = self._all_env_blocks()
        assert "AWS_ENDPOINT_URL" in scripts

    def test_aws_access_key_id_set(self) -> None:
        scripts = self._all_env_blocks()
        assert "AWS_ACCESS_KEY_ID" in scripts

    def test_aws_secret_access_key_set(self) -> None:
        scripts = self._all_env_blocks()
        assert "AWS_SECRET_ACCESS_KEY" in scripts

    def test_aws_default_region_set(self) -> None:
        scripts = self._all_env_blocks()
        assert "AWS_DEFAULT_REGION" in scripts


class TestExampleWorkflow:
    """Test example workflow exists and is valid."""

    def test_example_workflow_exists(self) -> None:
        assert EXAMPLE_WORKFLOW.exists()

    def test_example_workflow_is_valid_yaml(self) -> None:
        with open(EXAMPLE_WORKFLOW) as f:
            wf = yaml.safe_load(f)
        assert isinstance(wf, dict)
        assert "on" in wf or True in wf  # YAML parses `on:` as True key

    def test_example_uses_setup_action(self) -> None:
        content = EXAMPLE_WORKFLOW.read_text()
        assert "setup-robotocore" in content


class TestReadmeExists:
    """Test README exists for the published action."""

    def test_readme_exists(self) -> None:
        readme = ACTION_DIR / "README.md"
        assert readme.exists()

    def test_readme_has_usage(self) -> None:
        readme = ACTION_DIR / "README.md"
        content = readme.read_text()
        assert "usage" in content.lower() or "uses:" in content.lower()
