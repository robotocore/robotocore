"""Tests for pipeline configuration via SSM Parameter Store."""

from .models import PipelineConfig


class TestPipelineConfiguration:
    """SSM Parameter Store-based pipeline configuration."""

    def test_store_and_read_config(self, pipeline):
        config = PipelineConfig(
            repo_url="https://github.com/org/myapp",
            build_commands=["npm install", "npm run build"],
            deploy_target="ecs-cluster-prod",
            notification_topic="arn:aws:sns:us-east-1:123456789012:builds",
            branch_filter="main",
        )
        pipeline.store_config("org/myapp", config)

        retrieved = pipeline.get_config("org/myapp")
        assert retrieved.repo_url == "https://github.com/org/myapp"
        assert retrieved.build_commands == ["npm install", "npm run build"]
        assert retrieved.deploy_target == "ecs-cluster-prod"
        assert retrieved.notification_topic == "arn:aws:sns:us-east-1:123456789012:builds"
        assert retrieved.branch_filter == "main"

        pipeline.delete_config("org/myapp")

    def test_update_config_field(self, pipeline):
        config = PipelineConfig(
            repo_url="https://github.com/org/app2",
            build_commands=["make build"],
            deploy_target="lambda",
            notification_topic="arn:aws:sns:us-east-1:123456789012:builds2",
        )
        pipeline.store_config("org/app2", config)

        pipeline.update_config_field("org/app2", "deploy_target", "ecs-fargate")

        updated = pipeline.get_config("org/app2")
        assert updated.deploy_target == "ecs-fargate"
        # Other fields unchanged
        assert updated.repo_url == "https://github.com/org/app2"

        pipeline.delete_config("org/app2")

    def test_multiple_repos_different_configs(self, pipeline):
        config_a = PipelineConfig(
            repo_url="https://github.com/org/frontend",
            build_commands=["yarn build"],
            deploy_target="s3-static",
            notification_topic="arn:aws:sns:us-east-1:123456789012:fe",
            branch_filter="main",
        )
        config_b = PipelineConfig(
            repo_url="https://github.com/org/backend",
            build_commands=["go build"],
            deploy_target="ecs-cluster",
            notification_topic="arn:aws:sns:us-east-1:123456789012:be",
            branch_filter="release/*",
        )
        pipeline.store_config("org/frontend", config_a)
        pipeline.store_config("org/backend", config_b)

        retrieved_a = pipeline.get_config("org/frontend")
        retrieved_b = pipeline.get_config("org/backend")

        assert retrieved_a.build_commands == ["yarn build"]
        assert retrieved_b.build_commands == ["go build"]
        assert retrieved_a.deploy_target != retrieved_b.deploy_target

        pipeline.delete_config("org/frontend")
        pipeline.delete_config("org/backend")

    def test_hierarchical_parameter_paths(self, pipeline):
        """Config uses hierarchical paths: /pipeline/{repo}/field."""
        config = PipelineConfig(
            repo_url="https://github.com/org/hier-app",
            build_commands=["pytest"],
            deploy_target="lambda",
            notification_topic="arn:aws:sns:us-east-1:123456789012:hier",
        )
        pipeline.store_config("org/hier-app", config)

        # Verify individual parameter by name
        param_name = f"{pipeline.config_prefix}/org/hier-app/build_commands"
        resp = pipeline.ssm.get_parameter(Name=param_name)
        assert resp["Parameter"]["Value"] == "pytest"

        pipeline.delete_config("org/hier-app")

    def test_overwrite_existing_config(self, pipeline):
        config_v1 = PipelineConfig(
            repo_url="https://github.com/org/versioned",
            build_commands=["make v1"],
            deploy_target="old-target",
            notification_topic="arn:aws:sns:us-east-1:123456789012:v1",
        )
        pipeline.store_config("org/versioned", config_v1)

        config_v2 = PipelineConfig(
            repo_url="https://github.com/org/versioned",
            build_commands=["make v2"],
            deploy_target="new-target",
            notification_topic="arn:aws:sns:us-east-1:123456789012:v2",
        )
        pipeline.store_config("org/versioned", config_v2)

        retrieved = pipeline.get_config("org/versioned")
        assert retrieved.build_commands == ["make v2"]
        assert retrieved.deploy_target == "new-target"

        pipeline.delete_config("org/versioned")
