"""
Tests for SSM Parameter Store configuration and Secrets Manager credentials.
"""

import pytest

from .models import PipelineConfig

pytestmark = pytest.mark.apps


class TestSSMConfiguration:
    """SSM Parameter Store: hierarchical pipeline configuration."""

    def test_store_and_load_config(self, pipeline, unique_suffix):
        """Store a PipelineConfig in SSM and load it back."""
        config = PipelineConfig(
            stream_name="test-stream",
            batch_size=200,
            flush_interval=60,
            s3_prefix="archive",
            table_name="test-readings",
        )
        pipeline_id = f"cfg-{unique_suffix}"
        pipeline.store_config(pipeline_id, config)

        loaded = pipeline.load_config(pipeline_id)
        assert loaded.stream_name == "test-stream"
        assert loaded.batch_size == 200
        assert loaded.flush_interval == 60
        assert loaded.s3_prefix == "archive"
        assert loaded.table_name == "test-readings"

        # Cleanup
        pipeline.delete_config(pipeline_id)

    def test_update_config_param(self, pipeline, unique_suffix):
        """Update a single config parameter and verify the change."""
        pipeline_id = f"upd-{unique_suffix}"
        config = PipelineConfig(stream_name="s", batch_size=100)
        pipeline.store_config(pipeline_id, config)

        # Update batch_size
        version = pipeline.update_config_param(pipeline_id, "batch_size", "500")
        assert version >= 2  # first put was version 1, update is version 2

        loaded = pipeline.load_config(pipeline_id)
        assert loaded.batch_size == 500

        pipeline.delete_config(pipeline_id)

    def test_config_versioning(self, pipeline, unique_suffix):
        """Overwrite a parameter multiple times and check version history."""
        pipeline_id = f"ver-{unique_suffix}"
        config = PipelineConfig(stream_name="s")
        pipeline.store_config(pipeline_id, config)

        # Overwrite batch_size 3 more times
        for val in ["200", "300", "400"]:
            pipeline.update_config_param(pipeline_id, "batch_size", val)

        history = pipeline.get_config_history(pipeline_id, "batch_size")
        assert len(history) == 4  # initial + 3 updates
        versions = [h["Version"] for h in history]
        assert versions == [1, 2, 3, 4]

        pipeline.delete_config(pipeline_id)

    def test_hierarchical_parameters(self, pipeline, unique_suffix):
        """Verify parameters are stored under /pipeline/{id}/{key} hierarchy."""
        pipeline_id = f"hier-{unique_suffix}"
        config = PipelineConfig(stream_name="s", batch_size=50, flush_interval=15)
        pipeline.store_config(pipeline_id, config)

        resp = pipeline.ssm.get_parameters_by_path(Path=f"/pipeline/{pipeline_id}", Recursive=True)
        param_names = {p["Name"] for p in resp["Parameters"]}
        assert f"/pipeline/{pipeline_id}/batch_size" in param_names
        assert f"/pipeline/{pipeline_id}/flush_interval" in param_names
        assert f"/pipeline/{pipeline_id}/stream_name" in param_names

        pipeline.delete_config(pipeline_id)

    def test_delete_config_cleans_up(self, pipeline, unique_suffix):
        """delete_config removes all parameters under the pipeline prefix."""
        pipeline_id = f"del-{unique_suffix}"
        config = PipelineConfig(stream_name="s")
        pipeline.store_config(pipeline_id, config)

        pipeline.delete_config(pipeline_id)

        resp = pipeline.ssm.get_parameters_by_path(Path=f"/pipeline/{pipeline_id}", Recursive=True)
        assert len(resp["Parameters"]) == 0


class TestSecretsManager:
    """Secrets Manager: database credentials and API keys."""

    def test_store_and_retrieve_credentials(self, pipeline, unique_suffix):
        """Store JSON credentials and retrieve them."""
        secret_name = f"pipeline/creds-{unique_suffix}"
        creds = {
            "host": "timescaledb.internal",
            "port": 5432,
            "username": "pipeline_writer",
            "password": "s3cur3-p@ss!",
            "database": "sensor_data",
        }
        arn = pipeline.store_credentials(secret_name, creds)
        assert arn  # non-empty ARN

        retrieved = pipeline.get_credentials(secret_name)
        assert retrieved["host"] == "timescaledb.internal"
        assert retrieved["port"] == 5432
        assert retrieved["username"] == "pipeline_writer"

        pipeline.delete_credentials(secret_name)

    def test_update_credentials(self, pipeline, unique_suffix):
        """Update stored credentials and verify the new values."""
        secret_name = f"pipeline/upd-creds-{unique_suffix}"
        creds = {"username": "reader", "password": "old-pass"}
        pipeline.store_credentials(secret_name, creds)

        # Update password
        creds["password"] = "new-secure-pass!"
        pipeline.update_credentials(secret_name, creds)

        retrieved = pipeline.get_credentials(secret_name)
        assert retrieved["password"] == "new-secure-pass!"
        assert retrieved["username"] == "reader"

        pipeline.delete_credentials(secret_name)

    def test_store_api_keys(self, pipeline, unique_suffix):
        """Store API keys for external enrichment services."""
        secret_name = f"pipeline/api-keys-{unique_suffix}"
        keys = {
            "weather_api_key": "wk-abc123",
            "geocoding_api_key": "gk-xyz789",
        }
        pipeline.store_credentials(secret_name, keys)

        retrieved = pipeline.get_credentials(secret_name)
        assert retrieved["weather_api_key"] == "wk-abc123"
        assert retrieved["geocoding_api_key"] == "gk-xyz789"

        pipeline.delete_credentials(secret_name)

    def test_credential_lifecycle(self, pipeline, unique_suffix):
        """Full lifecycle: create, read, update, read, delete."""
        secret_name = f"pipeline/lifecycle-{unique_suffix}"
        # Create
        pipeline.store_credentials(secret_name, {"key": "v1"})
        assert pipeline.get_credentials(secret_name)["key"] == "v1"

        # Update
        pipeline.update_credentials(secret_name, {"key": "v2"})
        assert pipeline.get_credentials(secret_name)["key"] == "v2"

        # Delete
        pipeline.delete_credentials(secret_name)
        # After deletion, get should raise
        with pytest.raises(Exception):
            pipeline.get_credentials(secret_name)
