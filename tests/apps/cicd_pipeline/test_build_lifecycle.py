"""Tests for build lifecycle management."""

import pytest

from .app import CICDPipeline


class TestBuildLifecycle:
    """Build status transitions and lifecycle management."""

    def test_queue_new_build(self, pipeline):
        build = pipeline.queue_build(
            repo="org/myapp",
            branch="main",
            commit_sha="aaa111",
        )
        assert build.status == CICDPipeline.QUEUED
        assert build.repo == "org/myapp"
        assert build.branch == "main"
        assert build.commit_sha == "aaa111"
        assert build.build_id.startswith("build-")

    def test_queue_sets_started_at(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="bbb222")
        assert build.started_at is not None
        assert "T" in build.started_at  # ISO format

    def test_transition_queued_to_building(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="ccc333")
        updated = pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
        assert updated.status == CICDPipeline.BUILDING

    def test_full_status_progression(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="ddd444")
        statuses = [
            CICDPipeline.BUILDING,
            CICDPipeline.TESTING,
            CICDPipeline.DEPLOYING,
            CICDPipeline.SUCCESS,
        ]
        for status in statuses:
            build = pipeline.transition_build(build.build_id, status)
            assert build.status == status

        # Terminal status should set finished_at
        assert build.finished_at is not None

    def test_build_with_artifact(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="eee555")
        pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
        artifact = pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="app.zip",
            content=b"compiled binary",
            commit_sha="eee555",
            branch="main",
        )
        assert artifact.key == f"artifacts/{build.build_id}/app.zip"

        refreshed = pipeline.get_build(build.build_id)
        assert refreshed.artifact_key == artifact.key

    def test_build_failure_with_error_message(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="fff666")
        pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
        failed = pipeline.fail_build(build.build_id, "Compilation error on line 42")
        assert failed.status == CICDPipeline.FAILED
        assert failed.error_message == "Compilation error on line 42"
        assert failed.finished_at is not None

    def test_retry_failed_build(self, pipeline):
        build = pipeline.queue_build(
            repo="org/myapp", branch="feature/x", commit_sha="ggg777", build_number=1
        )
        pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
        pipeline.fail_build(build.build_id, "OOM killed")

        retried = pipeline.retry_build(build.build_id)
        assert retried.status == CICDPipeline.QUEUED
        assert retried.repo == "org/myapp"
        assert retried.branch == "feature/x"
        assert retried.commit_sha == "ggg777"
        assert retried.build_number == 2
        assert retried.build_id != build.build_id

    def test_retry_non_failed_build_raises(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="hhh888")
        with pytest.raises(ValueError, match="Can only retry FAILED builds"):
            pipeline.retry_build(build.build_id)

    def test_cancel_build(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="iii999")
        cancelled = pipeline.cancel_build(build.build_id)
        assert cancelled.status == CICDPipeline.CANCELLED
        assert cancelled.finished_at is not None

    def test_cancel_terminal_build_raises(self, pipeline):
        build = pipeline.queue_build(repo="org/myapp", branch="main", commit_sha="jjj000")
        pipeline.transition_build(build.build_id, CICDPipeline.BUILDING)
        pipeline.transition_build(build.build_id, CICDPipeline.SUCCESS)
        with pytest.raises(ValueError, match="Cannot cancel"):
            pipeline.cancel_build(build.build_id)

    def test_get_build_not_found_raises(self, pipeline):
        with pytest.raises(KeyError, match="not found"):
            pipeline.get_build("build-nonexistent")

    def test_list_builds_by_repo(self, pipeline):
        for i in range(3):
            pipeline.queue_build(repo="org/listed-app", branch="main", commit_sha=f"sha{i}")
        pipeline.queue_build(repo="org/other-app", branch="main", commit_sha="other")

        builds = pipeline.list_builds_by_repo("org/listed-app")
        assert len(builds) == 3
        assert all(b.repo == "org/listed-app" for b in builds)

    def test_list_builds_by_status(self, pipeline):
        b1 = pipeline.queue_build(repo="org/status-app", branch="main", commit_sha="s1")
        b2 = pipeline.queue_build(repo="org/status-app", branch="main", commit_sha="s2")
        pipeline.transition_build(b1.build_id, CICDPipeline.SUCCESS)

        success_builds = pipeline.list_builds_by_status(CICDPipeline.SUCCESS)
        queued_builds = pipeline.list_builds_by_status(CICDPipeline.QUEUED)
        assert any(b.build_id == b1.build_id for b in success_builds)
        assert any(b.build_id == b2.build_id for b in queued_builds)

    def test_sample_build_fixture(self, sample_build):
        assert sample_build.status == CICDPipeline.SUCCESS
        assert sample_build.repo == "org/sample-app"
        assert sample_build.artifact_key is not None
        assert sample_build.finished_at is not None
