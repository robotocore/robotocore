"""Tests for build artifact management in S3."""


class TestArtifacts:
    """S3-based build artifact storage and management."""

    def test_upload_artifact_with_git_sha_tag(self, pipeline):
        build = pipeline.queue_build(repo="org/art-app", branch="main", commit_sha="deadbeef1234")
        artifact = pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="app.zip",
            content=b"zipped binary",
            commit_sha="deadbeef1234",
            branch="main",
            build_number=7,
        )
        assert artifact.sha == "deadbeef1234"
        assert artifact.tags["commit-sha"] == "deadbeef1234"
        assert artifact.tags["branch"] == "main"
        assert artifact.tags["build-number"] == "7"

        # Verify in S3
        meta = pipeline.get_artifact_metadata(artifact.key)
        assert meta["commit-sha"] == "deadbeef1234"

    def test_download_artifact(self, pipeline):
        build = pipeline.queue_build(repo="org/dl-app", branch="main", commit_sha="aaa")
        artifact = pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="bundle.tar.gz",
            content=b"tarball bytes here",
            commit_sha="aaa",
            branch="main",
        )
        downloaded = pipeline.download_artifact(artifact.key)
        assert downloaded == b"tarball bytes here"

    def test_list_artifacts_for_build(self, pipeline):
        build = pipeline.queue_build(repo="org/list-app", branch="main", commit_sha="bbb")
        for name in ["app.zip", "test-report.xml", "coverage.json"]:
            pipeline.upload_artifact(
                build_id=build.build_id,
                artifact_name=name,
                content=b"data",
                commit_sha="bbb",
                branch="main",
            )
        keys = pipeline.list_artifacts(build_id=build.build_id)
        assert len(keys) == 3
        filenames = sorted(k.split("/")[-1] for k in keys)
        assert filenames == ["app.zip", "coverage.json", "test-report.xml"]

    def test_list_all_artifacts(self, pipeline):
        b1 = pipeline.queue_build(repo="org/all-art", branch="main", commit_sha="c1")
        b2 = pipeline.queue_build(repo="org/all-art", branch="main", commit_sha="c2")
        pipeline.upload_artifact(
            build_id=b1.build_id,
            artifact_name="a.zip",
            content=b"a",
            commit_sha="c1",
            branch="main",
        )
        pipeline.upload_artifact(
            build_id=b2.build_id,
            artifact_name="b.zip",
            content=b"b",
            commit_sha="c2",
            branch="main",
        )
        all_keys = pipeline.list_artifacts()
        assert len(all_keys) >= 2

    def test_promote_artifact_between_environments(self, pipeline):
        build = pipeline.queue_build(repo="org/promo-app", branch="main", commit_sha="ppp")
        artifact = pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="app.zip",
            content=b"promotable artifact",
            commit_sha="ppp",
            branch="main",
            environment="staging",
        )
        # Verify staging metadata
        meta = pipeline.get_artifact_metadata(artifact.key)
        assert meta["environment"] == "staging"

        # Promote to production
        prod_key = pipeline.promote_artifact(artifact.key, "production")
        assert "production" in prod_key

        # Verify production copy has updated metadata
        prod_meta = pipeline.get_artifact_metadata(prod_key)
        assert prod_meta["environment"] == "production"
        assert prod_meta["commit-sha"] == "ppp"

        # Verify content is the same
        prod_content = pipeline.download_artifact(prod_key)
        assert prod_content == b"promotable artifact"

    def test_delete_old_artifacts(self, pipeline):
        build = pipeline.queue_build(repo="org/del-app", branch="main", commit_sha="ddd")
        for name in ["app.zip", "docs.zip"]:
            pipeline.upload_artifact(
                build_id=build.build_id,
                artifact_name=name,
                content=b"data",
                commit_sha="ddd",
                branch="main",
            )
        # Verify artifacts exist
        keys = pipeline.list_artifacts(build_id=build.build_id)
        assert len(keys) == 2

        # Delete them
        count = pipeline.delete_artifacts(build.build_id)
        assert count == 2

        # Verify they're gone
        keys_after = pipeline.list_artifacts(build_id=build.build_id)
        assert len(keys_after) == 0

    def test_calculate_storage_usage(self, pipeline):
        build = pipeline.queue_build(repo="org/storage-app", branch="main", commit_sha="sss")
        pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="big.zip",
            content=b"x" * 1000,
            commit_sha="sss",
            branch="main",
        )
        usage = pipeline.calculate_storage_usage()
        assert usage["total_objects"] >= 1
        assert usage["total_size_bytes"] >= 1000

    def test_artifact_metadata_includes_build_id(self, pipeline):
        build = pipeline.queue_build(repo="org/meta-app", branch="develop", commit_sha="mmm")
        artifact = pipeline.upload_artifact(
            build_id=build.build_id,
            artifact_name="output.jar",
            content=b"java bytes",
            commit_sha="mmm",
            branch="develop",
        )
        meta = pipeline.get_artifact_metadata(artifact.key)
        assert meta["build-id"] == build.build_id
        assert meta["branch"] == "develop"
