"""EMR Serverless compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def emr_serverless():
    return make_client("emr-serverless")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def application(emr_serverless):
    """Create a SPARK application and clean it up after the test."""
    resp = emr_serverless.create_application(
        releaseLabel="emr-6.15.0",
        type="SPARK",
        name=_unique("test-app"),
    )
    app_id = resp["applicationId"]
    yield app_id
    try:
        emr_serverless.stop_application(applicationId=app_id)
    except Exception:
        pass  # best-effort cleanup
    try:
        emr_serverless.delete_application(applicationId=app_id)
    except Exception:
        pass  # best-effort cleanup


class TestEMRServerlessApplications:
    def test_create_application(self, emr_serverless):
        name = _unique("test-app")
        resp = emr_serverless.create_application(
            releaseLabel="emr-6.15.0",
            type="SPARK",
            name=name,
        )
        assert "applicationId" in resp
        assert "arn" in resp
        assert resp["name"] == name
        app_id = resp["applicationId"]
        # Cleanup
        emr_serverless.stop_application(applicationId=app_id)
        emr_serverless.delete_application(applicationId=app_id)

    def test_list_applications(self, emr_serverless, application):
        resp = emr_serverless.list_applications()
        assert "applications" in resp
        app_ids = [a["id"] for a in resp["applications"]]
        assert application in app_ids

    def test_get_application(self, emr_serverless, application):
        resp = emr_serverless.get_application(applicationId=application)
        app = resp["application"]
        assert app["applicationId"] == application
        assert app["releaseLabel"] == "emr-6.15.0"
        assert app["type"] == "Spark"
        assert "name" in app
        assert "arn" in app
        assert "state" in app

    def test_stop_application(self, emr_serverless, application):
        # Application starts in STARTED state
        app = emr_serverless.get_application(applicationId=application)["application"]
        assert app["state"] == "STARTED"

        emr_serverless.stop_application(applicationId=application)

        app = emr_serverless.get_application(applicationId=application)["application"]
        assert app["state"] == "STOPPED"

    def test_delete_application(self, emr_serverless):
        resp = emr_serverless.create_application(
            releaseLabel="emr-6.15.0",
            type="SPARK",
            name=_unique("delete-app"),
        )
        app_id = resp["applicationId"]

        emr_serverless.stop_application(applicationId=app_id)
        emr_serverless.delete_application(applicationId=app_id)

        # After deletion, get_application returns TERMINATED state
        app = emr_serverless.get_application(applicationId=app_id)["application"]
        assert app["state"] == "TERMINATED"

    def test_update_application(self, emr_serverless):
        resp = emr_serverless.create_application(
            releaseLabel="emr-6.15.0",
            type="SPARK",
            name=_unique("update-app"),
        )
        app_id = resp["applicationId"]

        # Must stop before updating
        emr_serverless.stop_application(applicationId=app_id)

        resp = emr_serverless.update_application(
            applicationId=app_id,
            maximumCapacity={"cpu": "4 vCPU", "memory": "16 GB", "disk": "100 GB"},
        )
        assert resp["application"]["applicationId"] == app_id

        # Cleanup
        emr_serverless.delete_application(applicationId=app_id)


class TestEMRServerlessJobRuns:
    @pytest.fixture
    def job_run(self, emr_serverless, application):
        resp = emr_serverless.start_job_run(
            applicationId=application,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://bucket/script.py",
                }
            },
        )
        return resp["jobRunId"]

    def test_start_job_run(self, emr_serverless, application):
        resp = emr_serverless.start_job_run(
            applicationId=application,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://bucket/script.py",
                }
            },
        )
        assert "jobRunId" in resp
        assert "arn" in resp

    def test_list_job_runs(self, emr_serverless, application, job_run):
        resp = emr_serverless.list_job_runs(applicationId=application)
        assert "jobRuns" in resp
        run_ids = [r["id"] for r in resp["jobRuns"]]
        assert job_run in run_ids

    def test_get_job_run(self, emr_serverless, application, job_run):
        resp = emr_serverless.get_job_run(
            applicationId=application,
            jobRunId=job_run,
        )
        jr = resp["jobRun"]
        assert jr["jobRunId"] == job_run
        assert jr["applicationId"] == application
        assert "state" in jr
        assert "executionRole" in jr

    def test_cancel_job_run(self, emr_serverless, application):
        """CancelJobRun cancels a running job and returns its IDs."""
        resp = emr_serverless.start_job_run(
            applicationId=application,
            executionRoleArn="arn:aws:iam::123456789012:role/test-role",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": "s3://bucket/cancel-script.py",
                }
            },
        )
        job_run_id = resp["jobRunId"]

        cancel = emr_serverless.cancel_job_run(
            applicationId=application,
            jobRunId=job_run_id,
        )
        assert cancel["applicationId"] == application
        assert cancel["jobRunId"] == job_run_id


class TestEMRServerlessApplicationLifecycle:
    def test_start_application(self, emr_serverless):
        """StartApplication transitions a stopped app to STARTED."""
        resp = emr_serverless.create_application(
            releaseLabel="emr-6.15.0",
            type="SPARK",
            name=_unique("start-app"),
        )
        app_id = resp["applicationId"]
        try:
            # Stop first (app starts in STARTED state)
            emr_serverless.stop_application(applicationId=app_id)
            app = emr_serverless.get_application(applicationId=app_id)["application"]
            assert app["state"] == "STOPPED"

            # Start again
            emr_serverless.start_application(applicationId=app_id)
            app = emr_serverless.get_application(applicationId=app_id)["application"]
            assert app["state"] == "STARTED"
        finally:
            try:
                emr_serverless.stop_application(applicationId=app_id)
            except Exception:
                pass  # best-effort cleanup
            try:
                emr_serverless.delete_application(applicationId=app_id)
            except Exception:
                pass  # best-effort cleanup
