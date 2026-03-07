"""AWS Batch compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def batch():
    return make_client("batch")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_compute_env(batch, name):
    """Helper to create a FARGATE compute environment."""
    batch.create_compute_environment(
        computeEnvironmentName=name,
        type="MANAGED",
        computeResources={
            "type": "FARGATE",
            "maxvCpus": 2,
            "subnets": ["subnet-12345"],
            "securityGroupIds": ["sg-12345"],
        },
    )
    resp = batch.describe_compute_environments(computeEnvironments=[name])
    return resp["computeEnvironments"][0]["computeEnvironmentArn"]


class TestComputeEnvironments:
    def test_create_compute_environment(self, batch):
        name = _unique("ce")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        assert "computeEnvironmentName" in resp
        assert resp["computeEnvironmentName"] == name
        batch.delete_compute_environment(computeEnvironment=name)

    def test_create_compute_environment_returns_arn(self, batch):
        name = _unique("ce-arn")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        assert "computeEnvironmentArn" in resp
        assert name in resp["computeEnvironmentArn"]
        batch.delete_compute_environment(computeEnvironment=name)

    def test_describe_compute_environments(self, batch):
        name = _unique("desc-ce")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = batch.describe_compute_environments(computeEnvironments=[name])
            assert len(resp["computeEnvironments"]) >= 1
            ce = resp["computeEnvironments"][0]
            assert ce["computeEnvironmentName"] == name
            assert "computeEnvironmentArn" in ce
            assert "state" in ce or "status" in ce
        finally:
            batch.delete_compute_environment(computeEnvironment=name)

    def test_describe_compute_environments_all(self, batch):
        """Describe all compute environments without filter."""
        name = _unique("all-ce")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = batch.describe_compute_environments()
            names = [ce["computeEnvironmentName"] for ce in resp["computeEnvironments"]]
            assert name in names
        finally:
            batch.delete_compute_environment(computeEnvironment=name)

    def test_delete_compute_environment(self, batch):
        name = _unique("del-ce")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        batch.delete_compute_environment(computeEnvironment=name)
        resp = batch.describe_compute_environments(computeEnvironments=[name])
        assert len(resp["computeEnvironments"]) == 0

    def test_update_compute_environment(self, batch):
        """Update maxvCpus on a compute environment."""
        name = _unique("upd-ce")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            batch.update_compute_environment(
                computeEnvironment=name,
                computeResources={"maxvCpus": 4},
            )
            resp = batch.describe_compute_environments(computeEnvironments=[name])
            resources = resp["computeEnvironments"][0].get("computeResources", {})
            assert resources.get("maxvCpus") == 4
        finally:
            batch.delete_compute_environment(computeEnvironment=name)


class TestJobDefinitions:
    def test_register_job_definition(self, batch):
        name = _unique("jd")
        resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
        )
        assert resp["jobDefinitionName"] == name
        assert resp["revision"] == 1
        assert "jobDefinitionArn" in resp
        batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_register_job_definition_with_environment(self, batch):
        """Register a job definition with environment variables."""
        name = _unique("jd-env")
        resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
                "environment": [
                    {"name": "MY_VAR", "value": "hello"},
                ],
            },
        )
        assert resp["jobDefinitionName"] == name
        batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_describe_job_definitions(self, batch):
        name = _unique("desc-jd")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
        )
        try:
            resp = batch.describe_job_definitions(jobDefinitionName=name)
            assert len(resp["jobDefinitions"]) >= 1
            jd = resp["jobDefinitions"][0]
            assert jd["jobDefinitionName"] == name
            assert "jobDefinitionArn" in jd
            assert jd["type"] == "container"
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_describe_job_definitions_by_arn(self, batch):
        """Describe job definitions using ARNs."""
        name = _unique("arn-jd")
        reg_resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        arn = reg_resp["jobDefinitionArn"]
        try:
            resp = batch.describe_job_definitions(jobDefinitions=[arn])
            assert len(resp["jobDefinitions"]) >= 1
            assert resp["jobDefinitions"][0]["jobDefinitionArn"] == arn
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_job_definition_revisions(self, batch):
        name = _unique("rev-jd")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        resp2 = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 2, "memory": 256},
        )
        try:
            assert resp2["revision"] == 2
            resp = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
            revisions = [d["revision"] for d in resp["jobDefinitions"]]
            assert 1 in revisions
            assert 2 in revisions
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")
            batch.deregister_job_definition(jobDefinition=f"{name}:2")

    def test_deregister_job_definition(self, batch):
        """Deregistering makes the job definition INACTIVE."""
        name = _unique("dereg-jd")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        batch.deregister_job_definition(jobDefinition=f"{name}:1")
        resp = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        active_names = [d["jobDefinitionName"] for d in resp["jobDefinitions"]]
        assert name not in active_names


class TestJobQueues:
    @pytest.fixture
    def compute_env(self, batch):
        name = _unique("jq-ce")
        arn = _create_compute_env(batch, name)
        yield arn, name
        try:
            batch.delete_compute_environment(computeEnvironment=name)
        except Exception:
            pass

    def test_create_job_queue(self, batch, compute_env):
        ce_arn, _ = compute_env
        name = _unique("jq")
        resp = batch.create_job_queue(
            jobQueueName=name,
            priority=1,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": ce_arn},
            ],
        )
        assert resp["jobQueueName"] == name
        assert "jobQueueArn" in resp
        batch.delete_job_queue(jobQueue=name)

    def test_create_job_queue_with_priority(self, batch, compute_env):
        """Create a job queue with a specific priority."""
        ce_arn, _ = compute_env
        name = _unique("jq-pri")
        batch.create_job_queue(
            jobQueueName=name,
            priority=10,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": ce_arn},
            ],
        )
        try:
            resp = batch.describe_job_queues(jobQueues=[name])
            assert resp["jobQueues"][0]["priority"] == 10
        finally:
            batch.delete_job_queue(jobQueue=name)

    def test_describe_job_queues(self, batch, compute_env):
        ce_arn, _ = compute_env
        name = _unique("desc-jq")
        batch.create_job_queue(
            jobQueueName=name,
            priority=1,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": ce_arn},
            ],
        )
        try:
            resp = batch.describe_job_queues(jobQueues=[name])
            assert len(resp["jobQueues"]) >= 1
            jq = resp["jobQueues"][0]
            assert jq["jobQueueName"] == name
            assert "jobQueueArn" in jq
            assert "priority" in jq
        finally:
            batch.delete_job_queue(jobQueue=name)

    def test_describe_job_queues_all(self, batch, compute_env):
        """Describe all job queues without filtering."""
        ce_arn, _ = compute_env
        name = _unique("all-jq")
        batch.create_job_queue(
            jobQueueName=name,
            priority=1,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": ce_arn},
            ],
        )
        try:
            resp = batch.describe_job_queues()
            names = [jq["jobQueueName"] for jq in resp["jobQueues"]]
            assert name in names
        finally:
            batch.delete_job_queue(jobQueue=name)

    def test_delete_job_queue(self, batch, compute_env):
        ce_arn, _ = compute_env
        name = _unique("del-jq")
        batch.create_job_queue(
            jobQueueName=name,
            priority=1,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": ce_arn},
            ],
        )
        batch.delete_job_queue(jobQueue=name)
        resp = batch.describe_job_queues(jobQueues=[name])
        assert len(resp["jobQueues"]) == 0


class TestJobs:
    @pytest.fixture
    def job_infra(self, batch):
        ce_name = _unique("job-ce")
        ce_arn = _create_compute_env(batch, ce_name)

        jq_name = _unique("job-jq")
        batch.create_job_queue(
            jobQueueName=jq_name,
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
        )

        jd_name = _unique("job-jd")
        batch.register_job_definition(
            jobDefinitionName=jd_name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )

        yield jq_name, jd_name

        try:
            batch.deregister_job_definition(jobDefinition=f"{jd_name}:1")
        except Exception:
            pass
        try:
            batch.delete_job_queue(jobQueue=jq_name)
        except Exception:
            pass
        try:
            batch.delete_compute_environment(computeEnvironment=ce_name)
        except Exception:
            pass

    def test_submit_job(self, batch, job_infra):
        jq_name, jd_name = job_infra
        resp = batch.submit_job(
            jobName=_unique("job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        assert "jobId" in resp
        assert "jobName" in resp

    def test_submit_job_with_overrides(self, batch, job_infra):
        """Submit a job with container overrides."""
        jq_name, jd_name = job_infra
        resp = batch.submit_job(
            jobName=_unique("job-ovr"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
            containerOverrides={
                "vcpus": 2,
                "memory": 256,
                "environment": [
                    {"name": "OVERRIDE_VAR", "value": "override_value"},
                ],
            },
        )
        assert "jobId" in resp

    def test_describe_jobs(self, batch, job_infra):
        jq_name, jd_name = job_infra
        submitted = batch.submit_job(
            jobName=_unique("desc-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        resp = batch.describe_jobs(jobs=[job_id])
        assert len(resp["jobs"]) == 1
        job = resp["jobs"][0]
        assert job["jobId"] == job_id
        assert "jobName" in job
        assert "status" in job
        assert "jobDefinition" in job
        assert "jobQueue" in job

    def test_describe_jobs_multiple(self, batch, job_infra):
        """Describe multiple jobs at once."""
        jq_name, jd_name = job_infra
        id1 = batch.submit_job(
            jobName=_unique("multi-1"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )["jobId"]
        id2 = batch.submit_job(
            jobName=_unique("multi-2"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )["jobId"]
        resp = batch.describe_jobs(jobs=[id1, id2])
        returned_ids = {j["jobId"] for j in resp["jobs"]}
        assert id1 in returned_ids
        assert id2 in returned_ids

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_list_jobs(self, batch, job_infra):
        jq_name, jd_name = job_infra
        batch.submit_job(
            jobName=_unique("list-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        resp = batch.list_jobs(jobQueue=jq_name)
        assert "jobSummaryList" in resp
        assert len(resp["jobSummaryList"]) >= 1

    @pytest.mark.xfail(reason="Not yet implemented")
    def test_list_jobs_has_summary_fields(self, batch, job_infra):
        """Verify job summary list entries have expected fields."""
        jq_name, jd_name = job_infra
        batch.submit_job(
            jobName=_unique("fields-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        resp = batch.list_jobs(jobQueue=jq_name)
        assert len(resp["jobSummaryList"]) >= 1
        summary = resp["jobSummaryList"][0]
        assert "jobId" in summary
        assert "jobName" in summary

    def test_cancel_job(self, batch, job_infra):
        """Cancel a submitted job."""
        jq_name, jd_name = job_infra
        submitted = batch.submit_job(
            jobName=_unique("cancel-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        batch.cancel_job(jobId=job_id, reason="testing cancellation")
        resp = batch.describe_jobs(jobs=[job_id])
        # Cancelled jobs should not be in RUNNABLE/RUNNING
        assert resp["jobs"][0]["status"] in ("FAILED", "SUCCEEDED", "CANCELLED")

    def test_terminate_job(self, batch, job_infra):
        """Terminate a submitted job."""
        jq_name, jd_name = job_infra
        submitted = batch.submit_job(
            jobName=_unique("term-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        batch.terminate_job(jobId=job_id, reason="testing termination")
        resp = batch.describe_jobs(jobs=[job_id])
        assert resp["jobs"][0]["status"] in ("FAILED", "SUCCEEDED")
