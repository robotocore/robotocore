"""AWS Batch compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def batch():
    return make_client("batch")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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
            assert resp["computeEnvironments"][0]["computeEnvironmentName"] == name
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
            assert resp["jobDefinitions"][0]["jobDefinitionName"] == name
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_job_definition_revisions(self, batch):
        name = _unique("rev-jd")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 2, "memory": 256},
        )
        try:
            resp = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
            revisions = [d["revision"] for d in resp["jobDefinitions"]]
            assert 2 in revisions
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")
            batch.deregister_job_definition(jobDefinition=f"{name}:2")


class TestJobQueues:
    @pytest.fixture
    def compute_env(self, batch):
        name = _unique("jq-ce")
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
        arn = resp["computeEnvironments"][0]["computeEnvironmentArn"]
        yield arn, name
        batch.delete_compute_environment(computeEnvironment=name)

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
            assert resp["jobQueues"][0]["jobQueueName"] == name
        finally:
            batch.delete_job_queue(jobQueue=name)


class TestJobs:
    @pytest.fixture
    def job_infra(self, batch):
        ce_name = _unique("job-ce")
        batch.create_compute_environment(
            computeEnvironmentName=ce_name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        ce_resp = batch.describe_compute_environments(computeEnvironments=[ce_name])
        ce_arn = ce_resp["computeEnvironments"][0]["computeEnvironmentArn"]

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

        batch.deregister_job_definition(jobDefinition=f"{jd_name}:1")
        batch.delete_job_queue(jobQueue=jq_name)
        batch.delete_compute_environment(computeEnvironment=ce_name)

    def test_submit_job(self, batch, job_infra):
        jq_name, jd_name = job_infra
        resp = batch.submit_job(
            jobName=_unique("job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        assert "jobId" in resp
        assert "jobName" in resp

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
        assert resp["jobs"][0]["jobId"] == job_id

    def test_list_jobs(self, batch, job_infra):
        jq_name, jd_name = job_infra
        batch.submit_job(
            jobName=_unique("list-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        resp = batch.list_jobs(jobQueue=jq_name)
        assert "jobSummaryList" in resp
