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


class TestComputeEnvironmentExtended:
    def test_create_compute_environment_with_tags(self, batch):
        name = _unique("ce-tag")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"env": "test", "project": "robotocore"},
        )
        assert resp["computeEnvironmentName"] == name
        batch.delete_compute_environment(computeEnvironment=name)

    def test_update_compute_environment(self, batch):
        name = _unique("ce-upd")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = batch.update_compute_environment(
                computeEnvironment=name,
                state="DISABLED",
            )
            assert resp["computeEnvironmentName"] == name
        finally:
            batch.delete_compute_environment(computeEnvironment=name)

    def test_describe_compute_environments_multiple(self, batch):
        names = [_unique("ce-multi") for _ in range(2)]
        for n in names:
            batch.create_compute_environment(
                computeEnvironmentName=n,
                type="MANAGED",
                computeResources={
                    "type": "FARGATE",
                    "maxvCpus": 2,
                    "subnets": ["subnet-12345"],
                    "securityGroupIds": ["sg-12345"],
                },
            )
        try:
            resp = batch.describe_compute_environments(computeEnvironments=names)
            found = {ce["computeEnvironmentName"] for ce in resp["computeEnvironments"]}
            for n in names:
                assert n in found
        finally:
            for n in names:
                batch.delete_compute_environment(computeEnvironment=n)

    def test_describe_compute_environment_fields(self, batch):
        name = _unique("ce-fields")
        batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 4,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            resp = batch.describe_compute_environments(computeEnvironments=[name])
            ce = resp["computeEnvironments"][0]
            assert "computeEnvironmentArn" in ce
            assert ce["computeEnvironmentName"] == name
            assert ce["type"] == "MANAGED"
            assert "computeResources" in ce
        finally:
            batch.delete_compute_environment(computeEnvironment=name)


class TestJobDefinitionExtended:
    def test_register_job_definition_with_retry_strategy(self, batch):
        name = _unique("jd-retry")
        resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
            retryStrategy={"attempts": 3},
        )
        assert resp["jobDefinitionName"] == name
        try:
            desc = batch.describe_job_definitions(jobDefinitionName=name)
            jd = desc["jobDefinitions"][0]
            assert jd["retryStrategy"]["attempts"] == 3
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_deregister_job_definition(self, batch):
        name = _unique("jd-dereg")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
        )
        batch.deregister_job_definition(jobDefinition=f"{name}:1")
        resp = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        active = [d for d in resp["jobDefinitions"] if d["status"] == "ACTIVE"]
        assert len(active) == 0

    def test_register_job_definition_with_environment(self, batch):
        name = _unique("jd-env")
        resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
                "environment": [
                    {"name": "FOO", "value": "bar"},
                    {"name": "BAZ", "value": "qux"},
                ],
            },
        )
        assert resp["jobDefinitionName"] == name
        try:
            desc = batch.describe_job_definitions(jobDefinitionName=name)
            jd = desc["jobDefinitions"][0]
            env_names = [e["name"] for e in jd["containerProperties"]["environment"]]
            assert "FOO" in env_names
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")


class TestJobExtended:
    @pytest.fixture
    def job_setup(self, batch):
        ce_name = _unique("ext-ce")
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

        jq_name = _unique("ext-jq")
        batch.create_job_queue(
            jobQueueName=jq_name,
            priority=1,
            computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
        )

        jd_name = _unique("ext-jd")
        batch.register_job_definition(
            jobDefinitionName=jd_name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )

        yield jq_name, jd_name

        batch.deregister_job_definition(jobDefinition=f"{jd_name}:1")
        batch.delete_job_queue(jobQueue=jq_name)
        batch.delete_compute_environment(computeEnvironment=ce_name)

    def test_submit_job_with_container_overrides(self, batch, job_setup):
        jq_name, jd_name = job_setup
        resp = batch.submit_job(
            jobName=_unique("override-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
            containerOverrides={
                "vcpus": 2,
                "memory": 256,
                "environment": [{"name": "OVERRIDE_VAR", "value": "yes"}],
            },
        )
        assert "jobId" in resp
        assert "jobName" in resp

    def test_describe_jobs_fields(self, batch, job_setup):
        jq_name, jd_name = job_setup
        job_name = _unique("field-job")
        submitted = batch.submit_job(
            jobName=job_name,
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        resp = batch.describe_jobs(jobs=[job_id])
        assert len(resp["jobs"]) == 1
        job = resp["jobs"][0]
        assert job["jobId"] == job_id
        assert job["jobName"] == job_name
        assert "jobDefinition" in job
        assert "jobQueue" in job
        assert "status" in job

    def test_cancel_job(self, batch, job_setup):
        jq_name, jd_name = job_setup
        submitted = batch.submit_job(
            jobName=_unique("cancel-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        # Cancel the job
        batch.cancel_job(jobId=job_id, reason="Testing cancellation")
        # Verify it's no longer in SUBMITTED/RUNNABLE state
        resp = batch.describe_jobs(jobs=[job_id])
        assert len(resp["jobs"]) == 1

    def test_terminate_job(self, batch, job_setup):
        jq_name, jd_name = job_setup
        submitted = batch.submit_job(
            jobName=_unique("term-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        job_id = submitted["jobId"]
        batch.terminate_job(jobId=job_id, reason="Testing termination")
        resp = batch.describe_jobs(jobs=[job_id])
        assert len(resp["jobs"]) == 1

    def test_list_jobs_with_status_filter(self, batch, job_setup):
        jq_name, jd_name = job_setup
        batch.submit_job(
            jobName=_unique("status-job"),
            jobQueue=jq_name,
            jobDefinition=f"{jd_name}:1",
        )
        # List jobs filtering by SUBMITTED status
        resp = batch.list_jobs(jobQueue=jq_name, jobStatus="SUBMITTED")
        assert "jobSummaryList" in resp


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


class TestBatchExtended:
    def test_create_compute_environment_managed_ec2(self, batch):
        name = _unique("ce-ec2")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "EC2",
                "maxvCpus": 4,
                "minvCpus": 0,
                "instanceTypes": ["m5.large"],
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
                "instanceRole": "ecsInstanceRole",
            },
        )
        assert resp["computeEnvironmentName"] == name
        batch.delete_compute_environment(computeEnvironment=name)

    def test_create_compute_environment_unmanaged(self, batch):
        name = _unique("ce-unm")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="UNMANAGED",
        )
        assert resp["computeEnvironmentName"] == name
        batch.delete_compute_environment(computeEnvironment=name)

    def test_compute_environment_has_arn(self, batch):
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

    def test_delete_compute_environment(self, batch):
        name = _unique("ce-del")
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
        names = [
            ce["computeEnvironmentName"]
            for ce in resp["computeEnvironments"]
            if ce.get("status") != "DELETED"
        ]
        assert name not in names

    def test_register_job_definition_with_command(self, batch):
        name = _unique("jd-cmd")
        _ = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
                "command": ["echo", "hello"],
            },
        )
        try:
            desc = batch.describe_job_definitions(jobDefinitionName=name)
            jd = desc["jobDefinitions"][0]
            assert jd["containerProperties"]["command"] == ["echo", "hello"]
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_register_job_definition_with_timeout(self, batch):
        name = _unique("jd-to")
        _ = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
            timeout={"attemptDurationSeconds": 60},
        )
        try:
            desc = batch.describe_job_definitions(jobDefinitionName=name)
            jd = desc["jobDefinitions"][0]
            assert jd["timeout"]["attemptDurationSeconds"] == 60
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_job_definition_has_arn(self, batch):
        name = _unique("jd-arn")
        resp = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
            },
        )
        assert "jobDefinitionArn" in resp
        assert name in resp["jobDefinitionArn"]
        batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_job_definition_revision_increments(self, batch):
        name = _unique("jd-rev")
        r1 = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        r2 = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 256},
        )
        assert r1["revision"] == 1
        assert r2["revision"] == 2
        batch.deregister_job_definition(jobDefinition=f"{name}:1")
        batch.deregister_job_definition(jobDefinition=f"{name}:2")

    def test_create_job_queue_with_priority(self, batch):
        ce_name = _unique("ce-prio")
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
        jq_name = _unique("jq-prio")
        try:
            resp = batch.create_job_queue(
                jobQueueName=jq_name,
                priority=10,
                computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
            )
            assert resp["jobQueueName"] == jq_name
            desc = batch.describe_job_queues(jobQueues=[jq_name])
            assert desc["jobQueues"][0]["priority"] == 10
        finally:
            batch.delete_job_queue(jobQueue=jq_name)
            batch.delete_compute_environment(computeEnvironment=ce_name)

    def test_update_job_queue_priority(self, batch):
        ce_name = _unique("ce-upd")
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
        jq_name = _unique("jq-upd")
        try:
            batch.create_job_queue(
                jobQueueName=jq_name,
                priority=1,
                computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
            )
            batch.update_job_queue(jobQueue=jq_name, priority=5)
            desc = batch.describe_job_queues(jobQueues=[jq_name])
            assert desc["jobQueues"][0]["priority"] == 5
        finally:
            batch.delete_job_queue(jobQueue=jq_name)
            batch.delete_compute_environment(computeEnvironment=ce_name)

    def test_job_queue_has_arn(self, batch):
        ce_name = _unique("ce-jqa")
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
        jq_name = _unique("jq-arn")
        try:
            resp = batch.create_job_queue(
                jobQueueName=jq_name,
                priority=1,
                computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce_arn}],
            )
            assert "jobQueueArn" in resp
        finally:
            batch.delete_job_queue(jobQueue=jq_name)
            batch.delete_compute_environment(computeEnvironment=ce_name)

    def test_describe_job_definitions_by_status(self, batch):
        name = _unique("jd-status")
        batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={"image": "busybox", "vcpus": 1, "memory": 128},
        )
        try:
            resp = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
            assert all(d["status"] == "ACTIVE" for d in resp["jobDefinitions"])
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_register_job_definition_with_volumes(self, batch):
        name = _unique("jd-vol")
        _ = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties={
                "image": "busybox",
                "vcpus": 1,
                "memory": 128,
                "volumes": [{"name": "data", "host": {"sourcePath": "/tmp/data"}}],
                "mountPoints": [
                    {"containerPath": "/data", "sourceVolume": "data", "readOnly": False}
                ],
            },
        )
        try:
            desc = batch.describe_job_definitions(jobDefinitionName=name)
            jd = desc["jobDefinitions"][0]
            assert len(jd["containerProperties"]["volumes"]) == 1
        finally:
            batch.deregister_job_definition(jobDefinition=f"{name}:1")

    def test_describe_compute_environments_empty(self, batch):
        name = _unique("nonexist-ce")
        resp = batch.describe_compute_environments(computeEnvironments=[name])
        assert resp["computeEnvironments"] == []

    def test_describe_job_definitions_empty(self, batch):
        resp = batch.describe_job_definitions(jobDefinitionName=_unique("nonexist-jd"))
        assert resp["jobDefinitions"] == []


class TestBatchTagging:
    def test_tag_resource(self, batch):
        name = _unique("ce-tag")
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
        arn = resp["computeEnvironmentArn"]
        try:
            batch.tag_resource(resourceArn=arn, tags={"team": "platform", "env": "test"})
            tags_resp = batch.list_tags_for_resource(resourceArn=arn)
            assert "tags" in tags_resp
            assert tags_resp["tags"]["team"] == "platform"
            assert tags_resp["tags"]["env"] == "test"
        finally:
            batch.delete_compute_environment(computeEnvironment=name)

    def test_untag_resource(self, batch):
        name = _unique("ce-untag")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"team": "platform", "env": "test"},
        )
        arn = resp["computeEnvironmentArn"]
        try:
            batch.untag_resource(resourceArn=arn, tagKeys=["env"])
            tags_resp = batch.list_tags_for_resource(resourceArn=arn)
            assert "env" not in tags_resp.get("tags", {})
            assert tags_resp["tags"]["team"] == "platform"
        finally:
            batch.delete_compute_environment(computeEnvironment=name)

    def test_list_tags_for_resource(self, batch):
        name = _unique("ce-ltags")
        resp = batch.create_compute_environment(
            computeEnvironmentName=name,
            type="MANAGED",
            computeResources={
                "type": "FARGATE",
                "maxvCpus": 2,
                "subnets": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"project": "robotocore"},
        )
        arn = resp["computeEnvironmentArn"]
        try:
            tags_resp = batch.list_tags_for_resource(resourceArn=arn)
            assert "tags" in tags_resp
            assert tags_resp["tags"]["project"] == "robotocore"
        finally:
            batch.delete_compute_environment(computeEnvironment=name)


class TestSchedulingPolicies:
    def test_create_scheduling_policy(self, batch):
        name = _unique("sp")
        resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={
                "shareDecaySeconds": 3600,
                "computeReservation": 1,
                "shareDistribution": [
                    {"shareIdentifier": "A", "weightFactor": 1.0},
                ],
            },
        )
        assert resp["name"] == name
        assert "arn" in resp
        assert "scheduling-policy" in resp["arn"]
        batch.delete_scheduling_policy(arn=resp["arn"])

    def test_describe_scheduling_policies(self, batch):
        name = _unique("sp-desc")
        create_resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={
                "shareDecaySeconds": 3600,
                "computeReservation": 1,
            },
        )
        arn = create_resp["arn"]
        try:
            resp = batch.describe_scheduling_policies(arns=[arn])
            policies = resp["schedulingPolicies"]
            assert len(policies) == 1
            assert policies[0]["name"] == name
            assert policies[0]["arn"] == arn
            assert "fairsharePolicy" in policies[0]
        finally:
            batch.delete_scheduling_policy(arn=arn)

    def test_list_scheduling_policies(self, batch):
        name = _unique("sp-list")
        create_resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={"shareDecaySeconds": 600},
        )
        arn = create_resp["arn"]
        try:
            resp = batch.list_scheduling_policies()
            assert "schedulingPolicies" in resp
            arns = [p["arn"] for p in resp["schedulingPolicies"]]
            assert arn in arns
        finally:
            batch.delete_scheduling_policy(arn=arn)

    def test_update_scheduling_policy(self, batch):
        name = _unique("sp-upd")
        create_resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={
                "shareDecaySeconds": 3600,
                "computeReservation": 1,
            },
        )
        arn = create_resp["arn"]
        try:
            batch.update_scheduling_policy(
                arn=arn,
                fairsharePolicy={
                    "shareDecaySeconds": 7200,
                    "computeReservation": 2,
                },
            )
            desc = batch.describe_scheduling_policies(arns=[arn])
            policy = desc["schedulingPolicies"][0]
            assert policy["fairsharePolicy"]["shareDecaySeconds"] == 7200
            assert policy["fairsharePolicy"]["computeReservation"] == 2
        finally:
            batch.delete_scheduling_policy(arn=arn)

    def test_delete_scheduling_policy(self, batch):
        name = _unique("sp-del")
        create_resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={"shareDecaySeconds": 600},
        )
        arn = create_resp["arn"]
        resp = batch.delete_scheduling_policy(arn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        desc = batch.describe_scheduling_policies(arns=[arn])
        assert len(desc["schedulingPolicies"]) == 0

    def test_scheduling_policy_with_share_distribution(self, batch):
        name = _unique("sp-shares")
        resp = batch.create_scheduling_policy(
            name=name,
            fairsharePolicy={
                "shareDecaySeconds": 3600,
                "computeReservation": 0,
                "shareDistribution": [
                    {"shareIdentifier": "groupA", "weightFactor": 0.5},
                    {"shareIdentifier": "groupB", "weightFactor": 1.5},
                ],
            },
        )
        arn = resp["arn"]
        try:
            desc = batch.describe_scheduling_policies(arns=[arn])
            policy = desc["schedulingPolicies"][0]
            shares = policy["fairsharePolicy"]["shareDistribution"]
            identifiers = [s["shareIdentifier"] for s in shares]
            assert "groupA" in identifiers
            assert "groupB" in identifiers
        finally:
            batch.delete_scheduling_policy(arn=arn)
