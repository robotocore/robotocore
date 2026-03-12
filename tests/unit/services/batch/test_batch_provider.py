"""Unit tests for the Batch native provider."""

import pytest

from robotocore.services.batch.provider import (
    BatchError,
    BatchStore,
    _advance_job,
    _cancel_job,
    _create_compute_environment,
    _create_job_queue,
    _delete_compute_environment,
    _delete_job_queue,
    _deregister_job_definition,
    _describe_compute_environments,
    _describe_job_definitions,
    _describe_job_queues,
    _describe_jobs,
    _get_store,
    _list_jobs,
    _list_tags_for_resource,
    _register_job_definition,
    _resolve_job_definition,
    _submit_job,
    _tag_resource,
    _terminate_job,
    _untag_resource,
    _update_compute_environment,
    _update_job_queue,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture()
def store():
    """Fresh BatchStore per test."""
    return BatchStore(REGION, ACCOUNT)


# ---------------------------------------------------------------------------
# Compute Environment CRUD
# ---------------------------------------------------------------------------


class TestCreateComputeEnvironment:
    def test_basic_create(self, store):
        result = _create_compute_environment(
            store,
            {"computeEnvironmentName": "my-ce", "type": "MANAGED"},
            REGION,
            ACCOUNT,
        )
        assert result["computeEnvironmentName"] == "my-ce"
        assert (
            "arn:aws:batch:us-east-1:123456789012:compute-environment/my-ce"
            == result["computeEnvironmentArn"]
        )
        assert "my-ce" in store.compute_envs

    def test_defaults(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "ce1"}, REGION, ACCOUNT)
        ce = store.compute_envs["ce1"]
        assert ce["type"] == "MANAGED"
        assert ce["state"] == "ENABLED"
        assert ce["status"] == "VALID"

    def test_with_tags(self, store):
        _create_compute_environment(
            store,
            {"computeEnvironmentName": "ce-tagged", "tags": {"env": "test"}},
            REGION,
            ACCOUNT,
        )
        arn = store.compute_envs["ce-tagged"]["computeEnvironmentArn"]
        assert store.tags[arn] == {"env": "test"}

    def test_missing_name_raises(self, store):
        with pytest.raises(BatchError, match="computeEnvironmentName is required"):
            _create_compute_environment(store, {}, REGION, ACCOUNT)

    def test_duplicate_name_raises(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "dup"}, REGION, ACCOUNT)
        with pytest.raises(BatchError, match="already exists"):
            _create_compute_environment(store, {"computeEnvironmentName": "dup"}, REGION, ACCOUNT)


class TestDescribeComputeEnvironments:
    def test_describe_all(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "a"}, REGION, ACCOUNT)
        _create_compute_environment(store, {"computeEnvironmentName": "b"}, REGION, ACCOUNT)
        result = _describe_compute_environments(store, {}, REGION, ACCOUNT)
        assert len(result["computeEnvironments"]) == 2

    def test_describe_by_name(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "x"}, REGION, ACCOUNT)
        _create_compute_environment(store, {"computeEnvironmentName": "y"}, REGION, ACCOUNT)
        result = _describe_compute_environments(
            store, {"computeEnvironments": ["x"]}, REGION, ACCOUNT
        )
        assert len(result["computeEnvironments"]) == 1
        assert result["computeEnvironments"][0]["computeEnvironmentName"] == "x"

    def test_describe_by_arn(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "z"}, REGION, ACCOUNT)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:compute-environment/z"
        result = _describe_compute_environments(
            store, {"computeEnvironments": [arn]}, REGION, ACCOUNT
        )
        assert len(result["computeEnvironments"]) == 1

    def test_describe_nonexistent_returns_empty(self, store):
        result = _describe_compute_environments(
            store, {"computeEnvironments": ["nope"]}, REGION, ACCOUNT
        )
        assert result["computeEnvironments"] == []

    def test_describe_empty_store(self, store):
        result = _describe_compute_environments(store, {}, REGION, ACCOUNT)
        assert result["computeEnvironments"] == []


class TestUpdateComputeEnvironment:
    def test_update_state(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "ce1"}, REGION, ACCOUNT)
        result = _update_compute_environment(
            store, {"computeEnvironment": "ce1", "state": "DISABLED"}, REGION, ACCOUNT
        )
        assert result["computeEnvironmentName"] == "ce1"
        assert store.compute_envs["ce1"]["state"] == "DISABLED"

    def test_update_compute_resources(self, store):
        _create_compute_environment(
            store,
            {"computeEnvironmentName": "ce2", "computeResources": {"minvCpus": 0}},
            REGION,
            ACCOUNT,
        )
        _update_compute_environment(
            store,
            {"computeEnvironment": "ce2", "computeResources": {"maxvCpus": 256}},
            REGION,
            ACCOUNT,
        )
        assert store.compute_envs["ce2"]["computeResources"]["maxvCpus"] == 256
        assert store.compute_envs["ce2"]["computeResources"]["minvCpus"] == 0

    def test_update_by_arn(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "ce3"}, REGION, ACCOUNT)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:compute-environment/ce3"
        result = _update_compute_environment(
            store, {"computeEnvironment": arn, "state": "DISABLED"}, REGION, ACCOUNT
        )
        assert result["computeEnvironmentName"] == "ce3"
        assert store.compute_envs["ce3"]["state"] == "DISABLED"

    def test_update_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _update_compute_environment(store, {"computeEnvironment": "missing"}, REGION, ACCOUNT)


class TestDeleteComputeEnvironment:
    def test_delete(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "del"}, REGION, ACCOUNT)
        result = _delete_compute_environment(store, {"computeEnvironment": "del"}, REGION, ACCOUNT)
        assert result == {}
        assert "del" not in store.compute_envs

    def test_delete_cleans_up_tags(self, store):
        _create_compute_environment(
            store,
            {"computeEnvironmentName": "tagged", "tags": {"k": "v"}},
            REGION,
            ACCOUNT,
        )
        arn = store.compute_envs["tagged"]["computeEnvironmentArn"]
        assert arn in store.tags
        _delete_compute_environment(store, {"computeEnvironment": "tagged"}, REGION, ACCOUNT)
        assert arn not in store.tags

    def test_delete_by_arn(self, store):
        _create_compute_environment(store, {"computeEnvironmentName": "ce4"}, REGION, ACCOUNT)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:compute-environment/ce4"
        _delete_compute_environment(store, {"computeEnvironment": arn}, REGION, ACCOUNT)
        assert "ce4" not in store.compute_envs

    def test_delete_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _delete_compute_environment(store, {"computeEnvironment": "ghost"}, REGION, ACCOUNT)


# ---------------------------------------------------------------------------
# Job Queue CRUD
# ---------------------------------------------------------------------------


class TestCreateJobQueue:
    def test_basic_create(self, store):
        result = _create_job_queue(store, {"jobQueueName": "q1", "priority": 10}, REGION, ACCOUNT)
        assert result["jobQueueName"] == "q1"
        assert "arn:aws:batch" in result["jobQueueArn"]
        assert store.job_queues["q1"]["priority"] == 10

    def test_defaults(self, store):
        _create_job_queue(store, {"jobQueueName": "q-default"}, REGION, ACCOUNT)
        q = store.job_queues["q-default"]
        assert q["state"] == "ENABLED"
        assert q["priority"] == 1
        assert q["status"] == "VALID"

    def test_with_tags(self, store):
        _create_job_queue(
            store, {"jobQueueName": "q-tag", "tags": {"team": "infra"}}, REGION, ACCOUNT
        )
        arn = store.job_queues["q-tag"]["jobQueueArn"]
        assert store.tags[arn] == {"team": "infra"}

    def test_missing_name_raises(self, store):
        with pytest.raises(BatchError, match="jobQueueName is required"):
            _create_job_queue(store, {}, REGION, ACCOUNT)

    def test_duplicate_name_raises(self, store):
        _create_job_queue(store, {"jobQueueName": "dup"}, REGION, ACCOUNT)
        with pytest.raises(BatchError, match="already exists"):
            _create_job_queue(store, {"jobQueueName": "dup"}, REGION, ACCOUNT)


class TestDescribeJobQueues:
    def test_describe_all(self, store):
        _create_job_queue(store, {"jobQueueName": "a"}, REGION, ACCOUNT)
        _create_job_queue(store, {"jobQueueName": "b"}, REGION, ACCOUNT)
        result = _describe_job_queues(store, {}, REGION, ACCOUNT)
        assert len(result["jobQueues"]) == 2

    def test_describe_by_name(self, store):
        _create_job_queue(store, {"jobQueueName": "q1"}, REGION, ACCOUNT)
        _create_job_queue(store, {"jobQueueName": "q2"}, REGION, ACCOUNT)
        result = _describe_job_queues(store, {"jobQueues": ["q1"]}, REGION, ACCOUNT)
        assert len(result["jobQueues"]) == 1
        assert result["jobQueues"][0]["jobQueueName"] == "q1"

    def test_describe_by_arn(self, store):
        _create_job_queue(store, {"jobQueueName": "q3"}, REGION, ACCOUNT)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:job-queue/q3"
        result = _describe_job_queues(store, {"jobQueues": [arn]}, REGION, ACCOUNT)
        assert len(result["jobQueues"]) == 1

    def test_describe_nonexistent_returns_empty(self, store):
        result = _describe_job_queues(store, {"jobQueues": ["nope"]}, REGION, ACCOUNT)
        assert result["jobQueues"] == []


class TestUpdateJobQueue:
    def test_update_state(self, store):
        _create_job_queue(store, {"jobQueueName": "q1"}, REGION, ACCOUNT)
        result = _update_job_queue(store, {"jobQueue": "q1", "state": "DISABLED"}, REGION, ACCOUNT)
        assert result["jobQueueName"] == "q1"
        assert store.job_queues["q1"]["state"] == "DISABLED"

    def test_update_priority(self, store):
        _create_job_queue(store, {"jobQueueName": "q2", "priority": 1}, REGION, ACCOUNT)
        _update_job_queue(store, {"jobQueue": "q2", "priority": 99}, REGION, ACCOUNT)
        assert store.job_queues["q2"]["priority"] == 99

    def test_update_compute_env_order(self, store):
        _create_job_queue(store, {"jobQueueName": "q3"}, REGION, ACCOUNT)
        new_order = [{"computeEnvironment": "ce1", "order": 1}]
        _update_job_queue(
            store,
            {"jobQueue": "q3", "computeEnvironmentOrder": new_order},
            REGION,
            ACCOUNT,
        )
        assert store.job_queues["q3"]["computeEnvironmentOrder"] == new_order

    def test_update_by_arn(self, store):
        _create_job_queue(store, {"jobQueueName": "q4"}, REGION, ACCOUNT)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT}:job-queue/q4"
        _update_job_queue(store, {"jobQueue": arn, "state": "DISABLED"}, REGION, ACCOUNT)
        assert store.job_queues["q4"]["state"] == "DISABLED"

    def test_update_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _update_job_queue(store, {"jobQueue": "ghost"}, REGION, ACCOUNT)


class TestDeleteJobQueue:
    def test_delete(self, store):
        _create_job_queue(store, {"jobQueueName": "q1"}, REGION, ACCOUNT)
        result = _delete_job_queue(store, {"jobQueue": "q1"}, REGION, ACCOUNT)
        assert result == {}
        assert "q1" not in store.job_queues

    def test_delete_cleans_tags(self, store):
        _create_job_queue(store, {"jobQueueName": "qt", "tags": {"a": "b"}}, REGION, ACCOUNT)
        arn = store.job_queues["qt"]["jobQueueArn"]
        assert arn in store.tags
        _delete_job_queue(store, {"jobQueue": "qt"}, REGION, ACCOUNT)
        assert arn not in store.tags

    def test_delete_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _delete_job_queue(store, {"jobQueue": "nope"}, REGION, ACCOUNT)


# ---------------------------------------------------------------------------
# Job Definitions
# ---------------------------------------------------------------------------


class TestRegisterJobDefinition:
    def test_basic_register(self, store):
        result = _register_job_definition(
            store, {"jobDefinitionName": "jd1", "type": "container"}, REGION, ACCOUNT
        )
        assert result["jobDefinitionName"] == "jd1"
        assert result["revision"] == 1
        assert ":1" in result["jobDefinitionArn"]

    def test_revision_increments(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        result2 = _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        assert result2["revision"] == 2
        assert ":2" in result2["jobDefinitionArn"]

    def test_defaults(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd-def"}, REGION, ACCOUNT)
        jd = store.job_definitions["jd-def"][0]
        assert jd["type"] == "container"
        assert jd["status"] == "ACTIVE"
        assert jd["retryStrategy"] == {"attempts": 1}

    def test_with_tags(self, store):
        _register_job_definition(
            store,
            {"jobDefinitionName": "jd-tagged", "tags": {"ver": "1"}},
            REGION,
            ACCOUNT,
        )
        arn = store.job_definitions["jd-tagged"][0]["jobDefinitionArn"]
        assert store.tags[arn] == {"ver": "1"}

    def test_missing_name_raises(self, store):
        with pytest.raises(BatchError, match="jobDefinitionName is required"):
            _register_job_definition(store, {}, REGION, ACCOUNT)


class TestDescribeJobDefinitions:
    def test_describe_all_active(self, store):
        _register_job_definition(store, {"jobDefinitionName": "a"}, REGION, ACCOUNT)
        _register_job_definition(store, {"jobDefinitionName": "b"}, REGION, ACCOUNT)
        result = _describe_job_definitions(store, {}, REGION, ACCOUNT)
        assert len(result["jobDefinitions"]) == 2

    def test_describe_by_name_filter(self, store):
        _register_job_definition(store, {"jobDefinitionName": "x"}, REGION, ACCOUNT)
        _register_job_definition(store, {"jobDefinitionName": "y"}, REGION, ACCOUNT)
        result = _describe_job_definitions(store, {"jobDefinitionName": "x"}, REGION, ACCOUNT)
        assert len(result["jobDefinitions"]) == 1
        assert result["jobDefinitions"][0]["jobDefinitionName"] == "x"

    def test_describe_by_ref_list(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        result = _describe_job_definitions(store, {"jobDefinitions": ["jd:1"]}, REGION, ACCOUNT)
        assert len(result["jobDefinitions"]) == 1
        assert result["jobDefinitions"][0]["revision"] == 1

    def test_describe_filters_inactive(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _deregister_job_definition(store, {"jobDefinition": "jd:1"}, REGION, ACCOUNT)
        result = _describe_job_definitions(store, {}, REGION, ACCOUNT)
        assert len(result["jobDefinitions"]) == 0

    def test_describe_inactive_with_status_filter(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _deregister_job_definition(store, {"jobDefinition": "jd:1"}, REGION, ACCOUNT)
        result = _describe_job_definitions(
            store, {"jobDefinitionName": "jd", "status": "INACTIVE"}, REGION, ACCOUNT
        )
        assert len(result["jobDefinitions"]) == 1


class TestDeregisterJobDefinition:
    def test_deregister_by_name_revision(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        result = _deregister_job_definition(store, {"jobDefinition": "jd:1"}, REGION, ACCOUNT)
        assert result == {}
        assert store.job_definitions["jd"][0]["status"] == "INACTIVE"

    def test_deregister_by_arn(self, store):
        res = _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        arn = res["jobDefinitionArn"]
        _deregister_job_definition(store, {"jobDefinition": arn}, REGION, ACCOUNT)
        assert store.job_definitions["jd"][0]["status"] == "INACTIVE"

    def test_deregister_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _deregister_job_definition(store, {"jobDefinition": "nope:1"}, REGION, ACCOUNT)


# ---------------------------------------------------------------------------
# Resolve Job Definition Helper
# ---------------------------------------------------------------------------


class TestResolveJobDefinition:
    def test_resolve_by_name_gets_latest_active(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        jd = _resolve_job_definition(store, "jd")
        assert jd is not None
        assert jd["revision"] == 2

    def test_resolve_by_name_revision(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        jd = _resolve_job_definition(store, "jd:1")
        assert jd is not None
        assert jd["revision"] == 1

    def test_resolve_by_arn(self, store):
        res = _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        jd = _resolve_job_definition(store, res["jobDefinitionArn"])
        assert jd is not None
        assert jd["revision"] == 1

    def test_resolve_nonexistent_returns_none(self, store):
        assert _resolve_job_definition(store, "nope") is None

    def test_resolve_invalid_revision_returns_none(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        assert _resolve_job_definition(store, "jd:abc") is None

    def test_resolve_by_name_skips_inactive(self, store):
        _register_job_definition(store, {"jobDefinitionName": "jd"}, REGION, ACCOUNT)
        _deregister_job_definition(store, {"jobDefinition": "jd:1"}, REGION, ACCOUNT)
        assert _resolve_job_definition(store, "jd") is None


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


class TestSubmitJob:
    def test_basic_submit(self, store):
        result = _submit_job(
            store,
            {"jobName": "j1", "jobQueue": "q1", "jobDefinition": "jd:1"},
            REGION,
            ACCOUNT,
        )
        assert result["jobName"] == "j1"
        assert "jobId" in result
        assert "jobArn" in result
        # Job advances to SUCCEEDED immediately
        job = store.jobs[result["jobId"]]
        assert job["status"] == "SUCCEEDED"

    def test_submit_stores_metadata(self, store):
        result = _submit_job(
            store,
            {
                "jobName": "j2",
                "jobQueue": "q1",
                "jobDefinition": "jd:1",
                "parameters": {"key": "val"},
                "containerOverrides": {"vcpus": 2},
                "tags": {"env": "dev"},
            },
            REGION,
            ACCOUNT,
        )
        job = store.jobs[result["jobId"]]
        assert job["parameters"] == {"key": "val"}
        assert job["container"] == {"vcpus": 2}
        assert job["tags"] == {"env": "dev"}

    def test_submit_missing_name_raises(self, store):
        with pytest.raises(BatchError, match="jobName is required"):
            _submit_job(store, {"jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT)

    def test_submit_with_tags_stored(self, store):
        result = _submit_job(
            store,
            {"jobName": "jt", "jobQueue": "q", "jobDefinition": "jd:1", "tags": {"k": "v"}},
            REGION,
            ACCOUNT,
        )
        arn = result["jobArn"]
        assert store.tags[arn] == {"k": "v"}

    def test_submit_timestamps_set(self, store):
        result = _submit_job(
            store,
            {"jobName": "jts", "jobQueue": "q", "jobDefinition": "jd:1"},
            REGION,
            ACCOUNT,
        )
        job = store.jobs[result["jobId"]]
        assert job["createdAt"] > 0
        assert job["startedAt"] > 0
        assert job["stoppedAt"] > 0


class TestDescribeJobs:
    def test_describe_existing(self, store):
        res = _submit_job(
            store,
            {"jobName": "j1", "jobQueue": "q", "jobDefinition": "jd:1"},
            REGION,
            ACCOUNT,
        )
        result = _describe_jobs(store, {"jobs": [res["jobId"]]}, REGION, ACCOUNT)
        assert len(result["jobs"]) == 1
        assert result["jobs"][0]["jobName"] == "j1"

    def test_describe_nonexistent_id(self, store):
        result = _describe_jobs(store, {"jobs": ["no-such-id"]}, REGION, ACCOUNT)
        assert result["jobs"] == []

    def test_describe_multiple(self, store):
        r1 = _submit_job(
            store, {"jobName": "a", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        r2 = _submit_job(
            store, {"jobName": "b", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        result = _describe_jobs(store, {"jobs": [r1["jobId"], r2["jobId"]]}, REGION, ACCOUNT)
        assert len(result["jobs"]) == 2


class TestListJobs:
    def test_list_all(self, store):
        _submit_job(
            store, {"jobName": "j1", "jobQueue": "q1", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        result = _list_jobs(store, {}, REGION, ACCOUNT)
        assert len(result["jobSummaryList"]) == 1
        assert result["jobSummaryList"][0]["jobName"] == "j1"

    def test_list_by_queue(self, store):
        _submit_job(
            store, {"jobName": "a", "jobQueue": "q1", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        _submit_job(
            store, {"jobName": "b", "jobQueue": "q2", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        result = _list_jobs(store, {"jobQueue": "q1"}, REGION, ACCOUNT)
        assert len(result["jobSummaryList"]) == 1
        assert result["jobSummaryList"][0]["jobName"] == "a"

    def test_list_by_status(self, store):
        _submit_job(
            store, {"jobName": "j1", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        # Job is SUCCEEDED after submit
        result = _list_jobs(store, {"jobStatus": "SUCCEEDED"}, REGION, ACCOUNT)
        assert len(result["jobSummaryList"]) == 1
        result2 = _list_jobs(store, {"jobStatus": "SUBMITTED"}, REGION, ACCOUNT)
        assert len(result2["jobSummaryList"]) == 0

    def test_list_empty(self, store):
        result = _list_jobs(store, {}, REGION, ACCOUNT)
        assert result["jobSummaryList"] == []

    def test_list_summary_fields(self, store):
        _submit_job(
            store, {"jobName": "j1", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        summary = _list_jobs(store, {}, REGION, ACCOUNT)["jobSummaryList"][0]
        assert "jobArn" in summary
        assert "jobId" in summary
        assert "jobName" in summary
        assert "status" in summary
        assert "createdAt" in summary


class TestTerminateJob:
    def test_terminate(self, store):
        res = _submit_job(
            store, {"jobName": "j1", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        result = _terminate_job(
            store, {"jobId": res["jobId"], "reason": "test stop"}, REGION, ACCOUNT
        )
        assert result == {}
        job = store.jobs[res["jobId"]]
        assert job["status"] == "FAILED"
        assert job["statusReason"] == "test stop"
        assert job["stoppedAt"] > 0

    def test_terminate_default_reason(self, store):
        res = _submit_job(
            store, {"jobName": "j1", "jobQueue": "q", "jobDefinition": "jd:1"}, REGION, ACCOUNT
        )
        _terminate_job(store, {"jobId": res["jobId"]}, REGION, ACCOUNT)
        assert store.jobs[res["jobId"]]["statusReason"] == "Terminated by user"

    def test_terminate_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _terminate_job(store, {"jobId": "nope"}, REGION, ACCOUNT)


class TestCancelJob:
    def test_cancel_submitted_job(self, store):
        # Manually create a SUBMITTED job (don't use _submit_job which advances to SUCCEEDED)
        store.jobs["j1"] = {
            "jobArn": "arn:aws:batch:us-east-1:123456789012:job/j1",
            "jobId": "j1",
            "jobName": "test",
            "jobQueue": "q",
            "jobDefinition": "jd:1",
            "status": "SUBMITTED",
            "statusReason": "",
            "createdAt": 0,
            "startedAt": 0,
            "stoppedAt": 0,
            "dependsOn": [],
            "parameters": {},
            "container": {},
            "tags": {},
        }
        result = _cancel_job(store, {"jobId": "j1", "reason": "no longer needed"}, REGION, ACCOUNT)
        assert result == {}
        assert store.jobs["j1"]["status"] == "FAILED"
        assert store.jobs["j1"]["statusReason"] == "no longer needed"

    def test_cancel_running_job_raises(self, store):
        store.jobs["j2"] = {
            "jobArn": "arn",
            "jobId": "j2",
            "jobName": "test",
            "jobQueue": "q",
            "jobDefinition": "jd:1",
            "status": "RUNNING",
            "statusReason": "",
            "createdAt": 0,
            "startedAt": 0,
            "stoppedAt": 0,
            "dependsOn": [],
            "parameters": {},
            "container": {},
            "tags": {},
        }
        with pytest.raises(BatchError, match="Cannot cancel"):
            _cancel_job(store, {"jobId": "j2"}, REGION, ACCOUNT)

    def test_cancel_starting_job_raises(self, store):
        store.jobs["j3"] = {
            "jobArn": "arn",
            "jobId": "j3",
            "jobName": "test",
            "jobQueue": "q",
            "jobDefinition": "jd:1",
            "status": "STARTING",
            "statusReason": "",
            "createdAt": 0,
            "startedAt": 0,
            "stoppedAt": 0,
            "dependsOn": [],
            "parameters": {},
            "container": {},
            "tags": {},
        }
        with pytest.raises(BatchError, match="Cannot cancel"):
            _cancel_job(store, {"jobId": "j3"}, REGION, ACCOUNT)

    def test_cancel_not_found_raises(self, store):
        with pytest.raises(BatchError, match="not found"):
            _cancel_job(store, {"jobId": "nope"}, REGION, ACCOUNT)


# ---------------------------------------------------------------------------
# Advance Job Helper
# ---------------------------------------------------------------------------


class TestAdvanceJob:
    def test_advances_to_succeeded(self, store):
        store.jobs["j1"] = {
            "jobArn": "arn",
            "jobId": "j1",
            "jobName": "test",
            "status": "SUBMITTED",
            "startedAt": 0,
            "stoppedAt": 0,
        }
        _advance_job(store, "j1")
        assert store.jobs["j1"]["status"] == "SUCCEEDED"
        assert store.jobs["j1"]["startedAt"] > 0
        assert store.jobs["j1"]["stoppedAt"] > 0

    def test_advance_nonexistent_is_noop(self, store):
        _advance_job(store, "no-such-job")  # Should not raise


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


class TestTagging:
    def _make_ce(self, store, name="ce1"):
        _create_compute_environment(store, {"computeEnvironmentName": name}, REGION, ACCOUNT)
        return store.compute_envs[name]["computeEnvironmentArn"]

    def test_tag_resource(self, store):
        arn = self._make_ce(store)
        _tag_resource(store, arn, {"tags": {"k1": "v1", "k2": "v2"}})
        assert store.tags[arn] == {"k1": "v1", "k2": "v2"}

    def test_tag_resource_merges(self, store):
        arn = self._make_ce(store)
        _tag_resource(store, arn, {"tags": {"k1": "v1"}})
        _tag_resource(store, arn, {"tags": {"k2": "v2"}})
        assert store.tags[arn] == {"k1": "v1", "k2": "v2"}

    def test_tag_resource_not_found_raises(self, store):
        with pytest.raises(BatchError, match="Resource not found"):
            _tag_resource(store, "arn:aws:batch:us-east-1:123456789012:fake/nope", {"tags": {}})

    def test_untag_resource(self, store):
        arn = self._make_ce(store)
        _tag_resource(store, arn, {"tags": {"k1": "v1", "k2": "v2"}})
        _untag_resource(store, arn, ["k1"])
        assert store.tags[arn] == {"k2": "v2"}

    def test_untag_nonexistent_key_is_noop(self, store):
        arn = self._make_ce(store)
        _tag_resource(store, arn, {"tags": {"k1": "v1"}})
        _untag_resource(store, arn, ["nope"])
        assert store.tags[arn] == {"k1": "v1"}

    def test_untag_resource_not_found_raises(self, store):
        with pytest.raises(BatchError, match="Resource not found"):
            _untag_resource(store, "arn:aws:batch:us-east-1:123456789012:fake/nope", ["k"])

    def test_list_tags(self, store):
        arn = self._make_ce(store)
        _tag_resource(store, arn, {"tags": {"k1": "v1"}})
        result = _list_tags_for_resource(store, arn)
        assert result == {"tags": {"k1": "v1"}}

    def test_list_tags_empty(self, store):
        arn = self._make_ce(store)
        result = _list_tags_for_resource(store, arn)
        assert result == {"tags": {}}

    def test_list_tags_not_found_raises(self, store):
        with pytest.raises(BatchError, match="Resource not found"):
            _list_tags_for_resource(store, "arn:aws:batch:us-east-1:123456789012:fake/nope")


# ---------------------------------------------------------------------------
# Store isolation
# ---------------------------------------------------------------------------


class TestStoreIsolation:
    def test_get_store_returns_same_for_same_key(self):
        s1 = _get_store("us-east-1", "111111111111")
        s2 = _get_store("us-east-1", "111111111111")
        assert s1 is s2

    def test_get_store_different_region(self):
        s1 = _get_store("us-east-1", "222222222222")
        s2 = _get_store("eu-west-1", "222222222222")
        assert s1 is not s2

    def test_get_store_different_account(self):
        s1 = _get_store("us-east-1", "333333333333")
        s2 = _get_store("us-east-1", "444444444444")
        assert s1 is not s2


# ---------------------------------------------------------------------------
# BatchError
# ---------------------------------------------------------------------------


class TestBatchError:
    def test_error_fields(self):
        e = BatchError("ClientException", "something broke", 400)
        assert e.code == "ClientException"
        assert e.message == "something broke"
        assert e.status == 400

    def test_error_default_status(self):
        e = BatchError("Err", "msg")
        assert e.status == 400
