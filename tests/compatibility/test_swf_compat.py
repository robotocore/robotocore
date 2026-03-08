"""SWF compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def swf():
    return make_client("swf")


def _uid():
    return uuid.uuid4().hex[:8]


class TestSWFOperations:
    def test_register_domain(self, swf):
        name = f"test-domain-{_uid()}"
        response = swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        swf.deprecate_domain(name=name)

    def test_list_domains(self, swf):
        name = f"list-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        response = swf.list_domains(registrationStatus="REGISTERED")
        domain_names = [d["name"] for d in response["domainInfos"]]
        assert name in domain_names
        swf.deprecate_domain(name=name)

    def test_describe_domain(self, swf):
        name = f"describe-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="60",
        )
        response = swf.describe_domain(name=name)
        assert response["domainInfo"]["name"] == name
        assert response["domainInfo"]["status"] == "REGISTERED"
        assert response["configuration"]["workflowExecutionRetentionPeriodInDays"] == "60"
        swf.deprecate_domain(name=name)

    def test_deprecate_domain(self, swf):
        name = f"deprecate-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        swf.deprecate_domain(name=name)
        response = swf.describe_domain(name=name)
        assert response["domainInfo"]["status"] == "DEPRECATED"

    def test_register_workflow_type(self, swf):
        name = f"workflow-domain-{_uid()}"
        swf.register_domain(
            name=name,
            workflowExecutionRetentionPeriodInDays="30",
        )
        response = swf.register_workflow_type(
            domain=name,
            name="test-workflow",
            version="1.0",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        types = swf.list_workflow_types(
            domain=name,
            registrationStatus="REGISTERED",
        )
        type_names = [t["workflowType"]["name"] for t in types["typeInfos"]]
        assert "test-workflow" in type_names

        swf.deprecate_workflow_type(
            domain=name,
            workflowType={"name": "test-workflow", "version": "1.0"},
        )
        swf.deprecate_domain(name=name)

    def test_register_activity_type(self, swf):
        domain_name = f"activity-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        response = swf.register_activity_type(
            domain=domain_name, name="test-activity", version="1.0"
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        types = swf.list_activity_types(domain=domain_name, registrationStatus="REGISTERED")
        names = [t["activityType"]["name"] for t in types["typeInfos"]]
        assert "test-activity" in names
        swf.deprecate_activity_type(
            domain=domain_name,
            activityType={"name": "test-activity", "version": "1.0"},
        )
        swf.deprecate_domain(name=domain_name)

    def test_list_deprecated_domains(self, swf):
        domain_name = f"dep-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.deprecate_domain(name=domain_name)
        response = swf.list_domains(registrationStatus="DEPRECATED")
        names = [d["name"] for d in response["domainInfos"]]
        assert domain_name in names

    def test_describe_workflow_type(self, swf):
        domain_name = f"desc-wf-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(domain=domain_name, name="desc-workflow", version="1.0")
        response = swf.describe_workflow_type(
            domain=domain_name,
            workflowType={"name": "desc-workflow", "version": "1.0"},
        )
        assert response["typeInfo"]["workflowType"]["name"] == "desc-workflow"
        swf.deprecate_workflow_type(
            domain=domain_name,
            workflowType={"name": "desc-workflow", "version": "1.0"},
        )
        swf.deprecate_domain(name=domain_name)

    def test_describe_activity_type(self, swf):
        domain_name = f"desc-act-domain-{_uid()}"
        swf.register_domain(name=domain_name, workflowExecutionRetentionPeriodInDays="30")
        swf.register_activity_type(domain=domain_name, name="desc-activity", version="2.0")
        response = swf.describe_activity_type(
            domain=domain_name,
            activityType={"name": "desc-activity", "version": "2.0"},
        )
        assert response["typeInfo"]["activityType"]["name"] == "desc-activity"
        swf.deprecate_activity_type(
            domain=domain_name,
            activityType={"name": "desc-activity", "version": "2.0"},
        )
        swf.deprecate_domain(name=domain_name)

    def test_register_multiple_workflow_versions(self, swf):
        domain = f"ver-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        try:
            swf.register_workflow_type(domain=domain, name="ver-wf", version="1.0")
            swf.register_workflow_type(domain=domain, name="ver-wf", version="2.0")
            types = swf.list_workflow_types(domain=domain, registrationStatus="REGISTERED")
            versions = [
                t["workflowType"]["version"]
                for t in types["typeInfos"]
                if t["workflowType"]["name"] == "ver-wf"
            ]
            assert "1.0" in versions
            assert "2.0" in versions
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "ver-wf", "version": "1.0"}
            )
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "ver-wf", "version": "2.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_deprecate_workflow_type(self, swf):
        domain = f"depwf-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(domain=domain, name="dep-wf", version="1.0")
        swf.deprecate_workflow_type(
            domain=domain, workflowType={"name": "dep-wf", "version": "1.0"}
        )
        deprecated = swf.list_workflow_types(domain=domain, registrationStatus="DEPRECATED")
        names = [t["workflowType"]["name"] for t in deprecated["typeInfos"]]
        assert "dep-wf" in names
        swf.deprecate_domain(name=domain)

    def test_deprecate_activity_type(self, swf):
        domain = f"depact-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_activity_type(domain=domain, name="dep-act", version="1.0")
        swf.deprecate_activity_type(
            domain=domain, activityType={"name": "dep-act", "version": "1.0"}
        )
        deprecated = swf.list_activity_types(domain=domain, registrationStatus="DEPRECATED")
        names = [t["activityType"]["name"] for t in deprecated["typeInfos"]]
        assert "dep-act" in names
        swf.deprecate_domain(name=domain)

    def test_list_activity_types(self, swf):
        domain = f"listact-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        try:
            swf.register_activity_type(domain=domain, name="act-a", version="1.0")
            swf.register_activity_type(domain=domain, name="act-b", version="1.0")
            resp = swf.list_activity_types(domain=domain, registrationStatus="REGISTERED")
            names = [t["activityType"]["name"] for t in resp["typeInfos"]]
            assert "act-a" in names
            assert "act-b" in names
        finally:
            swf.deprecate_activity_type(
                domain=domain, activityType={"name": "act-a", "version": "1.0"}
            )
            swf.deprecate_activity_type(
                domain=domain, activityType={"name": "act-b", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_domain_retention_period(self, swf):
        domain = f"ret-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="90")
        resp = swf.describe_domain(name=domain)
        assert resp["configuration"]["workflowExecutionRetentionPeriodInDays"] == "90"
        swf.deprecate_domain(name=domain)

    def test_register_domain_with_description(self, swf):
        domain = f"desc-domain-{_uid()}"
        swf.register_domain(
            name=domain,
            workflowExecutionRetentionPeriodInDays="30",
            description="Test domain description",
        )
        resp = swf.describe_domain(name=domain)
        assert resp["domainInfo"]["description"] == "Test domain description"
        swf.deprecate_domain(name=domain)

    def test_undeprecate_domain(self, swf):
        domain = f"undep-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.deprecate_domain(name=domain)
        resp = swf.describe_domain(name=domain)
        assert resp["domainInfo"]["status"] == "DEPRECATED"

        swf.undeprecate_domain(name=domain)
        resp = swf.describe_domain(name=domain)
        assert resp["domainInfo"]["status"] == "REGISTERED"
        swf.deprecate_domain(name=domain)

    def test_undeprecate_workflow_type(self, swf):
        domain = f"undep-wf-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(domain=domain, name="undep-wf", version="1.0")
        swf.deprecate_workflow_type(
            domain=domain, workflowType={"name": "undep-wf", "version": "1.0"}
        )
        # Verify deprecated
        deprecated = swf.list_workflow_types(domain=domain, registrationStatus="DEPRECATED")
        names = [t["workflowType"]["name"] for t in deprecated["typeInfos"]]
        assert "undep-wf" in names

        swf.undeprecate_workflow_type(
            domain=domain, workflowType={"name": "undep-wf", "version": "1.0"}
        )
        # Verify re-registered
        resp = swf.describe_workflow_type(
            domain=domain, workflowType={"name": "undep-wf", "version": "1.0"}
        )
        assert resp["typeInfo"]["status"] == "REGISTERED"

        swf.deprecate_workflow_type(
            domain=domain, workflowType={"name": "undep-wf", "version": "1.0"}
        )
        swf.deprecate_domain(name=domain)

    def test_undeprecate_activity_type(self, swf):
        domain = f"undep-act-domain-{_uid()}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_activity_type(domain=domain, name="undep-act", version="1.0")
        swf.deprecate_activity_type(
            domain=domain, activityType={"name": "undep-act", "version": "1.0"}
        )
        # Verify deprecated
        deprecated = swf.list_activity_types(domain=domain, registrationStatus="DEPRECATED")
        names = [t["activityType"]["name"] for t in deprecated["typeInfos"]]
        assert "undep-act" in names

        swf.undeprecate_activity_type(
            domain=domain, activityType={"name": "undep-act", "version": "1.0"}
        )
        resp = swf.describe_activity_type(
            domain=domain, activityType={"name": "undep-act", "version": "1.0"}
        )
        assert resp["typeInfo"]["status"] == "REGISTERED"

        swf.deprecate_activity_type(
            domain=domain, activityType={"name": "undep-act", "version": "1.0"}
        )
        swf.deprecate_domain(name=domain)


class TestSWFTaskOperations:
    @pytest.fixture
    def swf(self):
        return make_client("swf")

    def _setup_activity_task(self, swf):
        """Set up domain, workflow, activity, start execution, schedule and poll activity task."""
        uid = _uid()
        domain = f"task-domain-{uid}"
        task_list = f"task-list-{uid}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(
            domain=domain,
            name="task-wf",
            version="1.0",
            defaultExecutionStartToCloseTimeout="3600",
            defaultTaskStartToCloseTimeout="300",
            defaultTaskList={"name": task_list},
            defaultChildPolicy="TERMINATE",
        )
        swf.register_activity_type(
            domain=domain,
            name="task-activity",
            version="1.0",
            defaultTaskStartToCloseTimeout="300",
            defaultTaskHeartbeatTimeout="60",
            defaultTaskList={"name": task_list},
            defaultTaskScheduleToStartTimeout="300",
            defaultTaskScheduleToCloseTimeout="600",
        )
        swf.start_workflow_execution(
            domain=domain,
            workflowId=f"wf-{uid}",
            workflowType={"name": "task-wf", "version": "1.0"},
            taskList={"name": task_list},
        )
        # Poll for decision task
        decision = swf.poll_for_decision_task(domain=domain, taskList={"name": task_list})
        decision_token = decision["taskToken"]
        # Schedule activity task via decision
        swf.respond_decision_task_completed(
            taskToken=decision_token,
            decisions=[
                {
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityType": {"name": "task-activity", "version": "1.0"},
                        "activityId": f"activity-{uid}",
                        "taskList": {"name": task_list},
                    },
                }
            ],
        )
        # Poll for activity task
        activity = swf.poll_for_activity_task(domain=domain, taskList={"name": task_list})
        return domain, uid, task_list, activity["taskToken"]

    def test_record_activity_task_heartbeat(self, swf):
        domain, uid, task_list, task_token = self._setup_activity_task(swf)
        try:
            resp = swf.record_activity_task_heartbeat(taskToken=task_token)
            assert "cancelRequested" in resp
            assert resp["cancelRequested"] is False
        finally:
            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-{uid}")
            swf.deprecate_activity_type(
                domain=domain, activityType={"name": "task-activity", "version": "1.0"}
            )
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "task-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_respond_activity_task_completed(self, swf):
        domain, uid, task_list, task_token = self._setup_activity_task(swf)
        try:
            resp = swf.respond_activity_task_completed(
                taskToken=task_token, result='{"status": "done"}'
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-{uid}")
            swf.deprecate_activity_type(
                domain=domain, activityType={"name": "task-activity", "version": "1.0"}
            )
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "task-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_respond_activity_task_failed(self, swf):
        domain, uid, task_list, task_token = self._setup_activity_task(swf)
        try:
            resp = swf.respond_activity_task_failed(
                taskToken=task_token, reason="test failure", details="some details"
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-{uid}")
            swf.deprecate_activity_type(
                domain=domain, activityType={"name": "task-activity", "version": "1.0"}
            )
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "task-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_respond_decision_task_completed(self, swf):
        uid = _uid()
        domain = f"dectask-domain-{uid}"
        task_list = f"dectask-list-{uid}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(
            domain=domain,
            name="dec-wf",
            version="1.0",
            defaultExecutionStartToCloseTimeout="3600",
            defaultTaskStartToCloseTimeout="300",
            defaultTaskList={"name": task_list},
            defaultChildPolicy="TERMINATE",
        )
        swf.start_workflow_execution(
            domain=domain,
            workflowId=f"wf-dec-{uid}",
            workflowType={"name": "dec-wf", "version": "1.0"},
            taskList={"name": task_list},
        )
        try:
            decision = swf.poll_for_decision_task(domain=domain, taskList={"name": task_list})
            assert decision["taskToken"] != ""
            resp = swf.respond_decision_task_completed(
                taskToken=decision["taskToken"], decisions=[]
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-dec-{uid}")
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "dec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)


class TestSWFWorkflowExecutions:
    @pytest.fixture
    def swf(self):
        return make_client("swf")

    def _setup_domain_and_workflow(self, swf):
        """Helper to create a domain and workflow type with all required defaults."""
        uid = _uid()
        domain = f"exec-domain-{uid}"
        swf.register_domain(name=domain, workflowExecutionRetentionPeriodInDays="30")
        swf.register_workflow_type(
            domain=domain,
            name="exec-wf",
            version="1.0",
            defaultExecutionStartToCloseTimeout="3600",
            defaultTaskStartToCloseTimeout="300",
            defaultTaskList={"name": "default"},
            defaultChildPolicy="TERMINATE",
        )
        return domain, uid

    def test_start_workflow_execution(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            resp = swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-start-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            assert "runId" in resp
            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-start-{uid}")
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_list_open_workflow_executions(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-open-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            resp = swf.list_open_workflow_executions(
                domain=domain,
                startTimeFilter={"oldestDate": "2020-01-01T00:00:00Z"},
            )
            wf_ids = [e["execution"]["workflowId"] for e in resp["executionInfos"]]
            assert f"wf-open-{uid}" in wf_ids

            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-open-{uid}")
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_terminate_and_list_closed_executions(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-close-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            swf.terminate_workflow_execution(
                domain=domain,
                workflowId=f"wf-close-{uid}",
                reason="testing closure",
            )
            resp = swf.list_closed_workflow_executions(
                domain=domain,
                startTimeFilter={"oldestDate": "2020-01-01T00:00:00Z"},
            )
            wf_ids = [e["execution"]["workflowId"] for e in resp["executionInfos"]]
            assert f"wf-close-{uid}" in wf_ids
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_get_workflow_execution_history(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            run = swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-hist-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            run_id = run["runId"]
            resp = swf.get_workflow_execution_history(
                domain=domain,
                execution={"workflowId": f"wf-hist-{uid}", "runId": run_id},
            )
            assert len(resp["events"]) >= 1
            # First event should be WorkflowExecutionStarted
            event_types = [e["eventType"] for e in resp["events"]]
            assert "WorkflowExecutionStarted" in event_types

            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-hist-{uid}")
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_signal_workflow_execution(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-sig-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            resp = swf.signal_workflow_execution(
                domain=domain,
                workflowId=f"wf-sig-{uid}",
                signalName="test-signal",
                input="{}",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            swf.terminate_workflow_execution(domain=domain, workflowId=f"wf-sig-{uid}")
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)

    def test_terminate_workflow_execution(self, swf):
        domain, uid = self._setup_domain_and_workflow(swf)
        try:
            swf.start_workflow_execution(
                domain=domain,
                workflowId=f"wf-term-{uid}",
                workflowType={"name": "exec-wf", "version": "1.0"},
            )
            resp = swf.terminate_workflow_execution(
                domain=domain,
                workflowId=f"wf-term-{uid}",
                reason="terminating for test",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify it's now in closed executions
            closed = swf.list_closed_workflow_executions(
                domain=domain,
                startTimeFilter={"oldestDate": "2020-01-01T00:00:00Z"},
            )
            wf_ids = [e["execution"]["workflowId"] for e in closed["executionInfos"]]
            assert f"wf-term-{uid}" in wf_ids
        finally:
            swf.deprecate_workflow_type(
                domain=domain, workflowType={"name": "exec-wf", "version": "1.0"}
            )
            swf.deprecate_domain(name=domain)
