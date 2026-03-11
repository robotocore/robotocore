"""Unit tests for EventBridge Pipes CRUD operations."""

import pytest

from robotocore.services.pipes.provider import (
    _create_pipe,
    _delete_pipe,
    _describe_pipe,
    _list_pipes,
    _start_pipe,
    _stop_pipe,
    reset_pipes_state,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture(autouse=True)
def clean_state():
    reset_pipes_state()
    yield
    reset_pipes_state()


def _make_pipe_params(**overrides):
    defaults = {
        "Source": "arn:aws:sqs:us-east-1:123456789012:my-source-queue",
        "Target": "arn:aws:sqs:us-east-1:123456789012:my-target-queue",
        "RoleArn": "arn:aws:iam::123456789012:role/pipe-role",
        "DesiredState": "STOPPED",
    }
    defaults.update(overrides)
    return defaults


class TestCreatePipe:
    def test_create_pipe_basic(self):
        result = _create_pipe("test-pipe", _make_pipe_params(), REGION, ACCOUNT)
        assert result["Name"] == "test-pipe"
        assert "Arn" in result
        assert "arn:aws:pipes:us-east-1:123456789012:pipe/test-pipe" == result["Arn"]
        assert result["DesiredState"] == "STOPPED"
        assert result["CurrentState"] == "STOPPED"

    def test_create_pipe_duplicate_raises_conflict(self):
        _create_pipe("dup-pipe", _make_pipe_params(), REGION, ACCOUNT)
        from robotocore.services.pipes.provider import PipesError

        with pytest.raises(PipesError) as exc_info:
            _create_pipe("dup-pipe", _make_pipe_params(), REGION, ACCOUNT)
        assert exc_info.value.code == "ConflictException"
        assert exc_info.value.status == 409

    def test_create_pipe_with_running_state(self):
        result = _create_pipe(
            "running-pipe",
            _make_pipe_params(DesiredState="RUNNING"),
            REGION,
            ACCOUNT,
        )
        assert result["DesiredState"] == "RUNNING"
        assert result["CurrentState"] == "RUNNING"


class TestDescribePipe:
    def test_describe_returns_correct_fields(self):
        params = _make_pipe_params(
            Description="my description",
            Enrichment="arn:aws:lambda:us-east-1:123456789012:function:enrich",
        )
        _create_pipe("desc-pipe", params, REGION, ACCOUNT)
        result = _describe_pipe("desc-pipe", REGION, ACCOUNT)
        assert result["Name"] == "desc-pipe"
        assert result["Source"] == params["Source"]
        assert result["Target"] == params["Target"]
        assert result["Enrichment"] == params["Enrichment"]
        assert result["Description"] == "my description"
        assert result["RoleArn"] == params["RoleArn"]
        assert "CreationTime" in result
        assert "_tags" not in result  # tags should be stripped

    def test_describe_nonexistent_raises_not_found(self):
        from robotocore.services.pipes.provider import PipesError

        with pytest.raises(PipesError) as exc_info:
            _describe_pipe("nonexistent", REGION, ACCOUNT)
        assert exc_info.value.code == "NotFoundException"
        assert exc_info.value.status == 404


class TestListPipes:
    def test_list_all_pipes(self):
        _create_pipe("pipe-a", _make_pipe_params(), REGION, ACCOUNT)
        _create_pipe("pipe-b", _make_pipe_params(), REGION, ACCOUNT)

        class QP:
            def get(self, key, default=None):
                return default

        result = _list_pipes(QP(), REGION, ACCOUNT)
        assert len(result["Pipes"]) == 2
        names = {p["Name"] for p in result["Pipes"]}
        assert names == {"pipe-a", "pipe-b"}

    def test_list_with_name_prefix(self):
        _create_pipe("test-one", _make_pipe_params(), REGION, ACCOUNT)
        _create_pipe("test-two", _make_pipe_params(), REGION, ACCOUNT)
        _create_pipe("other-pipe", _make_pipe_params(), REGION, ACCOUNT)

        class QP:
            def get(self, key, default=None):
                return {"NamePrefix": "test-"}.get(key, default)

        result = _list_pipes(QP(), REGION, ACCOUNT)
        assert len(result["Pipes"]) == 2

    def test_list_with_state_filter(self):
        _create_pipe("stopped-pipe", _make_pipe_params(DesiredState="STOPPED"), REGION, ACCOUNT)
        _create_pipe("running-pipe", _make_pipe_params(DesiredState="RUNNING"), REGION, ACCOUNT)

        class QP:
            def get(self, key, default=None):
                return {"CurrentState": "STOPPED"}.get(key, default)

        result = _list_pipes(QP(), REGION, ACCOUNT)
        assert len(result["Pipes"]) == 1
        assert result["Pipes"][0]["Name"] == "stopped-pipe"


class TestUpdatePipe:
    def test_update_target(self):
        from robotocore.services.pipes.provider import _update_pipe

        _create_pipe("upd-pipe", _make_pipe_params(), REGION, ACCOUNT)
        new_target = "arn:aws:sqs:us-east-1:123456789012:new-target"
        result = _update_pipe("upd-pipe", {"Target": new_target}, REGION, ACCOUNT)
        assert result["Name"] == "upd-pipe"

        pipe = _describe_pipe("upd-pipe", REGION, ACCOUNT)
        assert pipe["Target"] == new_target

    def test_update_enrichment(self):
        from robotocore.services.pipes.provider import _update_pipe

        _create_pipe("enrich-pipe", _make_pipe_params(), REGION, ACCOUNT)
        enrichment = "arn:aws:lambda:us-east-1:123456789012:function:my-enricher"
        _update_pipe("enrich-pipe", {"Enrichment": enrichment}, REGION, ACCOUNT)

        pipe = _describe_pipe("enrich-pipe", REGION, ACCOUNT)
        assert pipe["Enrichment"] == enrichment

    def test_update_nonexistent_raises(self):
        from robotocore.services.pipes.provider import PipesError, _update_pipe

        with pytest.raises(PipesError) as exc_info:
            _update_pipe("nope", {}, REGION, ACCOUNT)
        assert exc_info.value.code == "NotFoundException"


class TestDeletePipe:
    def test_delete_pipe(self):
        _create_pipe("del-pipe", _make_pipe_params(), REGION, ACCOUNT)
        result = _delete_pipe("del-pipe", REGION, ACCOUNT)
        assert result["Name"] == "del-pipe"
        assert result["CurrentState"] == "DELETING"

        from robotocore.services.pipes.provider import PipesError

        with pytest.raises(PipesError):
            _describe_pipe("del-pipe", REGION, ACCOUNT)

    def test_delete_nonexistent_raises(self):
        from robotocore.services.pipes.provider import PipesError

        with pytest.raises(PipesError) as exc_info:
            _delete_pipe("nonexistent", REGION, ACCOUNT)
        assert exc_info.value.code == "NotFoundException"


class TestStartStopPipe:
    def test_start_transitions_to_running(self):
        _create_pipe("start-pipe", _make_pipe_params(DesiredState="STOPPED"), REGION, ACCOUNT)
        result = _start_pipe("start-pipe", REGION, ACCOUNT)
        assert result["CurrentState"] == "RUNNING"
        assert result["DesiredState"] == "RUNNING"

    def test_stop_transitions_to_stopped(self):
        _create_pipe("stop-pipe", _make_pipe_params(DesiredState="RUNNING"), REGION, ACCOUNT)
        result = _stop_pipe("stop-pipe", REGION, ACCOUNT)
        assert result["CurrentState"] == "STOPPED"
        assert result["DesiredState"] == "STOPPED"

    def test_pipe_lifecycle(self):
        """Test full lifecycle: CREATING -> STOPPED -> RUNNING -> STOPPED -> DELETING."""
        params = _make_pipe_params(DesiredState="STOPPED")
        create_result = _create_pipe("lifecycle-pipe", params, REGION, ACCOUNT)
        assert create_result["CurrentState"] == "STOPPED"

        # Start
        start_result = _start_pipe("lifecycle-pipe", REGION, ACCOUNT)
        assert start_result["CurrentState"] == "RUNNING"

        # Stop
        stop_result = _stop_pipe("lifecycle-pipe", REGION, ACCOUNT)
        assert stop_result["CurrentState"] == "STOPPED"

        # Start again
        start2 = _start_pipe("lifecycle-pipe", REGION, ACCOUNT)
        assert start2["CurrentState"] == "RUNNING"

        # Delete
        delete_result = _delete_pipe("lifecycle-pipe", REGION, ACCOUNT)
        assert delete_result["CurrentState"] == "DELETING"
