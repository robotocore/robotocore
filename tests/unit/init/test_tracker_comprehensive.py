"""Comprehensive unit tests for init script tracker."""

import robotocore.init.tracker as tracker_mod
from robotocore.init.tracker import InitTracker, ScriptRecord, ScriptStatus, get_init_tracker


class TestScriptStatus:
    """Test the ScriptStatus enum values and behavior."""

    def test_status_values_are_strings(self):
        assert ScriptStatus.PENDING == "pending"
        assert ScriptStatus.RUNNING == "running"
        assert ScriptStatus.COMPLETED == "completed"
        assert ScriptStatus.FAILED == "failed"

    def test_status_value_attribute(self):
        assert ScriptStatus.PENDING.value == "pending"
        assert ScriptStatus.RUNNING.value == "running"
        assert ScriptStatus.COMPLETED.value == "completed"
        assert ScriptStatus.FAILED.value == "failed"


class TestScriptRecord:
    """Test ScriptRecord dataclass and serialization."""

    def test_default_record_has_pending_status(self):
        record = ScriptRecord(filename="test.sh", stage="boot")
        assert record.status == ScriptStatus.PENDING
        assert record.duration is None
        assert record.error is None

    def test_to_dict_minimal(self):
        record = ScriptRecord(filename="test.sh", stage="boot")
        d = record.to_dict()
        assert d == {
            "filename": "test.sh",
            "stage": "boot",
            "status": "pending",
        }
        # duration and error should be absent when None
        assert "duration" not in d
        assert "error" not in d

    def test_to_dict_with_duration(self):
        record = ScriptRecord(filename="test.sh", stage="boot", duration=1.5)
        d = record.to_dict()
        assert d["duration"] == 1.5

    def test_to_dict_with_error(self):
        record = ScriptRecord(
            filename="test.sh",
            stage="boot",
            status=ScriptStatus.FAILED,
            error="timeout",
        )
        d = record.to_dict()
        assert d["error"] == "timeout"
        assert d["status"] == "failed"

    def test_to_dict_with_all_fields(self):
        record = ScriptRecord(
            filename="setup.sh",
            stage="ready",
            status=ScriptStatus.COMPLETED,
            duration=2.5,
            error=None,
        )
        d = record.to_dict()
        assert d["filename"] == "setup.sh"
        assert d["stage"] == "ready"
        assert d["status"] == "completed"
        assert d["duration"] == 2.5
        assert "error" not in d

    def test_to_dict_includes_error_even_if_empty_string(self):
        record = ScriptRecord(filename="test.sh", stage="boot", error="")
        d = record.to_dict()
        # empty string is not None, so it should be included
        assert d["error"] == ""


class TestInitTrackerFindOrCreate:
    """Test the _find_or_create internal method."""

    def test_creates_new_record_for_new_filename(self):
        tracker = InitTracker()
        record = tracker._find_or_create("new.sh", "boot")
        assert record.filename == "new.sh"
        assert record.stage == "boot"
        assert record.status == ScriptStatus.PENDING

    def test_returns_existing_record_for_same_filename_and_stage(self):
        tracker = InitTracker()
        r1 = tracker._find_or_create("test.sh", "boot")
        r1.status = ScriptStatus.RUNNING
        r2 = tracker._find_or_create("test.sh", "boot")
        assert r1 is r2
        assert r2.status == ScriptStatus.RUNNING

    def test_different_stages_create_separate_records(self):
        tracker = InitTracker()
        r1 = tracker._find_or_create("test.sh", "boot")
        r2 = tracker._find_or_create("test.sh", "ready")
        assert r1 is not r2
        assert r1.stage == "boot"
        assert r2.stage == "ready"


class TestInitTrackerStateTransitions:
    """Test recording state transitions."""

    def setup_method(self):
        self.tracker = InitTracker()

    def test_pending_to_running_to_completed(self):
        self.tracker.record_pending("s.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "pending"

        self.tracker.record_start("s.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "running"

        self.tracker.record_complete("s.sh", "boot", duration=0.5)
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "completed"
        assert scripts[0]["duration"] == 0.5

    def test_pending_to_running_to_failed(self):
        self.tracker.record_pending("s.sh", "boot")
        self.tracker.record_start("s.sh", "boot")
        self.tracker.record_failure("s.sh", "boot", error="crash", duration=0.1)
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "failed"
        assert scripts[0]["error"] == "crash"
        assert scripts[0]["duration"] == 0.1

    def test_record_complete_without_prior_start(self):
        """Should still work -- creates the record and marks complete."""
        self.tracker.record_complete("orphan.sh", "boot", duration=0.0)
        scripts = self.tracker.get_scripts("boot")
        assert len(scripts) == 1
        assert scripts[0]["status"] == "completed"

    def test_record_failure_without_prior_start(self):
        """Should still work -- creates the record and marks failed."""
        self.tracker.record_failure("orphan.sh", "boot", error="boom")
        scripts = self.tracker.get_scripts("boot")
        assert len(scripts) == 1
        assert scripts[0]["status"] == "failed"
        assert scripts[0]["error"] == "boom"

    def test_default_duration_is_zero(self):
        self.tracker.record_complete("s.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["duration"] == 0.0

    def test_default_failure_error_is_empty(self):
        self.tracker.record_failure("s.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["error"] == ""


class TestInitTrackerSummary:
    """Test the get_summary method."""

    def setup_method(self):
        self.tracker = InitTracker()

    def test_empty_tracker_summary(self):
        summary = self.tracker.get_summary()
        assert summary == {"stages": {}}

    def test_summary_counts_all_statuses(self):
        self.tracker.record_pending("a.sh", "boot")
        self.tracker.record_start("b.sh", "boot")
        self.tracker.record_complete("c.sh", "boot", duration=0.1)
        self.tracker.record_failure("d.sh", "boot", error="x")
        summary = self.tracker.get_summary()
        boot = summary["stages"]["boot"]
        assert boot["total"] == 4
        assert boot["pending"] == 1
        assert boot["running"] == 1
        assert boot["completed"] == 1
        assert boot["failed"] == 1

    def test_summary_multiple_stages(self):
        self.tracker.record_complete("a.sh", "boot", duration=0.1)
        self.tracker.record_complete("b.sh", "ready", duration=0.2)
        self.tracker.record_start("c.sh", "shutdown")
        summary = self.tracker.get_summary()
        assert set(summary["stages"].keys()) == {"boot", "ready", "shutdown"}
        assert summary["stages"]["boot"]["total"] == 1
        assert summary["stages"]["ready"]["total"] == 1
        assert summary["stages"]["shutdown"]["total"] == 1

    def test_summary_is_json_serializable(self):
        import json

        self.tracker.record_complete("a.sh", "boot", duration=0.1)
        summary = self.tracker.get_summary()
        serialized = json.dumps(summary)
        assert serialized  # non-empty string


class TestGetInitTrackerSingleton:
    """Test the global singleton accessor."""

    def setup_method(self):
        tracker_mod._tracker = None

    def teardown_method(self):
        tracker_mod._tracker = None

    def test_returns_same_instance(self):
        t1 = get_init_tracker()
        t2 = get_init_tracker()
        assert t1 is t2

    def test_creates_instance_on_first_call(self):
        assert tracker_mod._tracker is None
        t = get_init_tracker()
        assert t is not None
        assert isinstance(t, InitTracker)

    def test_state_persists_across_calls(self):
        t1 = get_init_tracker()
        t1.record_start("x.sh", "boot")
        t2 = get_init_tracker()
        scripts = t2.get_scripts("boot")
        assert len(scripts) == 1
        assert scripts[0]["filename"] == "x.sh"
