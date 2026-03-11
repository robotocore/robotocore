"""Unit tests for init script execution tracker."""

from robotocore.init.tracker import InitTracker


class TestInitTracker:
    def setup_method(self):
        self.tracker = InitTracker()

    def test_record_script_execution(self):
        self.tracker.record_start("01-setup.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert len(scripts) == 1
        assert scripts[0]["filename"] == "01-setup.sh"
        assert scripts[0]["stage"] == "boot"
        assert scripts[0]["status"] == "running"

    def test_record_script_completion_with_duration(self):
        self.tracker.record_start("01-setup.sh", "boot")
        self.tracker.record_complete("01-setup.sh", "boot", duration=1.23)
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "completed"
        assert scripts[0]["duration"] == 1.23

    def test_record_script_failure_with_error(self):
        self.tracker.record_start("02-fail.sh", "ready")
        self.tracker.record_failure("02-fail.sh", "ready", error="Permission denied", duration=0.5)
        scripts = self.tracker.get_scripts("ready")
        assert scripts[0]["status"] == "failed"
        assert scripts[0]["error"] == "Permission denied"
        assert scripts[0]["duration"] == 0.5

    def test_get_scripts_by_stage(self):
        self.tracker.record_start("a.sh", "boot")
        self.tracker.record_start("b.sh", "ready")
        self.tracker.record_start("c.sh", "boot")
        boot_scripts = self.tracker.get_scripts("boot")
        assert len(boot_scripts) == 2
        ready_scripts = self.tracker.get_scripts("ready")
        assert len(ready_scripts) == 1

    def test_get_summary(self):
        self.tracker.record_start("a.sh", "boot")
        self.tracker.record_complete("a.sh", "boot", duration=0.1)
        self.tracker.record_start("b.sh", "boot")
        self.tracker.record_failure("b.sh", "boot", error="err", duration=0.2)
        self.tracker.record_start("c.sh", "ready")

        summary = self.tracker.get_summary()
        assert "boot" in summary["stages"]
        assert summary["stages"]["boot"]["total"] == 2
        assert summary["stages"]["boot"]["completed"] == 1
        assert summary["stages"]["boot"]["failed"] == 1
        assert "ready" in summary["stages"]
        assert summary["stages"]["ready"]["total"] == 1
        assert summary["stages"]["ready"]["running"] == 1

    def test_multiple_scripts_tracked_in_order(self):
        self.tracker.record_start("01-first.sh", "boot")
        self.tracker.record_start("02-second.sh", "boot")
        self.tracker.record_start("03-third.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert [s["filename"] for s in scripts] == [
            "01-first.sh",
            "02-second.sh",
            "03-third.sh",
        ]

    def test_pending_running_completed_failed_states(self):
        self.tracker.record_pending("setup.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "pending"

        self.tracker.record_start("setup.sh", "boot")
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "running"

        self.tracker.record_complete("setup.sh", "boot", duration=1.0)
        scripts = self.tracker.get_scripts("boot")
        assert scripts[0]["status"] == "completed"

    def test_unknown_stage_returns_empty_list(self):
        scripts = self.tracker.get_scripts("nonexistent")
        assert scripts == []
