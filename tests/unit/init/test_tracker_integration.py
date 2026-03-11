"""Semantic tests for init tracker — end-to-end with endpoint JSON structure."""

import json

import robotocore.init.tracker as tracker_mod


class TestInitTrackerIntegration:
    def setup_method(self):
        """Reset global tracker for each test."""
        tracker_mod._tracker = None

    def test_execute_scripts_then_get_init_summary(self):
        tracker = tracker_mod.get_init_tracker()
        tracker.record_start("01-setup.sh", "boot")
        tracker.record_complete("01-setup.sh", "boot", duration=0.5)
        tracker.record_start("02-db.sh", "boot")
        tracker.record_failure("02-db.sh", "boot", error="connection refused", duration=1.2)
        tracker.record_start("01-warm.sh", "ready")
        tracker.record_complete("01-warm.sh", "ready", duration=0.1)

        summary = tracker.get_summary()
        assert summary["stages"]["boot"]["total"] == 2
        assert summary["stages"]["boot"]["completed"] == 1
        assert summary["stages"]["boot"]["failed"] == 1
        assert summary["stages"]["ready"]["total"] == 1
        assert summary["stages"]["ready"]["completed"] == 1

    def test_get_init_stage_ready_scripts(self):
        tracker = tracker_mod.get_init_tracker()
        tracker.record_start("warmup.sh", "ready")
        tracker.record_complete("warmup.sh", "ready", duration=0.3)
        tracker.record_start("notify.sh", "ready")
        tracker.record_complete("notify.sh", "ready", duration=0.1)

        scripts = tracker.get_scripts("ready")
        assert len(scripts) == 2
        assert scripts[0]["filename"] == "warmup.sh"
        assert scripts[1]["filename"] == "notify.sh"

    def test_management_endpoint_json_structure(self):
        """Verify the JSON structure matches what the endpoint should return."""
        tracker = tracker_mod.get_init_tracker()
        tracker.record_start("a.sh", "boot")
        tracker.record_complete("a.sh", "boot", duration=0.1)

        summary = tracker.get_summary()
        # Verify it's JSON-serializable
        json_str = json.dumps(summary)
        parsed = json.loads(json_str)
        assert "stages" in parsed
        assert isinstance(parsed["stages"], dict)
        assert isinstance(parsed["stages"]["boot"]["total"], int)
