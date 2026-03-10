"""Unit tests for S3 replication engine helpers."""

import logging
from unittest.mock import MagicMock, patch


class TestRuleHelpers:
    def test_get_rule_prefix_old_style(self):
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Prefix": "logs/", "Status": "Enabled"}
        assert _get_rule_prefix(rule) == "logs/"

    def test_get_rule_prefix_new_style_filter(self):
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Filter": {"Prefix": "data/"}, "Status": "Enabled"}
        assert _get_rule_prefix(rule) == "data/"

    def test_get_rule_prefix_empty(self):
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Status": "Enabled"}
        assert _get_rule_prefix(rule) == ""

    def test_get_rule_prefix_and_filter(self):
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Filter": {"And": {"Prefix": "docs/"}}, "Status": "Enabled"}
        assert _get_rule_prefix(rule) == "docs/"

    def test_get_rule_prefix_and_filter_no_prefix_returns_empty(self):
        """And filter with no Prefix key → replicate all (return "")."""
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Filter": {"And": {"Tags": [{"Key": "env", "Value": "prod"}]}}}
        assert _get_rule_prefix(rule) == ""

    def test_get_rule_prefix_non_dict_filter_falls_through(self):
        """A non-dict Filter value falls through to old-style top-level Prefix."""
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Filter": None, "Prefix": "fallback/"}
        assert _get_rule_prefix(rule) == "fallback/"

    def test_get_rule_prefix_filter_prefix_empty_string(self):
        """Filter.Prefix == "" means replicate all (startswith("") is always True)."""
        from robotocore.services.s3.replication import _get_rule_prefix

        rule = {"Filter": {"Prefix": ""}}
        assert _get_rule_prefix(rule) == ""

    def test_parse_dest_bucket_with_arn(self):
        from robotocore.services.s3.replication import _parse_dest_bucket

        rule = {"Destination": {"Bucket": "arn:aws:s3:::my-dest-bucket"}}
        assert _parse_dest_bucket(rule) == "my-dest-bucket"

    def test_parse_dest_bucket_missing(self):
        from robotocore.services.s3.replication import _parse_dest_bucket

        rule = {}
        assert _parse_dest_bucket(rule) is None

    def test_parse_dest_bucket_no_arn_prefix(self):
        from robotocore.services.s3.replication import _parse_dest_bucket

        rule = {"Destination": {"Bucket": "plain-bucket-name"}}
        assert _parse_dest_bucket(rule) == "plain-bucket-name"

    def test_parse_dest_bucket_empty_bucket(self):
        from robotocore.services.s3.replication import _parse_dest_bucket

        rule = {"Destination": {"Bucket": ""}}
        assert _parse_dest_bucket(rule) is None

    def test_parse_dest_bucket_non_dict_destination(self):
        """If Destination value is not a dict, return None."""
        from robotocore.services.s3.replication import _parse_dest_bucket

        rule = {"Destination": "not-a-dict"}
        assert _parse_dest_bucket(rule) is None


class TestMaybeReplicate:
    def test_no_replication_config_is_noop(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = None
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            from robotocore.services.s3.replication import maybe_replicate

            maybe_replicate("src-bucket", "key.txt", "us-east-1", "123456789012")
        # No exception, no submit

    def test_disabled_rule_no_submit(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [{"Status": "Disabled", "Destination": {"Bucket": "arn:aws:s3:::dest"}}]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "key.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_not_called()

    def test_filter_and_prefix_matching_submits(self):
        """Filter.And.Prefix is used for prefix matching — matching key is replicated."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Filter": {"And": {"Prefix": "docs/"}},
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    "ID": "and-rule",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "docs/readme.md", "us-east-1", "123456789012")
        mock_ex.submit.assert_called_once()

    def test_filter_and_prefix_mismatch_no_submit(self):
        """Filter.And.Prefix prefix mismatch — key is not replicated."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Filter": {"And": {"Prefix": "docs/"}},
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    "ID": "and-rule",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "other/file.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_not_called()

    def test_prefix_mismatch_no_submit(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "logs/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "data/key.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_not_called()

    def test_matching_rule_submits(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "data/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    "ID": "r1",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "data/key.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_called_once()

    def test_no_dest_bucket_no_submit(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "",
                    "Destination": {},
                    "ID": "r1",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "key.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_not_called()

    def test_exception_is_swallowed(self):
        """If get_backend raises, maybe_replicate logs and returns without raising."""
        with patch(
            "robotocore.services.s3.replication.get_backend", side_effect=RuntimeError("boom")
        ):
            from robotocore.services.s3.replication import maybe_replicate

            # Must not raise
            maybe_replicate("src", "key.txt", "us-east-1", "123456789012")

    def test_exception_is_logged(self, caplog):
        """If get_backend raises, the exception is logged at ERROR level."""
        with patch(
            "robotocore.services.s3.replication.get_backend", side_effect=RuntimeError("test-err")
        ):
            with caplog.at_level(logging.ERROR, logger="robotocore.services.s3.replication"):
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src-bucket", "key.txt", "us-east-1", "123456789012")
        assert any("src-bucket" in rec.message for rec in caplog.records)

    def test_multiple_rules_all_matching_submits_all(self):
        """Multiple enabled matching rules each produce a submit call."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "data/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest1"},
                    "ID": "r1",
                },
                {
                    "Status": "Enabled",
                    "Prefix": "data/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest2"},
                    "ID": "r2",
                },
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "data/file.txt", "us-east-1", "123456789012")
        assert mock_ex.submit.call_count == 2

    def test_rule_without_id_field_uses_empty_string(self):
        """A rule with no ID field passes empty string as rule_id to _replicate_object."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "",
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    # No "ID" key
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "key.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_called_once()
        # submit(fn, src_bucket, key, dest_bucket, region, account_id, rule_id)
        # rule_id is the last positional argument (index 6)
        submit_args = mock_ex.submit.call_args[0]
        assert submit_args[6] == ""

    def test_mixed_enabled_disabled_rules(self):
        """Only enabled rules with matching prefix produce submit calls."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Prefix": "logs/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest1"},
                    "ID": "r1",
                },
                {
                    "Status": "Disabled",
                    "Prefix": "logs/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest2"},
                    "ID": "r2",
                },
                {
                    "Status": "Enabled",
                    "Prefix": "other/",
                    "Destination": {"Bucket": "arn:aws:s3:::dest3"},
                    "ID": "r3",
                },
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                from robotocore.services.s3.replication import maybe_replicate

                maybe_replicate("src", "logs/access.log", "us-east-1", "123456789012")
        # Only r1 matches (enabled + prefix match); r2 disabled, r3 prefix mismatch
        assert mock_ex.submit.call_count == 1


class TestReplicateObject:
    def test_src_key_none_returns_early(self):
        """If get_object returns None, _replicate_object does nothing."""
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = None
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                from robotocore.services.s3.replication import _replicate_object

                _replicate_object("src", "key.txt", "dest", "us-east-1", "123456789012", "r1")
        mock_backend.copy_object.assert_not_called()
        mock_fire.assert_not_called()

    def test_successful_replication_fires_event(self):
        """Happy path: object found, copied, replication event fired."""
        mock_backend = MagicMock()
        mock_src_key = MagicMock()
        mock_backend.get_object.return_value = mock_src_key
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                from robotocore.services.s3.replication import _replicate_object

                _replicate_object(
                    "src-bucket", "key.txt", "dest-bucket", "us-east-1", "123456789012", "r1"
                )
        mock_backend.copy_object.assert_called_once_with(mock_src_key, "dest-bucket", "key.txt")
        mock_fire.assert_called_once()
        assert mock_fire.call_args[0][0] == "s3:Replication:OperationReplicatedAfterThreshold"
        assert mock_fire.call_args[0][1] == "src-bucket"
        assert mock_fire.call_args[0][2] == "key.txt"

    def test_copy_exception_is_swallowed(self):
        """If copy_object raises, _replicate_object logs and does not re-raise."""
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        mock_backend.copy_object.side_effect = RuntimeError("copy failed")
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            from robotocore.services.s3.replication import _replicate_object

            # Must not raise
            _replicate_object("src", "key.txt", "dest", "us-east-1", "123456789012", "r1")

    def test_copy_exception_is_logged(self, caplog):
        """If copy_object raises, the exception is logged with relevant info."""
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        mock_backend.copy_object.side_effect = RuntimeError("copy failed")
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with caplog.at_level(logging.ERROR, logger="robotocore.services.s3.replication"):
                from robotocore.services.s3.replication import _replicate_object

                _replicate_object("src", "key.txt", "dest", "us-east-1", "123456789012", "my-rule")
        assert any("src" in rec.message for rec in caplog.records)

    def test_replicate_object_passes_region_and_account_to_fire_event(self):
        """The region and account_id are forwarded to fire_event."""
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                from robotocore.services.s3.replication import _replicate_object

                _replicate_object("src", "my-key", "dest", "eu-central-1", "999888777666", "rule-x")
        assert mock_fire.call_args[0][3] == "eu-central-1"
        assert mock_fire.call_args[0][4] == "999888777666"
