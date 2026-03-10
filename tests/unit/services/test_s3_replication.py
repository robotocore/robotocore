"""Unit tests for S3 replication engine helpers."""

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
