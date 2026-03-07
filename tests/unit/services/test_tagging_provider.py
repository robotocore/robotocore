"""Unit tests for Resource Groups Tagging API native provider."""

from robotocore.services.tagging.provider import _matches_tag_filters


class TestMatchesTagFilters:
    def test_no_filters_matches_all(self):
        tags = [{"Key": "env", "Value": "prod"}]
        assert _matches_tag_filters(tags, []) is True

    def test_matching_key_and_value(self):
        tags = [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}]
        filters = [{"Key": "env", "Values": ["prod"]}]
        assert _matches_tag_filters(tags, filters) is True

    def test_non_matching_value(self):
        tags = [{"Key": "env", "Value": "staging"}]
        filters = [{"Key": "env", "Values": ["prod"]}]
        assert _matches_tag_filters(tags, filters) is False

    def test_missing_key(self):
        tags = [{"Key": "team", "Value": "backend"}]
        filters = [{"Key": "env", "Values": ["prod"]}]
        assert _matches_tag_filters(tags, filters) is False

    def test_key_only_filter_no_values(self):
        tags = [{"Key": "env", "Value": "anything"}]
        filters = [{"Key": "env", "Values": []}]
        assert _matches_tag_filters(tags, filters) is True

    def test_multiple_filters_all_must_match(self):
        tags = [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}]
        filters = [
            {"Key": "env", "Values": ["prod"]},
            {"Key": "team", "Values": ["backend"]},
        ]
        assert _matches_tag_filters(tags, filters) is True

    def test_multiple_filters_one_fails(self):
        tags = [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "frontend"}]
        filters = [
            {"Key": "env", "Values": ["prod"]},
            {"Key": "team", "Values": ["backend"]},
        ]
        assert _matches_tag_filters(tags, filters) is False

    def test_empty_tags_with_filter(self):
        assert _matches_tag_filters([], [{"Key": "env", "Values": ["prod"]}]) is False

    def test_multiple_allowed_values(self):
        tags = [{"Key": "env", "Value": "staging"}]
        filters = [{"Key": "env", "Values": ["prod", "staging"]}]
        assert _matches_tag_filters(tags, filters) is True
