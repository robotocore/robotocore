"""Tests for canary scheduler: schedule parsing and periodic execution."""

from robotocore.services.synthetics.scheduler import (
    parse_cron_minutes,
    parse_rate_seconds,
)


class TestParseRateSeconds:
    """Test rate() expression parsing."""

    def test_rate_5_minutes(self):
        assert parse_rate_seconds("rate(5 minutes)") == 300

    def test_rate_1_minute(self):
        assert parse_rate_seconds("rate(1 minute)") == 60

    def test_rate_1_hour(self):
        assert parse_rate_seconds("rate(1 hour)") == 3600

    def test_rate_2_hours(self):
        assert parse_rate_seconds("rate(2 hours)") == 7200

    def test_rate_1_day(self):
        assert parse_rate_seconds("rate(1 day)") == 86400

    def test_rate_0_minutes_returns_none(self):
        assert parse_rate_seconds("rate(0 minutes)") is None

    def test_invalid_expression_returns_none(self):
        assert parse_rate_seconds("not a rate expression") is None

    def test_empty_string_returns_none(self):
        assert parse_rate_seconds("") is None

    def test_rate_with_extra_spaces(self):
        assert parse_rate_seconds("rate( 10  minutes )") == 600


class TestParseCronMinutes:
    """Test cron() expression parsing."""

    def test_every_5_minutes(self):
        result = parse_cron_minutes("cron(0/5 * * * ? *)")
        assert result == 300

    def test_every_10_minutes(self):
        result = parse_cron_minutes("cron(*/10 * * * ? *)")
        assert result == 600

    def test_hourly(self):
        result = parse_cron_minutes("cron(0 * * * ? *)")
        assert result == 3600

    def test_complex_cron_defaults_to_5min(self):
        result = parse_cron_minutes("cron(15 10 ? * MON-FRI *)")
        assert result == 300  # default for complex patterns

    def test_invalid_cron_returns_none(self):
        assert parse_cron_minutes("not a cron") is None

    def test_short_cron_fields_defaults(self):
        result = parse_cron_minutes("cron(0/5 *)")
        assert result == 300  # default for too few fields

    def test_step_with_start_offset(self):
        result = parse_cron_minutes("cron(5/15 * * * ? *)")
        assert result == 900  # 15 minutes

    def test_every_1_minute_minimum(self):
        result = parse_cron_minutes("cron(*/1 * * * ? *)")
        assert result == 60  # minimum 60 seconds
