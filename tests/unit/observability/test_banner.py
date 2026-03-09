"""Tests for startup banner."""

from robotocore.observability.banner import BANNER, print_banner


class TestBanner:
    def test_banner_is_nonempty(self):
        assert len(BANNER.strip()) > 0

    def test_print_banner(self, capsys):
        print_banner(host="127.0.0.1", port=4566)
        captured = capsys.readouterr()
        assert "Robotocore" in captured.out
        assert "4566" in captured.out
        assert "Ready." in captured.out
        assert "AWS_ENDPOINT_URL" in captured.out

    def test_print_banner_custom_port(self, capsys):
        print_banner(host="0.0.0.0", port=9999)
        captured = capsys.readouterr()
        assert "9999" in captured.out
        assert "0.0.0.0" in captured.out
