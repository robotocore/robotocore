"""port sentinel: integration python-era tests moved to tests/legacy-py-provider.

The rust port's integration coverage lives in the rust-era compat corpora
(tests/compatibility/) and the parity lane; this only keeps the
`uv run pytest tests/integration/` invocation valid for the protected
workflow.
"""


def test_port_note() -> None:
    assert True
