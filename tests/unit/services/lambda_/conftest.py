"""Shared fixtures for Lambda runtime unit tests.

The fault-in installer (``runtimes/install.py``) is intentionally opt-out
here. By default these unit tests must never hit the network or touch
``/var/lib/robotocore/runtimes``. Tests that DO want to exercise
fault-in (see ``test_runtime_install.py``) re-enable it explicitly via
their own fixture that also redirects ``CACHE_DIR``/``WRAPPER_BIN_DIR``
to ``tmp_path``.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _disable_runtime_faultin(monkeypatch):
    from robotocore.services.lambda_.runtimes import install as install_mod

    monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", True)
    yield
