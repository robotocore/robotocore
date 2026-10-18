#!/usr/bin/env python3
"""cdklocal — AWS CDK CLI wrapper that auto-configures CDK for robotocore.

Usage:
    cdklocal.py deploy --all
    cdklocal.py bootstrap
    cdklocal.py synth

Environment variables:
    ROBOTOCORE_ENDPOINT  Full endpoint URL (overrides host/port)
    ROBOTOCORE_HOST      Robotocore host (default: localhost)
    ROBOTOCORE_PORT      Robotocore port (default: 4566)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

# Allow importing cdklocal_lib from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cdklocal_lib import build_command, build_env  # noqa: E402


def main() -> int:
    args = sys.argv[1:]

    # Check that cdk is available
    if not shutil.which("cdk"):
        print(
            "Error: 'cdk' command not found. Install AWS CDK CLI:\n"
            "  npm install -g aws-cdk\n"
            "See: https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html",
            file=sys.stderr,
        )
        return 1

    env = build_env(dict(os.environ), args)
    cmd = build_command(args)

    result = subprocess.run(cmd, env=env)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
