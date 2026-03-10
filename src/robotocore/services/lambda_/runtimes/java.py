"""Java runtime executor — compiles bootstrap + runs handler via subprocess."""

import glob
import os
import shutil
import subprocess

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

BOOTSTRAP_JAVA = os.path.join(os.path.dirname(__file__), "bootstraps", "Bootstrap.java")

# Cache compiled bootstrap class
_bootstrap_compiled_dir: str | None = None


def _ensure_bootstrap_compiled() -> str | None:
    """Compile Bootstrap.java once and cache the result. Returns dir with Bootstrap.class."""
    global _bootstrap_compiled_dir
    if _bootstrap_compiled_dir and os.path.isdir(_bootstrap_compiled_dir):
        return _bootstrap_compiled_dir

    javac = shutil.which("javac")
    if not javac:
        return None

    import tempfile

    outdir = tempfile.mkdtemp(prefix="lambda_java_bootstrap_")
    try:
        subprocess.run(
            [javac, "-d", outdir, BOOTSTRAP_JAVA],
            capture_output=True,
            timeout=30,
        )
        if os.path.exists(os.path.join(outdir, "Bootstrap.class")):
            _bootstrap_compiled_dir = outdir
            return outdir
    except Exception:
        pass
    return None


class JavaExecutor:
    def execute(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
    ) -> tuple[dict | str | list | None, str | None, str]:
        java_bin = shutil.which("java")
        if not java_bin:
            return None, "Runtime.InvalidRuntime", "Java not installed"

        bootstrap_dir = _ensure_bootstrap_compiled()
        if not bootstrap_dir:
            return None, "Runtime.InvalidRuntime", "Cannot compile Java bootstrap (javac not found)"

        tmpdir = extract_code(code_zip, layer_zips)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)

        # Build classpath: bootstrap dir + extracted code dir + all JARs
        cp_parts = [bootstrap_dir, tmpdir]
        # Add lib/ directory JARs
        lib_dir = os.path.join(tmpdir, "lib")
        if os.path.isdir(lib_dir):
            cp_parts.extend(glob.glob(os.path.join(lib_dir, "*.jar")))
        # Add root-level JARs
        cp_parts.extend(glob.glob(os.path.join(tmpdir, "*.jar")))
        # Java layers put deps in java/lib/
        java_dir = os.path.join(tmpdir, "java")
        if os.path.isdir(java_dir):
            cp_parts.append(java_dir)
            java_lib = os.path.join(java_dir, "lib")
            if os.path.isdir(java_lib):
                cp_parts.extend(glob.glob(os.path.join(java_lib, "*.jar")))

        classpath = os.pathsep.join(cp_parts)
        cmd = [java_bin, "-cp", classpath, f"-Xmx{memory_size}m", "Bootstrap"]
        return run_subprocess(cmd, event, tmpdir, env, timeout)
