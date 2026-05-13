"""Java runtime executor — compiles bootstrap + runs handler via subprocess."""

import glob
import logging
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


logger = logging.getLogger(__name__)

# Maps Lambda runtime identifiers to the versioned java binary name in the image.
# The Dockerfile installs JDKs (or symlinks) named java8, java11, java17, java21
# alongside a default "java". JVM 21 can run java8/11/17 bytecode, so symlinks
# pointing at a single JDK are an acceptable fallback in resource-constrained
# images. Runtimes absent from this map fall back to plain "java" with a warning.
_RUNTIME_BINARY: dict[str, str] = {
    "java8": "java8",
    "java8.al2": "java8",
    "java11": "java11",
    "java17": "java17",
    "java21": "java21",
}


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
    except Exception as exc:  # noqa: BLE001
        logger.debug("_ensure_bootstrap_compiled: run failed (non-fatal): %s", exc)
    return None


class JavaExecutor:
    def __init__(self, runtime: str = "") -> None:
        self._runtime = runtime

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
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        java_bin = self._resolve_binary()
        if not java_bin:
            return None, "Runtime.InvalidRuntime", "Java not installed"

        bootstrap_dir = _ensure_bootstrap_compiled()
        if not bootstrap_dir:
            return None, "Runtime.InvalidRuntime", "Cannot compile Java bootstrap (javac not found)"

        tmpdir = extract_code(code_zip, layer_zips, code_dir=code_dir, function_name=function_name)
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

    def _resolve_binary(self) -> str | None:
        """Return the java binary path, preferring the version-specific one.

        Logs a warning in two cases so a runtime mismatch is never silent:
          (a) a known runtime asked for ``javaX`` but that binary isn't on
              $PATH — we fall back to plain ``java`` (different JVM version);
          (b) an unknown runtime identifier — we don't recognize it at all
              and fall back to plain ``java``.
        """
        versioned = _RUNTIME_BINARY.get(self._runtime)
        if versioned:
            path = shutil.which(versioned)
            if path:
                return path
            logger.warning(
                "Versioned java binary %r for runtime %r not on $PATH — "
                "falling back to default 'java' (the executed JVM will not "
                "match the function's declared runtime).",
                versioned,
                self._runtime,
            )
        elif self._runtime:
            logger.warning(
                "No versioned java binary for runtime %r — falling back to 'java'. Supported: %s",
                self._runtime,
                ", ".join(sorted(_RUNTIME_BINARY)),
            )
        return shutil.which("java")
