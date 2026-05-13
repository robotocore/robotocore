---
session: "pr-275-runtime-faultin-framework"
timestamp: "2026-05-13T19:08:43Z"
model: claude-opus-4-7
---

## Human

What was the cost of this? Are our images bloated now? What should we
optimize before merging this?

[after presenting numbers: standard 722→987MB, java-and-dotnet
1578→3538MB; proposed strip + JDK→JRE + .NET trim optimizations]

I like this plan, but can we also devise a way that a user faults-in
the runtime they need when they try to use it? Only the users of these
JVM versions, e.g., should pay the cost of them.

## Assistant

## Key decisions

The pure "trim the image" plan would shave ~750MB off the heavy image.
But the user-pays-only-for-what-they-use design (fault-in installs)
gets the same effect AND scales linearly: a customer who only needs
java17 doesn't pay for java8/11/21 SDK download/storage at all.

I asked two decisions before implementing:

1. **Ruby source** — picked "pull from docker.io ruby:X-slim" (over
   compile-from-source). Already what we use baked-in; same wrapper
   shape; ~30-60s warm vs ~2min cold. Cost: a stdlib-only Docker
   Registry HTTP client (~120 lines).

2. **Install UX** — picked "block the invocation, log progress" (over
   fail-with-retry or silent fallback). Closest to real AWS Lambda cold
   start; subsequent invocations are fast; logs show installs clearly.

### Architecture

``runtimes/install.py`` is the framework module:
* ``InstallPlan`` dataclass — runtime ID, family, install prefix,
  wrapper binary name. Subclasses implement ``install()``.
* ``ensure_installed(runtime)`` — idempotent; holds ``flock`` over
  ``/var/lib/robotocore/runtimes/.locks/<rt>.lock`` so concurrent
  invocations don't double-download. Returns True when the runtime is
  ready to use.
* ``_load_plans()`` — lazy import of the five language-specific install
  modules, each of which registers its plans at top-level on first import.
* Configuration: ``ROBOTOCORE_RUNTIME_CACHE_DIR``,
  ``ROBOTOCORE_RUNTIME_BIN_DIR``,
  ``ROBOTOCORE_RUNTIME_DOWNLOAD_TIMEOUT``,
  ``ROBOTOCORE_RUNTIME_FAULTIN={disabled,0,false,off}``.

### Per-language installers

* ``install_java.py`` — Eclipse Temurin via Adoptium API.
  ``https://api.adoptium.net/v3/binary/latest/{major}/ga/linux/{arch}/jre/hotspot/normal/eclipse``
  is a redirect to the current JRE tarball. We fetch the JRE (not JDK)
  because the bootstrap is pre-compiled at image build time; runtime
  invocations only need ``java``.

* ``install_node.py`` — official nodejs.org tarballs. The "latest-vX.x"
  symlink resolves to the current minor.patch; we parse it from the
  SHASUMS256.txt file at that URL.

* ``install_python.py`` — Astral's
  ``python-build-standalone`` GitHub releases. Statically-linked
  portable CPython for 3.8 through 3.13 on x86_64 + aarch64 Linux.

* ``install_dotnet.py`` — Microsoft's official ``dotnet-install.sh``.
  All SDKs share a single install root so the dotnet host can find
  every TFM at once; each plan's prefix exists only for the
  ``.installed`` marker.

* ``install_ruby.py`` — stdlib-only Docker Registry HTTP client.
  Fetches an anonymous auth token, resolves the multi-arch manifest
  index to the linux/{arch} variant, streams each layer (gzipped tar)
  and extracts members whose path starts with ``usr/local/``. Writes a
  wrapper that mirrors the Dockerfile's shape (LD_LIBRARY_PATH +
  RUBYLIB + GEM_PATH).

* ``install_util.py`` — shared helpers: ``download_and_extract_tarball``
  for .tar.gz / .tar.xz / .tar with ``strip_components=N``, and
  ``download_to_file`` for single-file downloads.

### Integration with executors

``_resolve_binary()`` in ruby/java/node now tries ``ensure_installed()``
before falling back to the default. Python's ``_resolve_subprocess_binary()``
does the same. On install success, the wrapper is at
``/usr/local/bin/<rt>`` and the next ``shutil.which()`` finds it
without re-entering the install path.

### Endpoint + UX

* ``GET /_robotocore/runtimes`` adds a ``status`` field per runtime:
  ``installed`` / ``available_to_install`` / ``unavailable``. Plus a
  ``faultin_disabled`` boolean reflecting the env var.
* ``POST /_robotocore/runtimes/install`` with body
  ``{"runtimes": ["java17", "dotnet6"]}`` pre-warms one or more
  runtimes synchronously. Returns per-runtime
  ``installed`` / ``already_installed`` / ``failed`` / ``no_installer``.
  Useful in CI setup so first real invocation is fast.

### Testing

``conftest.py`` (autouse fixture, all lambda runtime tests) disables
fault-in by default — no test should hit the network or write outside
``tmp_path``. ``test_runtime_install.py`` re-enables it explicitly via
its ``isolated_cache`` fixture, which also redirects CACHE_DIR and
WRAPPER_BIN_DIR.

Tests cover:
* Plan registry (java8.al2 shares prefix with java8, unknown returns
  None, all five families register).
* ``ensure_installed`` (idempotent when marker present, runs install
  when missing, returns False when disabled, fails gracefully when
  install raises).
* Concurrency (three threads racing → exactly one install).
* ``is_installed``.
* Endpoint integration (``status`` field, ``/install`` endpoint
  success/failure/already-installed paths).

483 unit tests pass. Ruff clean.

### What's NOT in this commit

The Dockerfile still bakes in every version. The fault-in machinery is
infrastructure; the size savings come from a follow-up commit that
removes the COPY'd non-default versions and lets fault-in fetch them
on demand. Splitting these means the fault-in framework can be reviewed
in isolation before we change the image shape.

Image-size impact (projected after the follow-up):
* standard: 987MB → ~620MB
* java-and-dotnet: 3538MB → ~1500MB
