---
session: "pr-275-faultin-correctness"
timestamp: "2026-05-13T22:21:15Z"
model: claude-opus-4-7
---

## Human

Check recent PR comments and iterate. When you cannot improve it
further, enable automerge.

## Assistant

## Key decisions

Two correctness bugs from JackDanger's review of the fault-in
framework. Both were real — CI smoke tests passed because they
happened to exercise the baked-in defaults for these families, not the
fault-in paths.

### Bug 1: Java fault-in JRE + Bootstrap.class bytecode mismatch

``install_java.py`` pulls Adoptium JREs (not JDKs) for java8/11/17.
But ``JavaExecutor._ensure_bootstrap_compiled()`` compiles
``Bootstrap.java`` once with the baked ``javac`` (JDK 21) and caches
the result. Java 21 bytecode → ClassFormatError when loaded on
Java 8/11/17 JRE.

Fix: compile with ``javac --release 8``. The resulting Bootstrap.class
is loadable on every JVM major from 8 forward. Bootstrap uses only
JDK 8 features (basic IO, Class.forName, Method.invoke) so this
doesn't lose anything.

### Bug 2: Two dotnet roots → faulted-in SDKs invisible

Original split:
* Baked SDK 9.0 → ``/usr/share/dotnet`` with
  ``/usr/local/bin/dotnet`` symlink.
* Fault-in SDKs → ``/var/lib/robotocore/runtimes/dotnet`` per
  ``_DOTNET_ROOT`` in install_dotnet.py.

The dotnet host walks ONE root for SDKs and runtimes. So
``/usr/local/bin/dotnet`` (rooted at /usr/share/dotnet) never saw the
faulted-in SDKs. ``_list_installed_majors()`` reported only what was
in /usr/share/dotnet. Faulted-in dotnet6/dotnet8 invocations would
silently fall back to net9.0 (host max).

Compounding it: ``_list_installed_majors()`` and ``_cached_tfm`` are
module-level caches that never refresh, so even if the host root were
unified, the cached set would go stale across a fault-in.

Fix in two parts:

1. **Unify DOTNET_ROOT**. Move the baked SDK 9.0 install in the
   Dockerfile to ``/var/lib/robotocore/runtimes/dotnet`` and set
   ``ENV DOTNET_ROOT=...`` to the same path. The
   ``/usr/local/bin/dotnet`` symlink now points there. Fault-in
   installs (which already target the same path) layer on top
   transparently — one host root, multiple SDKs.

2. **Invalidate caches after install**. Added
   ``dotnet.invalidate_caches()`` that resets both
   ``_installed_majors`` and ``_cached_tfm`` to None. The .NET install
   plan calls it after ``dotnet-install.sh`` returns. Pinned by
   ``test_invalidate_caches_clears_both``.

Also removed the now-redundant ``shutil.which("dotnet")`` /
``_write_wrapper`` block from ``DotnetInstallPlan.install``. With the
unified root, the baked symlink is always correct — no need for a
fallback wrapper.

### Tests

484 unit tests pass. Added one for the new ``invalidate_caches()``;
the existing JVM-bytecode tests don't catch the bug because they ran
against an installed JDK, not a fault-in JRE — that's a runtime
integration concern. The CI docker-build job (which builds the full
image and runs handler smoke tests) will exercise both fixes
end-to-end on push.

After this lands and CI is green, enabling automerge.
