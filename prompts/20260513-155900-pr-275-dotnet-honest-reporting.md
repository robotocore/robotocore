---
session: "pr-275-dotnet-honest-reporting"
timestamp: "2026-05-13T15:59:00Z"
model: claude-opus-4-7
---

## Human

Check new review comments and then review this PR yourself. How are you
tracking toward support for every version of every runtime that Lambda
supports?

## Assistant

## Key decisions

JackDanger left two more inline comments after my host-max .NET fix. Both
were correct and surfaced the same underlying truth: once `_detect_tfm()`
became "always host-max", the warning and the endpoint reporting both
overstated what we actually deliver.

### Fix 1: warn on every runtime != host-max, not just on absent

Old logic: warn when requested major isn't in installed majors. So on a
`{6, 8, 9}` host:
  * `_detect_tfm("dotnet6")` → net9.0, no warning (6 is "installed",
    just not the max). Mismatch silent.
  * `_detect_tfm("dotnet8")` → net9.0, no warning. Mismatch silent.

New logic: warn whenever the requested major != host max. So:
  * `_detect_tfm("dotnet6")` on `{6, 8, 9}` → net9.0 + warning.
  * `_detect_tfm("dotnet9")` on `{6, 8, 9}` → net9.0 + no warning.
  * `_detect_tfm("dotnet6")` on `{8}` → net8.0 + warning.

Added two new tests:
  * `test_detect_tfm_warns_when_requested_below_host_max` — pins the new
    warning path on a multi-major host.
  * `test_detect_tfm_with_runtime_equal_to_host_max_no_warning` — pins
    the "no warning when matching" path.

The pre-existing `test_detect_tfm_falls_back_when_requested_missing`
still works (warns when absent), but its docstring updated to clarify
the warning trigger is "requested != host max" rather than "requested
not installed".

### Fix 2: endpoint reports only the runtime we faithfully execute

`/_robotocore/runtimes`'s `versions["dotnet"]` previously listed every
runtime whose major was installed. On `{6, 8, 9}` that meant
`[dotnet6, dotnet8, dotnet9]` — but `_detect_tfm()` runs all three at
net9.0. The endpoint was advertising fidelity we don't deliver.

New behavior: report only the runtime whose major == host max.

```
{6, 8, 9}  →  versions.dotnet = ["dotnet9"]
{8}        →  versions.dotnet = ["dotnet8"]
{}         →  versions.dotnet = []
```

This makes `versions.dotnet` mean "robotocore can faithfully execute
this Lambda runtime" instead of "this runtime is installed somewhere on
the host".

Added two new endpoint tests that patch `_installed_majors` to assert
both single-major and multi-major scenarios.

### Cleanup: github-code-quality bot's import-style nit

The bot kept flagging mixed `import X` + `from X import Y` styles in
the test files. Settled on a consistent pattern that satisfies both
rounds: drop the module-alias imports, define a single string constant
at module top (e.g.
`_RUBY_LOGGER = "robotocore.services.lambda_.runtimes.ruby.logger"`),
and use `patch(_RUBY_LOGGER + ".warning")` instead of
`patch.object(ruby_mod.logger, "warning")`. Applied to all four affected
files (ruby/java/python/node tests).

### Tests + lint

459 tests in the affected directories pass. Ruff clean. Pushing to PR
#275 and replying to the inline comments.
