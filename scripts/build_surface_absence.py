#!/usr/bin/env python3
"""Build/verify tooling/surface-absence.json — the absence manifest.

The botocore-surface plan (docs/botocore-surface-plan.md §3d) promises every
deny is a *recorded* decision. This script derives one manifest entry per
currently-`not_implemented` probe row, mechanically, from:

  - probes/*.json              (the per-op wire truth corpus)
  - rotocore src               (twin deny arms, moto-absent op tables,
                                committed unit tests pinning the deny)
  - crates/<svc>/src/**        (gateway-core deny arms — s3 et al)
  - the moto fork checkout     (optional at runtime: used to re-derive what
                                moto answers at runtime per fork source; a
                                forkless rebuild preserves committed rows)

Class taxonomy (wave-11 ledger row; docs/botocore-surface-plan.md §3d):

  modeled_deny                rotocore names the op in a deny context: an
                              explicit arm rendering the NotImplemented
                              envelope, a moto-absent op table consumed by
                              the module's deny path (ABSENT_OPS pattern),
                              or a committed unit test pinning the deny
                              (501 + NotImplemented envelope / is_none
                              arm fall-through).
  absent_moto_crash_faithful  moto routes the op but its handler crashes on
                              cold probe state (the workspaces KeyError-500
                              family) — decided per fork source via
                              OVERRIDES carrying a fork/wave citation.
  absent_no_handler           the twin has no deny arm and moto is absent:
                              moto raises its dispatch NotImplementedError
                              → HTTP 501 at runtime, so the twin's gateway
                              501 is faithful absence.
  legacy_gap                  residual rows disagreeing with moto truth:
                              moto implements the op (fork handler exists)
                              while the twin still 501s, or the probe row
                              predates the twin's arm (stale probe row).

Row shape: {"status": "not_implemented", "class": <one of the four>,
"evidence": "<citations>", "decided_in": "wave-11 ledger row …"}.

Determinism: the build is a pure function of probes + twin sources + the
fork (when present). Services and ops are sorted. A build without the fork
preserves committed moto-side classes verbatim (no rewrite diff). Line
numbers inside evidence move when rust files change — rebuild to refresh
them; the gate treats stale rows as a warning, not a failure.

Usage:
  uv run python scripts/build_surface_absence.py              # build/refresh
  uv run python scripts/build_surface_absence.py --check      # verify only

Exit codes: 0 ok (warnings on stderr); 2 = unbacked class, unrecorded
probe row, or a moto-implemented row without an OVERRIDES disposition.
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBES = ROOT / "probes"
MANIFEST = ROOT / "tooling" / "surface-absence.json"
ROT = ROOT / "rotocore" / "src" / "services"
CRATES = ROOT / "crates"

CLASSES = (
    "modeled_deny",
    "absent_moto_crash_faithful",
    "absent_no_handler",
    "legacy_gap",
)
DECIDED_IN = "wave-11 ledger row (surface-absence manifest build)"

# The deployed fork identity (docs/moto-parity-progress.md wave-2 ruling;
# uv-cache fork and the oracle venv are sha256-identical to 2016563fd).
FORK_ID = "moto 5.2.4 fork (JackDanger/moto@robotocore/all-fixes 2016563fd)"

MOTO_NOT_IMPL = (
    'moto dispatch raises NotImplementedError("The <action> action has not '
    'been implemented") → HTTP 501 '
)

# Probe service names whose arms live in crates/<svc>/src (gateway core)
# instead of rotocore/src/services. Derived from the tree so the list
# follows what is present, not a static guess.
CRATE_SERVICE_DIRS = {p.name: p / "src" for p in sorted(CRATES.iterdir()) if (p / "src").is_dir()}
CRATE_SERVICE_DIRS["logs"] = CRATES / "cloudwatch-logs" / "src"

# ---------------------------------------------------------------------------
# Manually-disposed rows: moto-implemented ops the twin still 501s (or probe
# rows predating a twin arm). Each carries dual citations checked at build
# time, recorded by hand from the fork checkout + live verification on the
# local twin (2026-10-24). A moto-implemented not_implemented row WITHOUT an
# entry here fails the build: the manifest never silently classifies those.
# ---------------------------------------------------------------------------
OVERRIDES: dict[tuple[str, str], dict] = {
    ("s3", "PutObjectAcl"): {
        "class": "legacy_gap",
        "evidence": (
            "stale probe row: the twin arm serves the op — crates/s3/src/"
            'handler.rs:538 (`"PutObjectAcl" => self.put_object_acl(&req)`); '
            "fork handler moto/s3/responses.py:2111 put_object_acl; fresh-"
            "session verification answers 200 (verified 2026-10-24 and "
            "re-verified 2026-10-25), while the fleet-sweep probe still "
            "records an error envelope: after a PutObject probe on the "
            "same botocore session the request resolves through the "
            "empty-op catch-all at crates/s3/src/handler.rs:636 (400 "
            "NotImplemented, 'The operation  is not implemented') — a "
            "probe-client artifact, not twin absence"
        ),
    },
    ("s3", "UploadPartCopy"): {
        "class": "legacy_gap",
        "evidence": (
            "stale probe row: the twin copy machinery serves the op — "
            "crates/s3/src/handler.rs:1918 (PUT with uploadId and "
            "x-amz-copy-source routes to the copy path); fork handler "
            "moto/s3/responses.py:2578 upload_part_copy; fresh-session "
            "verification answers 200 with CopyPartResult (verified "
            "2026-10-24 and re-verified 2026-10-25), while the fleet sweep "
            "reproduces the same single-session degradation the "
            "PutObjectAcl row shows (400 NotImplemented via the empty-op "
            "catch-all, handler.rs:636) — a probe artifact, not absence"
        ),
    },
    ("timestreaminfluxdb", "UntagResource"): {
        "class": "legacy_gap",
        "evidence": (
            "genuine fork-present residual: moto implements it — fork "
            "handler moto/timestreaminfluxdb/responses.py:92 "
            "untag_resource; the twin has no arm (rotocore/src/services/"
            "timestreaminfluxdb.rs names no UntagResource deny) and "
            "answers 501 — verified live 2026-10-24; needs the wave port"
        ),
    },
}


def camel_to_moto_snake(op: str) -> str:
    # moto's camelcase_to_underscores (moto/core/utils.py): insert `_` at a
    # lower/digit → upper boundary.
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", op).lower()


def moto_module(svc: str) -> str:
    # moto crate names use underscores (cognito_idp, service_quotas,
    # kinesis_video_archived_media).
    return f"moto/{svc.replace('-', '_')}"


def read_corpus() -> dict[str, dict]:
    """service → {op: probe row} for not_implemented rows only."""
    rows: dict[str, dict] = {}
    for f in sorted(PROBES.glob("*.json")):
        try:
            data = json.loads(f.read_text())
        except Exception as e:  # noqa: BLE001
            print(f"warn: unreadable probe artifact {f.name}: {e}", file=sys.stderr)
            continue
        svc = data.get("service", f.stem)
        rows[svc] = {
            op["operation"]: op
            for op in data.get("operations", [])
            if op.get("status") == "not_implemented"
        }
    return {s: ops for s, ops in rows.items() if ops}


def twin_files(svc: str) -> list[Path]:
    """Rust modules that may name this service's ops (arms, tables, tests)."""
    files: list[Path] = []
    for stem in (svc.replace("-", "_"), svc.replace("-", "")):
        d = ROT / stem
        if d.is_dir():
            files.extend(sorted(d.rglob("*.rs")))
        if (ROT / f"{stem}.rs").exists():
            files.append(ROT / f"{stem}.rs")
    crate = CRATE_SERVICE_DIRS.get(svc)
    if crate and crate.is_dir():
        files.extend(sorted(crate.rglob("*.rs")))
    out, uniq = [], set()
    for f in files:
        if f.exists() and f not in uniq:
            out.append(f)
            uniq.add(f)
    return out


def _fn_span(lines: list[str], idx: int) -> tuple[int, int] | None:
    """Brace-matched span of the `fn` containing line idx (0-based)."""
    fn_start = None
    for i in range(idx, -1, -1):
        if re.search(r"\bfn\s+\w+", lines[i]):
            fn_start = i
            break
    if fn_start is None:
        return None
    depth = 0
    opened = False
    for i in range(fn_start, len(lines)):
        for ch in lines[i]:
            if ch == "{":
                depth += 1
                opened = True
            elif ch == "}" and opened:
                depth -= 1
        if opened and depth <= 0:
            return (fn_start, i)
    return (fn_start, len(lines) - 1)


_ARM_HEAD = re.compile(r'^\s*(?:"(?:[^"\\]|\\.)*"|other\b|_\b)')
_FALL_HEAD = re.compile(r"^\s*(?:other|_)\s*=>")
ARM_DENY = r"not_implemented\(|NotImplemented|\.status\(501\)"


def _arm_span(lines: list[str], idx: int) -> tuple[int, int] | None:
    """Span (0-based, inclusive) of the match arm that opens on line idx
    (a quoted-pattern or other/_ head); None otherwise.

    A quoted head without `=>` may open a multi-line pipe pattern
    (`"A"` newline `| "B" => …`), so the span scans on until the arm's
    terminating comma — or a bare `}` for a comma-less last arm — at
    balanced bracket depth, keeping a multi-line arm body inside the span.
    """
    if not _ARM_HEAD.match(lines[idx]):
        return None
    depth = 0
    for i in range(idx, len(lines)):
        row = lines[i]
        depth += (
            row.count("{") + row.count("[") + row.count("(")
        ) - (row.count("}") + row.count("]") + row.count(")"))
        if depth == 0 and (
            row.rstrip().endswith(",") or (i > idx and row.rstrip() in ("}", "};"))
        ):
            return (idx, i)
    return (idx, len(lines) - 1)


def _arm_in_pattern(lines: list[str], span: tuple[int, int], idx: int) -> bool:
    """Whether line idx lies in the pattern area of the arm `span` (at or
    before the arm's first `=>` line)."""
    for i in range(span[0], span[1] + 1):
        if "=>" in lines[i]:
            return span[0] <= idx <= i
    return False


def _enclosing_arm(lines: list[str], idx: int) -> tuple[int, int] | None:
    """Match-arm span that contains line idx, whichever head opens it."""
    i = idx
    while i >= 0:
        span = _arm_span(lines, i)
        if span and span[0] <= idx <= span[1]:
            return span
        i -= 1
    return None


def deny_evidence(svc: str, op: str) -> str | None:
    """First deny-recording idiom the twin sources show for op, cited."""
    lit = f'"{op}"'
    callpat = re.compile(rf"not_implemented\(\s*{re.escape(lit)}")
    table_start = re.compile(r"const\s+\w+\s*:\s*&\[")
    for f in twin_files(svc):
        text = f.read_text()
        rel = f.relative_to(ROOT).as_posix()
        lines = text.split("\n")

        # D1 — direct call-site: not_implemented("<op>")
        m = callpat.search(text)
        if m:
            ln = text[: m.start()].count("\n") + 1
            return f"twin not_implemented call-site at {rel}:{ln}"

        # D4 — membership in a const deny table documented against moto
        for tm in table_start.finditer(text):
            eq = text.find("=", tm.start())
            j = text.find("[", eq) if eq != -1 else -1
            depth, end = 0, None
            for k in range(j, len(text)) if j != -1 else []:
                if text[k] == "[":
                    depth += 1
                elif text[k] == "]":
                    depth -= 1
                    if depth == 0:
                        end = k
                        break
            if end is None:
                continue
            table = text[j:end]
            if lit not in table:
                continue
            doc = text[max(0, tm.start() - 600) : tm.start()]
            if re.search(r"moto|fork", doc, re.IGNORECASE):
                ln = text[:j].count("\n") + 1 + table[: table.index(lit)].count("\n")
                return (
                    f"twin moto-absent op table at {rel}:~{ln} (op listed in a "
                    "const table citing the fork, answered by the module's "
                    "501 deny path)"
                )

        for m2 in re.finditer(re.escape(lit), text):
            ln = text[: m2.start()].count("\n") + 1
            # D3 — the literal lives in a unit test pinning the deny
            span = _fn_span(lines, ln - 1)
            if span:
                attr = "\n".join(lines[max(0, span[0] - 2) : span[0] + 1])
                body = "\n".join(lines[span[0] : span[1] + 1])
                if "#[test]" in attr and (
                    ("501" in body and ("NotImplemented" in body or "is_none()" in body))
                    or "is_none()" in body
                ):
                    return f"twin unit test pins the deny at {rel}:{ln}"
            # D2 — arm context: deny only through the arm the op itself
            # maps through: the literal sits in the enclosing arm's
            # PATTERN area (single- or multi-pattern head; multi-line pipe
            # patterns included), or inside the `other =>`/`_ =>`
            # fall-through arm whose span carries the literal, and the deny
            # marker occurs within that arm's span. A marker in a
            # serving-arm *neighbor* — the match's fall-through or a
            # sibling arm that begins after the literal's own serving arm
            # has already answered — must not model the op as denied (the
            # six directconnect wave-12 serving arms sit directly above the
            # same match's fall-through).
            span = None
            outer = _enclosing_arm(lines, ln - 1)
            if outer is not None and (
                _arm_in_pattern(lines, outer, ln - 1) or _FALL_HEAD.match(lines[outer[0]])
            ):
                span = outer
            if span is not None and re.search(
                ARM_DENY, "\n".join(lines[span[0] : span[1] + 1])
            ):
                return f"twin deny arm at {rel}:{ln}"
    return None


# The pinned moto fork checkout on this machine — the deployed fork identity
# the FORK_ID citations name (docs/moto-parity-progress.md wave-2 ruling).
# The legacy ~/.moto-venv site-packages copy is deliberately NOT consulted
# any more: it has drifted ahead of the git pin (it already carries
# vpclattice.list_service_network_vpc_associations, which the pinned fork
# does not), so every runtime-truth question must resolve against the pin.
PINNED_FORK = (
    Path.home()
    / ".cache/uv/git-v1/checkouts/eacacc220fef84f0/2016563fd/moto"
)


def fork_path() -> Path | None:
    """Locate the pinned moto fork checkout, when available."""
    argv = sys.argv
    if "--fork" in argv:
        p = Path(argv[argv.index("--fork") + 1])
        return p if p.is_dir() else None
    return PINNED_FORK if PINNED_FORK.is_dir() else None


def moto_module_dir(fork: Path | None, svc: str) -> Path | None:
    """The fork's module dir for a service, matching moto's naming (moto
    strips dashes entirely: cognitoidp, servicequotas, kinesisvideo…)."""
    if fork is None:
        return None
    for stem in (svc.replace("-", "_"), svc.replace("-", "")):
        cand = fork / stem
        if (cand / "responses.py").exists():
            return cand
    return None


def moto_present(fork: Path | None, svc: str, op: str) -> bool:
    """moto routes and handles the op per fork source."""
    mod = moto_module_dir(fork, svc)
    if mod is None:
        return False
    snake = camel_to_moto_snake(op)
    return bool(re.search(rf"def {re.escape(snake)}\(self\b", (mod / "responses.py").read_text()))


def moto_handler_bare_raise(fork: Path | None, svc: str, op: str) -> str | None:
    """moto handler whose body is a bare `raise NotImplementedError(...)` →
    moto 501s the op at runtime (the deliberate not-yet-implemented arm).
    Returns a fork citation, or None when the handler does real work."""
    mod = moto_module_dir(fork, svc)
    if mod is None:
        return None
    text = (mod / "responses.py").read_text()
    snake = camel_to_moto_snake(op)
    m = re.search(rf"def {re.escape(snake)}\(self\b", text)
    if not m:
        return None
    lines = text.split("\n")
    start = text[: m.start()].count("\n")
    def_indent = len(lines[start]) - len(lines[start].lstrip())
    body: list[tuple[int, str]] = []
    for i in range(start + 1, len(lines)):
        line = lines[i]
        if line.strip().startswith("def ") and len(line) - len(line.lstrip()) <= def_indent:
            break
        body.append((i, line))
    rel = [(i, ln) for i, ln in body if ln.strip() and not ln.lstrip().startswith("#")]
    if not rel:
        return None
    base = min(len(ln) - len(ln.lstrip()) for _, ln in rel)
    stmts = [
        (i, ln)
        for i, ln in rel
        if len(ln) - len(ln.lstrip()) == base and not re.fullmatch(r"\)+,?", ln.strip())
    ]
    if len(stmts) == 1 and re.match(r"raise NotImplementedError\(", stmts[0][1].strip()):
        i, ln = stmts[0]
        return (
            f"fork handler {mod.name}/responses.py:{i + 1} — the `{snake}` "
            "handler body is a bare `raise NotImplementedError(...)` so moto "
            "501s the op at runtime"
        )
    return None


def fork_absent_note(fork: Path | None, svc: str, op: str) -> str:
    """One-line statement of what moto answers at runtime for the op."""
    mod = moto_module_dir(fork, svc)
    if mod is not None and fork is not None:
        return (
            f"{FORK_ID}: {mod.relative_to(fork).as_posix()}/responses.py has "
            f"no `{camel_to_moto_snake(op)}` handler — {MOTO_NOT_IMPL}; "
            "the twin mirrors moto's absence with the gateway NotImplemented 501"
        )
    if fork is not None:
        return (
            f"{FORK_ID}: service unmodeled (no moto module for {svc}) — moto "
            f"answers 404/501 for {op}; the twin mirrors the absence with "
            "the gateway NotImplemented 501"
        )
    # Forkless rebuild for a genuinely new row: the citation is pre-baked in
    # the manifest build that had the checkout.
    return (
        f"{FORK_ID}: no `{camel_to_moto_snake(op)}` handler in "
        f"{moto_module(svc)} — {MOTO_NOT_IMPL}; twin gateway 501 mirrors "
        "moto's absence (citation from the manifest build that had the fork)"
    )


def derive(
    probes: dict[str, dict], fork: Path | None, committed: dict | None
) -> tuple[dict, list[str]]:
    """service → op → entry; deterministic; every not_implemented row."""
    committed_rows = committed.get("services", {}) if isinstance(committed, dict) else {}
    counts = {c: 0 for c in CLASSES}
    counts["not_implemented"] = 0
    unclassified: list[str] = []
    services: dict[str, dict] = {}
    for svc in sorted(probes):
        entries: dict[str, dict] = {}
        for op in sorted(probes[svc]):
            counts["not_implemented"] += 1
            over = OVERRIDES.get((svc, op))
            if over is not None:
                cls, ev = over["class"], over["evidence"]
                counts[cls] += 1
            elif moto_present(fork, svc, op):
                raise_cite = moto_handler_bare_raise(fork, svc, op)
                if raise_cite:
                    # moto's own arm deliberately denies: the twin's 501
                    # mirrors what moto returns at runtime (fork source).
                    cls = "absent_moto_crash_faithful"
                    ev = (
                        f"{raise_cite}; the twin has no arm answering it, so "
                        "the gateway NotImplemented 501 mirrors what moto "
                        "returns at runtime"
                    )
                else:
                    cls = "legacy_gap"
                    ev = (
                        f"UNCLASSIFIED: moto implements {op} — extend "
                        "OVERRIDES in scripts/build_surface_absence.py "
                        "with a fork-source citation before the gate can pass"
                    )
                    unclassified.append(f"{svc}.{op}")
                counts[cls] += 1
            elif fork is not None:
                # The fork is available: re-derive fully (including rows a
                # previous build recorded).
                twin_ev = deny_evidence(svc, op)
                if twin_ev:
                    cls = "modeled_deny"
                    ev = f"{twin_ev}; {fork_absent_note(fork, svc, op)}"
                else:
                    cls = "absent_no_handler"
                    ev = fork_absent_note(fork, svc, op)
                counts[cls] += 1
            else:
                # Forkless rebuild: preserve the committed moto-side fact for
                # rows already recorded; fresh rows get forkless citations.
                old = committed_rows.get(svc, {}).get(op)
                if old is not None:
                    cls = old.get("class") if old.get("class") in CLASSES else "absent_no_handler"
                    ev = old.get("evidence") or fork_absent_note(None, svc, op)
                else:
                    twin_ev = deny_evidence(svc, op)
                    if twin_ev:
                        cls = "modeled_deny"
                        ev = f"{twin_ev}; {fork_absent_note(None, svc, op)}"
                    else:
                        cls = "absent_no_handler"
                        ev = fork_absent_note(None, svc, op)
                counts[cls] += 1
            entries[op] = {
                "status": "not_implemented",
                "class": cls,
                "evidence": ev,
                "decided_in": DECIDED_IN,
            }
        services[svc] = entries
    manifest = {
        "comment": (
            "surface-absence manifest (generated) — every not_implemented "
            "probe row is a recorded decision; see docs/botocore-surface-plan.md "
            "§3d; rebuild deterministically via scripts/build_surface_absence.py"
        ),
        "counts": counts,
        "services": services,
    }
    return manifest, unclassified


def load_committed() -> dict | None:
    try:
        return json.loads(MANIFEST.read_text())
    except Exception:
        return None


def verify_backing(rows: dict, fork: Path | None) -> list[str]:
    """Hard failures: a manifest class not backed by evidence. `rows` is
    the service → op → entry map to verify."""
    failures: list[str] = []
    for svc, ops in rows.items():
        for op, row in ops.items():
            cls = row.get("class")
            if cls not in CLASSES:
                failures.append(f"{svc}.{op}: unknown class {cls!r}")
                continue
            if cls == "modeled_deny":
                f = None
                for cand in twin_files(svc):
                    if cand.exists() and f'"{op}"' in cand.read_text():
                        f = cand
                        break
                if f is None:
                    failures.append(
                        f"{svc}.{op}: modeled_deny citation not backed by any "
                        f"twin module — {row.get('evidence', '')[:120]}"
                    )
            elif cls == "legacy_gap":
                ev = row.get("evidence", "")
                if "UNCLASSIFIED" in ev:
                    continue
                name = moto_module(svc)
                if fork is not None and moto_module_dir(fork, svc) is None:
                    failures.append(
                        f"{svc}.{op}: legacy_gap cites {name} but the "
                        "fork checkout is unverifiable here"
                    )
                elif fork is None and name not in ev:
                    failures.append(f"{svc}.{op}: legacy_gap evidence cites no {name} source")
    return failures


def missing_rows(committed: dict | None, probes: dict[str, dict]) -> list[str]:
    if committed is None:
        return []
    return [
        f"{svc}.{op}"
        for svc, ops in probes.items()
        for op in ops
        if op not in committed.get("services", {}).get(svc, {})
    ]


def stale_rows(committed: dict | None, probes: dict[str, dict]) -> list[str]:
    if committed is None:
        return []
    return [
        f"{svc}.{op}"
        for svc, ops in committed.get("services", {}).items()
        for op in ops
        if op not in probes.get(svc, {})
    ]


def prune_stale(committed: dict, probes: dict[str, dict]) -> tuple[dict, list[str]]:
    """The committed manifest trimmed to rows still not_implemented in the
    probes; returns (checked-rows, stale-rows). Stale rows are gate-warned,
    never silently deleted."""
    stale = [
        f"{svc}.{op}"
        for svc, ops in committed.get("services", {}).items()
        for op in ops
        if op not in probes.get(svc, {})
    ]
    checked = {
        svc: {op: row for op, row in ops.items() if op in probes.get(svc, {})}
        for svc, ops in committed.get("services", {}).items()
    }
    checked = {s: ops for s, ops in checked.items() if ops}
    return checked, stale


def verify_unbacked(rows: dict) -> list[str]:
    """Committed rows left UNCLASSIFIED by a forkless build."""
    return [
        f"{svc}.{op}"
        for svc, ops in rows.items()
        for op, row in ops.items()
        if "UNCLASSIFIED" in (row.get("evidence") or "")
    ]


def build(check: bool = False) -> int:
    probes = read_corpus()
    fork = fork_path()
    committed = load_committed()
    manifest, unclassified = derive(probes, fork, committed)

    if check:
        # Verify the COMMITTED manifest: classes must be backed by the
        # current twin/fork sources and every probe row must be recorded.
        if committed is None:
            print(
                "manifest missing — run scripts/build_surface_absence.py once",
                file=sys.stderr,
            )
            return 2
        checked, stale = prune_stale(committed, probes)
        counts = {c: 0 for c in CLASSES}
        counts["not_implemented"] = 0
        for ops in checked.values():
            for row in ops.values():
                counts["not_implemented"] += 1
                counts[row.get("class", "absent_no_handler")] += 1
        total = counts.pop("not_implemented")
        per = ", ".join(f"{c}={counts[c]}" for c in CLASSES)
        print(f"surface-absence manifest: {total} rows ({per})")

        if fork is None:
            print(
                "note: moto fork unavailable — moto-side citations trusted "
                "from the committed manifest",
                file=sys.stderr,
            )
        unclassified = verify_unbacked(checked)
        for line in unclassified:
            print(f"not backed: {line} — extend OVERRIDES", file=sys.stderr)
        if unclassified:
            return 2
        failures = verify_backing(checked, fork)
        for line in failures:
            print(f"not backed: {line}", file=sys.stderr)
        if failures:
            return 2
        unrecorded = missing_rows(committed, probes)
        if unrecorded:
            print(
                "unrecorded not_implemented probe rows — run the builder (make absence-manifest):",
                file=sys.stderr,
            )
            for row in unrecorded[:10]:
                print(f"  {row}", file=sys.stderr)
            return 2
        if stale:
            print(
                f"warn: {len(stale)} manifest rows are no longer "
                "not_implemented in probes (stale):",
                file=sys.stderr,
            )
            for row in stale[:10]:
                print(f"  {row}", file=sys.stderr)
        return 0

    # build: write the mechanically re-derived manifest
    serialized = json.dumps(manifest, indent=1, sort_keys=True) + "\n"
    if committed != manifest:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(serialized)
        print(f"surface-absence manifest rebuilt → {MANIFEST.relative_to(ROOT)}")
    else:
        print("surface-absence manifest unchanged")

    counts = dict(manifest["counts"])
    total = counts.pop("not_implemented")
    per = ", ".join(f"{c}={counts[c]}" for c in CLASSES)
    print(f"surface-absence manifest: {total} rows ({per})")

    if fork is None:
        print(
            "note: moto fork unavailable — moto-side classes preserved from the committed manifest",
            file=sys.stderr,
        )
    for row in unclassified:
        print(f"not backed: {row} — extend OVERRIDES", file=sys.stderr)
    if unclassified:
        return 2

    failures = verify_backing(manifest["services"], fork)
    for line in failures:
        print(f"not backed: {line}", file=sys.stderr)
    if failures:
        return 2

    stale = stale_rows(committed, probes)
    if stale:
        print(
            f"warn: {len(stale)} manifest rows are no longer not_implemented in probes (stale):",
            file=sys.stderr,
        )
        for row in stale[:10]:
            print(f"  {row}", file=sys.stderr)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Surface-absence manifest builder/verifier")
    ap.add_argument(
        "--check",
        action="store_true",
        help="verify the manifest against the corpus without rewriting it",
    )
    ap.add_argument("--fork", help="path to the moto fork checkout")
    args = ap.parse_args()
    return build(check=args.check)


if __name__ == "__main__":
    sys.exit(main())
