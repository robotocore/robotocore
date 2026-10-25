"""Unit tests for the surface-absence manifest's deny-arm matcher.

Pins the D2 arm-context semantics introduced with the directconnect
serving-arm fix: a deny marker may only be credited when the op literal
sits in its own arm's pattern area or inside the `other =>`/`_ =>`
fall-through arm carrying the literal — markers in serving-arm
neighbors (the fall-through or sibling arms) never classify the op as a
modeled deny.
"""

import scripts.build_surface_absence as mod  # noqa: E402


def _deny(tmp_path, source: str, op: str = "DescribeSomething") -> str | None:
    """Run deny_evidence against a synthetic service module."""
    svc_dir = tmp_path / "synthsvc"
    svc_dir.mkdir(exist_ok=True)
    (svc_dir / "mod.rs").write_text(source)
    orig = mod.twin_files
    mod.twin_files = lambda svc: [svc_dir / "mod.rs"]
    try:
        return mod.deny_evidence("synth", op)
    finally:
        mod.twin_files = orig


class TestD2ArmScope:
    """The deny marker must belong to the arm the op itself maps through."""

    def test_serving_arm_neighbor_fallthrough_not_deny(self, tmp_path):
        # directconnect shape: serving arms sit directly above the same
        # match's other=> fall-through (the pre-fix false-positive).
        src = "\n".join(
            [
                "fn dispatch(st: &S) {",
                "    match op {",
                '        "DescribeLocations" => describe_locations(),',
                '        "DescribeVirtualGateways" => describe_virtual_gateways(),',
                "        other => json_error(",
                "            501,",
                '            "NotImplemented",',
                '            &format!("The {other} action has not been implemented"),',
                "        ),",
                "    }",
                "}",
            ]
        )
        assert _deny(tmp_path, src, "DescribeLocations") is None
        assert _deny(tmp_path, src, "DescribeVirtualGateways") is None

    def test_next_sibling_deny_arm_not_deny(self, tmp_path):
        # a deny arm AFTER the literal's serving arm does not deny the op
        src = "\n".join(
            [
                "    match op {",
                '        "DescribeSomething" => self.describe(),',
                '        "DescribeOther" => json_error(501, "NotImplemented", ""),',
                "    }",
            ]
        )
        assert _deny(tmp_path, src, "DescribeSomething") is None
        assert _deny(tmp_path, src, "DescribeOther") is not None

    def test_own_arm_multi_pattern_deny(self, tmp_path):
        # cloudformation/ds style: a multi-pattern deny arm models each op
        src = "\n".join(
            [
                "    match op {",
                '        "DescribeSomething" | "RegisterExtra" => Err(CfnError::new(',
                '            "NotImplemented",',
                '            format!("The {action} action has not been implemented"),',
                "        )",
                "        .status(501),",
                '        _ => Err(not_implemented(action)),',
                "    }",
            ]
        )
        assert "synth" in (_deny(tmp_path, src, "DescribeSomething") or "")
        assert "synth" in (_deny(tmp_path, src, "RegisterExtra") or "")

    def test_multiline_pipe_pattern_deny(self, tmp_path):
        # sdb style: the pattern spans several lines before the `=>`
        src = "\n".join(
            [
                "    match req.operation.as_str() {",
                '        "DomainMetadata"',
                '        | "Select" => Some(not_implemented(req)),',
                "        _ => Some(not_implemented(req)),",
                "    }",
            ]
        )
        assert "synth" in (_deny(tmp_path, src, "Select") or "")
        assert "synth" in (_deny(tmp_path, src, "DomainMetadata") or "")


class TestD2RealTree:
    """The matcher against the checked-out twin sources (repo tree)."""

    def test_directconnect_wave12_serving_arms_not_deny(self):
        for op in (
            "DescribeLocations",
            "DescribeVirtualGateways",
            "DescribeCustomerMetadata",
            "ConfirmCustomerAgreement",
            "DescribeLoa",
            "DescribeConnectionLoa",
        ):
            assert mod.deny_evidence("directconnect", op) is None, op

    def test_multiline_pattern_denies_still_hold(self):
        for svc, op in (
            ("cloudformation", "DescribeTypeRegistration"),
            ("ds", "UpdateTrust"),
            ("sdb", "Select"),
            ("lakeformation", "AssumeDecoratedRoleWithSAML"),
        ):
            ev = mod.deny_evidence(svc, op)
            assert ev is not None and ev.startswith("twin deny arm"), (svc, op, ev)

    def test_vpclattice_routes_literal_not_deny(self):
        # the literal lives in the ROUTES const table; the twin's
        # moto-style `_ =>` fall-through answers 501 → absent_no_handler
        assert mod.deny_evidence(
            "vpclattice", "ListServiceNetworkVpcAssociations"
        ) is None
