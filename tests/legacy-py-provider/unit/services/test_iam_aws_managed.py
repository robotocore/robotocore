"""AWS-managed IAM policies are materialized in an account when a request names them."""

from moto.iam.models import iam_backends

from robotocore.services.iam.provider import _ensure_aws_managed_policy


def test_named_managed_policy_is_materialized_in_account():
    arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
    _ensure_aws_managed_policy(arn, "300000000001")
    assert arn in iam_backends["300000000001"]["aws"].managed_policies


def test_service_role_path_is_preserved():
    arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    _ensure_aws_managed_policy(arn, "300000000002")
    policy = iam_backends["300000000002"]["aws"].managed_policies[arn]
    assert policy.path == "/service-role/"


def test_customer_policy_arn_is_ignored():
    before = dict(iam_backends["300000000003"]["aws"].managed_policies)
    _ensure_aws_managed_policy("arn:aws:iam::300000000003:policy/mine", "300000000003")
    assert iam_backends["300000000003"]["aws"].managed_policies == before


def test_unknown_managed_policy_is_not_invented():
    arn = "arn:aws:iam::aws:policy/DefinitelyNotARealPolicy"
    _ensure_aws_managed_policy(arn, "300000000004")
    assert arn not in iam_backends["300000000004"]["aws"].managed_policies


def test_supplement_covers_policy_newer_than_moto_catalog():
    arn = "arn:aws:iam::aws:policy/BedrockAgentCoreRuntimeInstancesOperatorRolePolicy"
    _ensure_aws_managed_policy(arn, "300000000005")
    assert arn in iam_backends["300000000005"]["aws"].managed_policies


def test_only_named_policies_are_loaded():
    _ensure_aws_managed_policy("arn:aws:iam::aws:policy/SecurityAudit", "300000000006")
    assert len(iam_backends["300000000006"]["aws"].managed_policies) == 1
