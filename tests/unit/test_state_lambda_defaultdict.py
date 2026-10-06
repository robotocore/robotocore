"""Moto backends holding ``defaultdict(lambda: ...)`` must survive save/load.

SSM's ``SimpleSystemManagerBackend._resource_tags`` is ``defaultdict(lambda: defaultdict(dict))``.
A lambda can't be pickled, and the save loop skips any service that fails to pickle, so every
SSM parameter was silently lost across a restart.
"""

import io
from collections import defaultdict

import boto3
from moto import mock_aws
from moto.backends import get_backend

from robotocore.state.manager import _RestrictedUnpickler, _safe_pickle_dumps


def _roundtrip(obj):
    return _RestrictedUnpickler(io.BytesIO(_safe_pickle_dumps(obj))).load()


def test_nested_lambda_defaultdict_roundtrips_with_working_factories():
    d = defaultdict(lambda: defaultdict(dict))
    d["a"]["b"]["c"] = 1
    out = _roundtrip(d)
    assert out["a"]["b"]["c"] == 1
    out["new"]["x"]["y"] = 2  # rebuilt factories still auto-vivify two levels
    assert isinstance(out["new"], defaultdict)


def test_ssm_backend_with_parameters_pickles():
    with mock_aws():
        boto3.client("ssm", region_name="us-east-1").put_parameter(
            Name="/p", Value="x", Type="String"
        )
        b = get_backend("ssm")
        state = {a: {r: b[a][r] for r in b[a].keys()} for a in b.keys()}
        out = _roundtrip(state)
        backend = next(iter(next(iter(out.values())).values()))
        assert "/p" in backend._parameters


def test_kms_backend_with_keys_and_aliases_pickles():
    with mock_aws():
        k = boto3.client("kms", region_name="us-east-1")
        rsa = k.create_key(KeySpec="RSA_2048", KeyUsage="SIGN_VERIFY")["KeyMetadata"]["KeyId"]
        ecc = k.create_key(KeySpec="ECC_NIST_P256", KeyUsage="SIGN_VERIFY")["KeyMetadata"]["KeyId"]
        k.create_alias(AliasName="alias/persisted", TargetKeyId=rsa)
        b = get_backend("kms")
        out = _roundtrip({a: {r: b[a][r] for r in b[a].keys()} for a in b.keys()})
        backend = next(iter(next(iter(out.values())).values()))
        assert {rsa, ecc} <= set(backend.keys)
        assert "alias/persisted" in backend.keys[rsa].aliases
        assert (
            backend.keys[rsa].private_key is not None and backend.keys[ecc].private_key is not None
        )
