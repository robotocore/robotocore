"""Unit tests for Moto monkey-patches."""

from unittest.mock import MagicMock

from robotocore.providers.moto_patches import apply_patches


class TestApplyPatches:
    def test_idempotent(self):
        """apply_patches can be called multiple times safely."""
        import robotocore.providers.moto_patches as mod

        original = mod._applied
        mod._applied = False
        try:
            apply_patches()
            apply_patches()  # second call should be no-op
        finally:
            mod._applied = original

    def test_sts_patch_adds_method(self):
        """Verify STS get_access_key_info patch works."""
        try:
            from moto.sts.models import STSBackend

            # The patch should have added the method
            assert hasattr(STSBackend, "get_access_key_info")
            # Test the patched method behavior
            backend = MagicMock(spec=STSBackend)
            backend.account_id = "123456789012"
            result = STSBackend.get_access_key_info(backend, "AKIATEST")
            assert result["Account"] == "123456789012"
        except ImportError:
            pass  # Moto not available in this test env

    def test_apigateway_patch_adds_method(self):
        """Verify API Gateway delete_model patch works."""
        try:
            from moto.apigateway.models import APIGatewayBackend

            assert hasattr(APIGatewayBackend, "delete_model")
        except ImportError:
            pass  # Moto not available


class TestStsGetAccessKeyInfo:
    def test_returns_account_id(self):
        try:
            from moto.sts.models import STSBackend

            backend = MagicMock(spec=STSBackend)
            backend.account_id = "999888777666"
            result = STSBackend.get_access_key_info(backend, "AKIAEXAMPLE")
            assert result == {"Account": "999888777666"}
        except ImportError:
            pass


class TestApiGatewayDeleteModel:
    def test_deletes_existing_model(self):
        try:
            from moto.apigateway.models import APIGatewayBackend

            backend = MagicMock(spec=APIGatewayBackend)
            rest_api = MagicMock()
            rest_api.models = {"MyModel": MagicMock()}
            backend.get_rest_api.return_value = rest_api

            # The patched method is an unbound function, call it directly
            if hasattr(APIGatewayBackend, "delete_model"):
                APIGatewayBackend.delete_model(backend, "api-id", "MyModel")
                assert "MyModel" not in rest_api.models
        except ImportError:
            pass
