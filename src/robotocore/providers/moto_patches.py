"""Monkey-patches for Moto to add missing operations.

Applied at import time via moto_bridge.py.
"""

import logging

logger = logging.getLogger(__name__)

_applied = False


def apply_patches() -> None:
    """Apply all Moto patches. Safe to call multiple times."""
    global _applied
    if _applied:
        return
    _applied = True

    _patch_sts_get_access_key_info()
    _patch_apigateway_delete_model()
    logger.debug("Moto patches applied")


def _patch_sts_get_access_key_info() -> None:
    """Add get_access_key_info to Moto's STS backend and responses."""
    try:
        from moto.sts.models import STSBackend

        def get_access_key_info(self, access_key_id):
            return {"Account": self.account_id}

        if not hasattr(STSBackend, "get_access_key_info"):
            STSBackend.get_access_key_info = get_access_key_info

        from moto.sts.responses import TokenResponse

        def call_get_access_key_info(self):
            access_key_id = self._get_param("AccessKeyId")
            result = self.backend.get_access_key_info(access_key_id)
            template = self.response_template(_GET_ACCESS_KEY_INFO_TEMPLATE)
            return template.render(**result)

        if not hasattr(TokenResponse, "get_access_key_info"):
            TokenResponse.get_access_key_info = call_get_access_key_info

    except Exception:
        logger.debug("Failed to patch STS get_access_key_info", exc_info=True)


_GET_ACCESS_KEY_INFO_TEMPLATE = """<GetAccessKeyInfoResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetAccessKeyInfoResult>
    <Account>{{ Account }}</Account>
  </GetAccessKeyInfoResult>
  <ResponseMetadata>
    <RequestId>12345678-1234-1234-1234-123456789012</RequestId>
  </ResponseMetadata>
</GetAccessKeyInfoResponse>"""


def _patch_apigateway_delete_model() -> None:
    """Add delete_model to Moto's API Gateway backend and responses."""
    try:
        from moto.apigateway.models import APIGatewayBackend

        def delete_model(self, rest_api_id, model_name):
            rest_api = self.get_rest_api(rest_api_id)
            if model_name in rest_api.models:
                del rest_api.models[model_name]
                return {}
            from moto.apigateway.exceptions import ModelNotFound

            raise ModelNotFound()

        if not hasattr(APIGatewayBackend, "delete_model"):
            APIGatewayBackend.delete_model = delete_model

        from moto.apigateway.responses import APIGatewayResponse

        def api_delete_model(self):
            url_path_parts = self.path.split("/")
            rest_api_id = url_path_parts[2]
            model_name = url_path_parts[4]
            self.backend.delete_model(rest_api_id, model_name)
            return 202, {}, "{}"

        if not hasattr(APIGatewayResponse, "delete_model"):
            APIGatewayResponse.delete_model = api_delete_model

    except Exception:
        logger.debug("Failed to patch API Gateway delete_model", exc_info=True)
