"""Python runtime executor — runs handler in-process for speed."""

from robotocore.services.lambda_.executor import execute_python_handler


class PythonExecutor:
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
        return execute_python_handler(
            code_zip=code_zip,
            handler=handler,
            event=event,
            function_name=function_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            layer_zips=layer_zips,
            code_dir=code_dir,
            hot_reload=hot_reload,
        )
