"""Unparseable PartiQL statements must answer AWS's ValidationException.

Real AWS answers a syntax error with a 4xx `ValidationException`; the twin's PartiQL
parser used to return `None` for such statements, which crashed `ExecuteStatement` with
a TypeError and surfaced as a 500 InternalError. Probes and Terraform refresh flows hit
that on any statement typo, and the 500 made probe-driven discovery report the endpoint
as broken rather than mis-used.
"""

from starlette.testclient import TestClient

from robotocore.gateway.app import app


class TestExecuteStatementSyntaxError:
    def test_garbage_statement_is_validation_exception(self):
        client = TestClient(app, raise_server_exceptions=False)
        r = client.post(
            "/",
            content=b'{"Statement": "{{not a statement"}',
            headers={
                "Content-Type": "application/x-amz-json-1.0",
                "X-Amz-Target": "DynamoDB_20120810.ExecuteStatement",
            },
        )
        assert r.status_code == 400
        assert "ValidationException" in r.text
