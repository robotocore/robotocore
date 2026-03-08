"""Notes app parity test -- API Gateway + Lambda + DynamoDB.

Derived from LocalStack's note_taking scenario test:
vendor/localstack/tests/aws/scenario/note_taking/test_note_taking.py

The original uses CDK to provision an API Gateway REST API backed by Lambda
functions that read/write to DynamoDB.

This test verifies:
1. API Gateway REST API creation with Lambda proxy integration
2. HTTP requests routed through APIGW reach Lambda and return its response
3. Lambda function creation and direct invocation
4. DynamoDB CRUD operations (the data layer of the notes app)
"""

import io
import json
import time
import uuid
import zipfile

import requests

# Stateless handler that echoes request info back
ECHO_HANDLER = """
import json

def handler(event, context):
    method = event.get("httpMethod", "GET")
    path = event.get("path", "/")
    path_params = event.get("pathParameters") or {}
    body = event.get("body")
    if body and isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            pass

    response = {
        "method": method,
        "path": path,
        "pathParameters": path_params,
        "body": body,
    }
    return {"statusCode": 200, "body": json.dumps(response)}
"""


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestNotesApp:
    """APIGW REST API -> Lambda, mirroring the note_taking scenario."""

    def test_apigw_lambda_proxy_integration(self, aws_client, lambda_role_arn):
        """Verify APIGW routes requests to Lambda and returns responses.

        This tests the core integration pattern from the notes scenario:
        HTTP request -> API Gateway -> Lambda -> HTTP response.
        """
        lam = aws_client.lambda_
        apigw = aws_client.apigateway

        fn_name = _unique("echo-handler")
        role_arn = lambda_role_arn
        rest_api_id = None

        try:
            # Create Lambda
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_lambda_zip(ECHO_HANDLER)},
                Timeout=30,
            )
            for _ in range(30):
                fn = lam.get_function(FunctionName=fn_name)
                if fn["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)

            # Create REST API
            api = apigw.create_rest_api(name=_unique("notes-api"))
            rest_api_id = api["id"]

            resources = apigw.get_resources(restApiId=rest_api_id)
            root_id = resources["items"][0]["id"]

            # /notes resource
            notes_res = apigw.create_resource(
                restApiId=rest_api_id, parentId=root_id, pathPart="notes"
            )
            notes_id = notes_res["id"]

            # /notes/{id} resource
            note_res = apigw.create_resource(
                restApiId=rest_api_id, parentId=notes_id, pathPart="{id}"
            )
            note_id = note_res["id"]

            fn_arn = lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
            uri = (
                "arn:aws:apigateway:us-east-1:lambda:path"
                f"/2015-03-31/functions/{fn_arn}/invocations"
            )

            # Wire up all methods
            method_map = [
                (notes_id, ["GET", "POST"]),
                (note_id, ["GET", "PUT", "DELETE"]),
            ]
            for res_id, methods in method_map:
                for method in methods:
                    apigw.put_method(
                        restApiId=rest_api_id,
                        resourceId=res_id,
                        httpMethod=method,
                        authorizationType="NONE",
                    )
                    apigw.put_integration(
                        restApiId=rest_api_id,
                        resourceId=res_id,
                        httpMethod=method,
                        type="AWS_PROXY",
                        integrationHttpMethod="POST",
                        uri=uri,
                    )

            apigw.create_deployment(restApiId=rest_api_id, stageName="test")

            from tests.localstack_parity.conftest import ENDPOINT_URL

            base = f"{ENDPOINT_URL}/restapis/{rest_api_id}/test/_user_request_"
            time.sleep(1)

            # GET /notes
            resp = requests.get(f"{base}/notes")
            assert resp.status_code == 200, f"GET /notes: {resp.text}"
            data = json.loads(resp.text)
            assert data["method"] == "GET"

            # POST /notes with body
            resp = requests.post(f"{base}/notes", json={"content": "my note"})
            assert resp.status_code == 200
            data = json.loads(resp.text)
            assert data["method"] == "POST"
            assert data["body"]["content"] == "my note"

            # GET /notes/{id} with path parameter
            resp = requests.get(f"{base}/notes/abc123")
            assert resp.status_code == 200
            data = json.loads(resp.text)
            assert data["method"] == "GET"
            assert data["pathParameters"]["id"] == "abc123"

            # PUT /notes/{id}
            resp = requests.put(f"{base}/notes/abc123", json={"content": "updated"})
            assert resp.status_code == 200
            data = json.loads(resp.text)
            assert data["method"] == "PUT"

            # DELETE /notes/{id}
            resp = requests.delete(f"{base}/notes/abc123")
            assert resp.status_code == 200
            data = json.loads(resp.text)
            assert data["method"] == "DELETE"

        finally:
            if rest_api_id:
                try:
                    apigw.delete_rest_api(restApiId=rest_api_id)
                except Exception:
                    pass
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass

    def test_dynamodb_notes_crud(self, aws_client):
        """Verify DynamoDB supports the notes app data operations.

        Tests the same DynamoDB operations that the notes Lambda functions
        perform: PutItem, GetItem, Scan, UpdateItem, DeleteItem.
        """
        ddb = aws_client.dynamodb
        table_name = _unique("notes")

        try:
            ddb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "noteId", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "noteId", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            ddb.get_waiter("table_exists").wait(TableName=table_name)

            # PutItem (createNote)
            ddb.put_item(
                TableName=table_name,
                Item={
                    "noteId": {"S": "note-1"},
                    "content": {"S": "hello world"},
                    "createdAt": {"S": "1234567890"},
                },
            )
            ddb.put_item(
                TableName=table_name,
                Item={
                    "noteId": {"S": "note-2"},
                    "content": {"S": "testing is fun"},
                    "createdAt": {"S": "1234567891"},
                },
            )

            # Scan (listNotes)
            scan = ddb.scan(TableName=table_name)
            assert scan["Count"] == 2

            # GetItem (getNote)
            item = ddb.get_item(TableName=table_name, Key={"noteId": {"S": "note-1"}})
            assert item["Item"]["content"]["S"] == "hello world"

            # UpdateItem (updateNote)
            ddb.update_item(
                TableName=table_name,
                Key={"noteId": {"S": "note-1"}},
                UpdateExpression="SET content = :c",
                ExpressionAttributeValues={":c": {"S": "updated"}},
            )
            item = ddb.get_item(TableName=table_name, Key={"noteId": {"S": "note-1"}})
            assert item["Item"]["content"]["S"] == "updated"

            # DeleteItem (deleteNote)
            ddb.delete_item(TableName=table_name, Key={"noteId": {"S": "note-2"}})
            scan = ddb.scan(TableName=table_name)
            assert scan["Count"] == 1

            # Verify deleted item not found
            item = ddb.get_item(TableName=table_name, Key={"noteId": {"S": "note-2"}})
            assert "Item" not in item

        finally:
            try:
                ddb.delete_table(TableName=table_name)
            except Exception:
                pass

    def test_lambda_invoke(self, aws_client, lambda_role_arn):
        """Verify Lambda function creation and invocation."""
        lam = aws_client.lambda_
        fn_name = _unique("invoke-test")

        handler_code = (
            "import json\n"
            "def handler(event, context):\n"
            "    return {'statusCode': 200, "
            "'body': json.dumps({'msg': event.get('msg', 'none')})}\n"
        )

        try:
            lam.create_function(
                FunctionName=fn_name,
                Runtime="python3.12",
                Role=lambda_role_arn,
                Handler="lambda_function.handler",
                Code={"ZipFile": _make_lambda_zip(handler_code)},
                Timeout=10,
            )
            for _ in range(30):
                fn = lam.get_function(FunctionName=fn_name)
                if fn["Configuration"]["State"] == "Active":
                    break
                time.sleep(1)

            resp = lam.invoke(
                FunctionName=fn_name,
                Payload=json.dumps({"msg": "hello"}).encode(),
            )
            payload = json.loads(resp["Payload"].read())
            assert payload["statusCode"] == 200
            body = json.loads(payload["body"])
            assert body["msg"] == "hello"

        finally:
            try:
                lam.delete_function(FunctionName=fn_name)
            except Exception:
                pass
