"""Native RDS Data API provider with real SQL execution.

Implements ExecuteStatement, BatchExecuteStatement, and transaction management
using the SQLite engines created by the RDS provider.
Uses rest-json protocol.
"""

import json
import logging
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.rds.provider import get_engine

logger = logging.getLogger(__name__)


def _extract_db_identifier_from_arn(resource_arn: str) -> str | None:
    """Extract DB instance/cluster identifier from an RDS ARN.

    ARN format: arn:aws:rds:<region>:<account>:db:<identifier>
             or arn:aws:rds:<region>:<account>:cluster:<identifier>
    """
    parts = resource_arn.split(":")
    if len(parts) >= 7:
        return parts[-1]
    return None


def _extract_region_from_arn(resource_arn: str) -> str | None:
    """Extract region from an RDS ARN."""
    parts = resource_arn.split(":")
    if len(parts) >= 4:
        return parts[3]
    return None


def _extract_account_from_arn(resource_arn: str) -> str | None:
    """Extract account ID from an RDS ARN."""
    parts = resource_arn.split(":")
    if len(parts) >= 5:
        return parts[4]
    return None


def _python_to_rds_field(value: Any) -> dict:
    """Convert a Python value to an RDS Data API typed field."""
    if value is None:
        return {"isNull": True}
    if isinstance(value, bool):
        return {"booleanValue": value}
    if isinstance(value, int):
        return {"longValue": value}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, bytes):
        import base64

        return {"blobValue": base64.b64encode(value).decode()}
    return {"stringValue": str(value)}


def _rds_param_to_python(param: dict) -> Any:
    """Convert an RDS Data API parameter to a Python value."""
    value = param.get("value", {})
    if value.get("isNull"):
        return None
    if "booleanValue" in value:
        return value["booleanValue"]
    if "longValue" in value:
        return value["longValue"]
    if "doubleValue" in value:
        return value["doubleValue"]
    if "stringValue" in value:
        return value["stringValue"]
    if "blobValue" in value:
        import base64

        return base64.b64decode(value["blobValue"])
    return None


def _sqlite_type_to_rds_type(value: Any) -> str:
    """Map Python/SQLite types to RDS Data API type names."""
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, bytes):
        return "BLOB"
    return "VARCHAR"


def _build_column_metadata(rows: list[dict]) -> list[dict]:
    """Build columnMetadata from result rows."""
    if not rows:
        return []
    first_row = rows[0]
    # Skip internal metadata rows (rowsAffected)
    if list(first_row.keys()) == ["rowsAffected"]:
        return []
    metadata = []
    for col_name, col_value in first_row.items():
        type_name = _sqlite_type_to_rds_type(col_value)
        metadata.append(
            {
                "name": col_name,
                "typeName": type_name,
                "label": col_name,
                "nullable": 1,
                "precision": 0,
                "scale": 0,
            }
        )
    return metadata


def _rows_to_records(rows: list[dict]) -> list[list[dict]]:
    """Convert result rows to RDS Data API record format."""
    records = []
    for row in rows:
        # Skip internal metadata rows
        if list(row.keys()) == ["rowsAffected"]:
            continue
        record = [_python_to_rds_field(v) for v in row.values()]
        records.append(record)
    return records


def _convert_parameters(sql: str, parameters: list[dict]) -> list | dict:
    """Convert RDS Data API parameters to SQLite-compatible format.

    If the SQL uses named parameters (:name), return a dict for SQLite's
    named parameter binding. Otherwise, return a positional list.
    """
    # Check if any parameter has a name and the SQL contains :name references
    has_named = any(p.get("name") for p in parameters)
    if has_named:
        result = {}
        for p in parameters:
            name = p.get("name", "")
            result[name] = _rds_param_to_python(p)
        return result
    return [_rds_param_to_python(p) for p in parameters]


def _json_response(data: dict, status_code: int = 200) -> Response:
    """Build a JSON response."""
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/json",
    )


def _error_response(code: str, message: str, status_code: int = 400) -> Response:
    """Build an error response in AWS JSON format."""
    return _json_response({"__type": code, "message": message}, status_code)


async def handle_rdsdata_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an RDS Data API request (rest-json protocol)."""
    path = request.url.path
    method = request.method

    # RDS Data API uses REST paths
    if method == "POST" and path.endswith("/Execute"):
        return await _execute_statement(request, region, account_id)
    elif method == "POST" and path.endswith("/BatchExecute"):
        return await _batch_execute_statement(request, region, account_id)
    elif method == "POST" and path.endswith("/BeginTransaction"):
        return await _begin_transaction(request, region, account_id)
    elif method == "POST" and path.endswith("/CommitTransaction"):
        return await _commit_transaction(request, region, account_id)
    elif method == "POST" and path.endswith("/RollbackTransaction"):
        return await _rollback_transaction(request, region, account_id)

    # Fall back to Moto for anything else
    return await forward_to_moto(request, "rds-data", account_id=account_id)


async def _execute_statement(request: Request, region: str, account_id: str) -> Response:
    """Handle ExecuteStatement — execute SQL against a real SQLite engine."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response("BadRequestException", "Invalid JSON in request body")

    resource_arn = data.get("resourceArn", "")
    sql = data.get("sql", "")
    parameters = data.get("parameters", [])
    transaction_id = data.get("transactionId")

    if not resource_arn:
        return _error_response("BadRequestException", "resourceArn is required")
    if not sql:
        return _error_response("BadRequestException", "sql is required")

    db_identifier = _extract_db_identifier_from_arn(resource_arn)
    arn_region = _extract_region_from_arn(resource_arn) or region
    arn_account = _extract_account_from_arn(resource_arn) or account_id

    if not db_identifier:
        return _error_response("BadRequestException", "Invalid resourceArn format")

    engine = get_engine(arn_account, arn_region, db_identifier)
    if engine is None:
        return _error_response(
            "BadRequestException",
            f"Database {db_identifier} not found. Create it with RDS CreateDBInstance first.",
        )

    # Convert parameters for SQLite
    sql_params = _convert_parameters(sql, parameters) if parameters else None

    try:
        if transaction_id:
            rows = engine.execute_in_transaction(transaction_id, sql, sql_params)
        else:
            rows = engine.execute_sql(sql, sql_params)
    except Exception as e:  # noqa: BLE001
        return _error_response("BadRequestException", f"SQL error: {e}")

    # Build response
    result: dict[str, Any] = {
        "numberOfRecordsUpdated": 0,
    }

    # Check if this was a DML statement (INSERT/UPDATE/DELETE)
    if rows and list(rows[0].keys()) == ["rowsAffected"]:
        result["numberOfRecordsUpdated"] = rows[0]["rowsAffected"]
        # For INSERT, try to get generated fields
        result["generatedFields"] = []
    else:
        # SELECT — include records and column metadata
        result["columnMetadata"] = _build_column_metadata(rows)
        result["records"] = _rows_to_records(rows)

    return _json_response(result)


async def _batch_execute_statement(request: Request, region: str, account_id: str) -> Response:
    """Handle BatchExecuteStatement — execute SQL with multiple parameter sets."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response("BadRequestException", "Invalid JSON in request body")

    resource_arn = data.get("resourceArn", "")
    sql = data.get("sql", "")
    parameter_sets = data.get("parameterSets", [])
    transaction_id = data.get("transactionId")

    if not resource_arn:
        return _error_response("BadRequestException", "resourceArn is required")
    if not sql:
        return _error_response("BadRequestException", "sql is required")

    db_identifier = _extract_db_identifier_from_arn(resource_arn)
    arn_region = _extract_region_from_arn(resource_arn) or region
    arn_account = _extract_account_from_arn(resource_arn) or account_id

    if not db_identifier:
        return _error_response("BadRequestException", "Invalid resourceArn format")

    engine = get_engine(arn_account, arn_region, db_identifier)
    if engine is None:
        return _error_response(
            "BadRequestException",
            f"Database {db_identifier} not found.",
        )

    update_results = []
    try:
        for param_set in parameter_sets:
            sql_params = _convert_parameters(sql, param_set)
            if transaction_id:
                rows = engine.execute_in_transaction(transaction_id, sql, sql_params)
            else:
                rows = engine.execute_sql(sql, sql_params)
            if rows and list(rows[0].keys()) == ["rowsAffected"]:
                update_results.append({"generatedFields": []})
            else:
                update_results.append({"generatedFields": []})
    except Exception as e:  # noqa: BLE001
        return _error_response("BadRequestException", f"SQL error: {e}")

    return _json_response({"updateResults": update_results})


async def _begin_transaction(request: Request, region: str, account_id: str) -> Response:
    """Handle BeginTransaction."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response("BadRequestException", "Invalid JSON in request body")

    resource_arn = data.get("resourceArn", "")

    db_identifier = _extract_db_identifier_from_arn(resource_arn)
    arn_region = _extract_region_from_arn(resource_arn) or region
    arn_account = _extract_account_from_arn(resource_arn) or account_id

    if not db_identifier:
        return _error_response("BadRequestException", "Invalid resourceArn format")

    engine = get_engine(arn_account, arn_region, db_identifier)
    if engine is None:
        return _error_response(
            "BadRequestException",
            f"Database {db_identifier} not found.",
        )

    tx_id = engine.begin_transaction()
    return _json_response({"transactionId": tx_id})


async def _commit_transaction(request: Request, region: str, account_id: str) -> Response:
    """Handle CommitTransaction."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response("BadRequestException", "Invalid JSON in request body")

    resource_arn = data.get("resourceArn", "")
    transaction_id = data.get("transactionId", "")

    db_identifier = _extract_db_identifier_from_arn(resource_arn)
    arn_region = _extract_region_from_arn(resource_arn) or region
    arn_account = _extract_account_from_arn(resource_arn) or account_id

    if not db_identifier:
        return _error_response("BadRequestException", "Invalid resourceArn format")

    engine = get_engine(arn_account, arn_region, db_identifier)
    if engine is None:
        return _error_response("BadRequestException", f"Database {db_identifier} not found.")

    try:
        engine.commit_transaction(transaction_id)
    except Exception as e:  # noqa: BLE001
        return _error_response("BadRequestException", f"Transaction error: {e}")

    return _json_response({"transactionStatus": "Transaction Committed"})


async def _rollback_transaction(request: Request, region: str, account_id: str) -> Response:
    """Handle RollbackTransaction."""
    body = await request.body()
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response("BadRequestException", "Invalid JSON in request body")

    resource_arn = data.get("resourceArn", "")
    transaction_id = data.get("transactionId", "")

    db_identifier = _extract_db_identifier_from_arn(resource_arn)
    arn_region = _extract_region_from_arn(resource_arn) or region
    arn_account = _extract_account_from_arn(resource_arn) or account_id

    if not db_identifier:
        return _error_response("BadRequestException", "Invalid resourceArn format")

    engine = get_engine(arn_account, arn_region, db_identifier)
    if engine is None:
        return _error_response("BadRequestException", f"Database {db_identifier} not found.")

    try:
        engine.rollback_transaction(transaction_id)
    except Exception as e:  # noqa: BLE001
        return _error_response("BadRequestException", f"Transaction error: {e}")

    return _json_response({"transactionStatus": "Transaction Rolledback"})
