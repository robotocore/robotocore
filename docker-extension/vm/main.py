"""Backend service for the Robotocore Docker Desktop extension.

Proxies between Docker Desktop UI and the robotocore container,
providing start/stop/status/logs management endpoints.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

ROBOTOCORE_IMAGE = os.environ.get("ROBOTOCORE_IMAGE", "robotocore/robotocore:latest")
ROBOTOCORE_CONTAINER = os.environ.get("ROBOTOCORE_CONTAINER", "robotocore")
ROBOTOCORE_PORT = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
DOCKER_SOCKET = os.environ.get("DOCKER_HOST", "unix:///var/run/docker.sock")


def _run_docker(*args: str) -> subprocess.CompletedProcess[str]:
    """Run a docker CLI command and return the result."""
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _get_container_info() -> dict[str, Any] | None:
    """Get info about the robotocore container if it exists."""
    result = _run_docker("inspect", ROBOTOCORE_CONTAINER, "--format", "{{json .}}")
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


async def status_endpoint(request: Request) -> JSONResponse:
    """GET /status - Check if robotocore container is running."""
    info = _get_container_info()
    if info is None:
        return JSONResponse(
            {
                "running": False,
                "container_id": None,
                "uptime": None,
                "port": ROBOTOCORE_PORT,
            }
        )

    state = info.get("State", {})
    running = state.get("Running", False)
    started_at = state.get("StartedAt", "") if running else None
    container_id = info.get("Id", "")[:12]

    return JSONResponse(
        {
            "running": running,
            "container_id": container_id,
            "uptime": started_at,
            "port": ROBOTOCORE_PORT,
        }
    )


async def start_endpoint(request: Request) -> JSONResponse:
    """POST /start - Start robotocore container with config."""
    body: dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        pass

    # Build environment variable flags
    env_flags: list[str] = []
    env_vars = body.get("env", {})
    for key, value in env_vars.items():
        env_flags.extend(["-e", f"{key}={value}"])

    # Remove existing container if present
    _run_docker("rm", "-f", ROBOTOCORE_CONTAINER)

    # Start new container
    result = _run_docker(
        "run",
        "-d",
        "--name",
        ROBOTOCORE_CONTAINER,
        "-p",
        f"{ROBOTOCORE_PORT}:{ROBOTOCORE_PORT}",
        *env_flags,
        ROBOTOCORE_IMAGE,
    )

    if result.returncode != 0:
        return JSONResponse(
            {"error": result.stderr.strip()},
            status_code=500,
        )

    container_id = result.stdout.strip()[:12]
    return JSONResponse(
        {
            "started": True,
            "container_id": container_id,
        }
    )


async def stop_endpoint(request: Request) -> JSONResponse:
    """POST /stop - Stop robotocore container."""
    result = _run_docker("stop", ROBOTOCORE_CONTAINER)
    if result.returncode != 0:
        return JSONResponse(
            {"error": result.stderr.strip()},
            status_code=500,
        )
    # Remove the container after stopping
    _run_docker("rm", ROBOTOCORE_CONTAINER)
    return JSONResponse({"stopped": True})


async def logs_endpoint(request: Request) -> StreamingResponse:
    """GET /logs - Stream container logs."""
    tail = request.query_params.get("tail", "100")
    result = _run_docker("logs", "--tail", tail, ROBOTOCORE_CONTAINER)

    content = result.stdout + result.stderr
    return StreamingResponse(
        iter([content]),
        media_type="text/plain",
    )


async def proxy_endpoint(request: Request) -> JSONResponse:
    """GET /proxy/* - Proxy requests to robotocore management endpoints.

    Strips the /proxy prefix before forwarding to robotocore.
    """
    # Strip the "/proxy" prefix from the path
    path = request.url.path.removeprefix("/proxy")
    if not path.startswith("/"):
        path = "/" + path

    url = f"http://localhost:{ROBOTOCORE_PORT}{path}"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.request(
                method=request.method,
                url=url,
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
                content=await request.body(),
                timeout=10.0,
            )
            return JSONResponse(
                content=resp.json()
                if resp.headers.get("content-type", "").startswith("application/json")
                else {"body": resp.text},
                status_code=resp.status_code,
            )
        except httpx.ConnectError:
            return JSONResponse(
                {"error": "Cannot connect to robotocore. Is it running?"},
                status_code=502,
            )
        except Exception as exc:
            return JSONResponse(
                {"error": str(exc)},
                status_code=500,
            )


routes = [
    Route("/status", status_endpoint, methods=["GET"]),
    Route("/start", start_endpoint, methods=["POST"]),
    Route("/stop", stop_endpoint, methods=["POST"]),
    Route("/logs", logs_endpoint, methods=["GET"]),
    Route("/proxy/{path:path}", proxy_endpoint, methods=["GET", "POST", "PUT", "DELETE"]),
]

app = Starlette(routes=routes)

if __name__ == "__main__":
    port = int(os.environ.get("EXTENSION_PORT", "8080"))
    uvicorn.run(app, host="0.0.0.0", port=port)
